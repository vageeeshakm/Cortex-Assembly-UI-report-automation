import os
import json
import logging
from datetime import datetime
from config.linkedin_config import (
    PATH,
    DATE,
    AWS_SECRET_MANAGER,
    PLATFORMTYPE,
    FILENAME,
    PANDASSETTINGS,
    EMAIL
)
from config.report_status_config import PLATFORM, REPORTLOCATION
from config.ftp_config import FTPSECRET
from config.email_config import EMAILSECRET
from utils.datasources.linkedin.linkedin_utils import LinkedInUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.ftp_utils import FtpUtils
from utils.common.email_utils import EmailUtils
from utils.common.pandas_utils import PandasUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.selenium.selenium_utils import SeleniumUtils
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class LinkedInReport():
    """ class with function to download LinkedIn report"""

    def create_directory_on_ftp(self, **kwargs):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_created
        """
        logging.info("fetching ftom secret manager")
        formated_current_date = datetime.today().strftime(DATE.YMD_DATE_FORMAT)

        aws_secret_manager_object = AwsSecretManager()
        ftp_secret_json = json.loads(aws_secret_manager_object.get_secret(FTPSECRET.FTP_SERCRET_NAME))
        logging.info("creating directory on ftp")
        ftp_object = FtpUtils(ftp_secret_json.get('host'), ftp_secret_json.get('username'), ftp_secret_json.get('password'))

        # Main directory to upload csv files
        parent_directory_to_upload = PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES
        directory_on_ftp = parent_directory_to_upload + formated_current_date + '/'

        # This should be replaced later. For testing , kept parent directory as test_dir. Later above line should be uncommented
        # directory_on_ftp = '/test_dir_linkedin/' + formated_current_date + '/'

        ftp_object.create_nested_directory(directory_on_ftp)
        return {
            "ftp_object": ftp_object,
            "directory_on_ftp": directory_on_ftp
        }

    def download_report(self, **kwargs):
        """
        Download LinkedIn report function
        INPUT : {dict}
                download_folder_path - path to the download foleder
                report_config(json file)
                    {
                        "account_name": "Ad account name",
                        "account_id": "Ad account id",
                        "date_range": "date range",
                        "platform_type": "platform type (daily/weekly)",
                        "report_type": "list of report type to be downloaded for an account"
                    }
        OUTPUT: Download fiile in csv format in specified folder.
        """
        try:
            selenium_object = None
            logging.info('LinkedIn report download started')

            logging.info("fetching from secret manager")

            # Fetch username and password from AWS secret
            aws_secret_manager_object = AwsSecretManager()
            linkedin_secret_json = json.loads(aws_secret_manager_object.get_secret(AWS_SECRET_MANAGER.SECRET_NAME))
            username = linkedin_secret_json.get('username')
            password = linkedin_secret_json.get('password')

            # Fetch report config information
            report_config = kwargs.get('report_config')
            account_id = report_config.get('account_id')
            date_range_to_select = report_config.get('date_range')
            platform_type = report_config.get('platform_type')
            report_type_list = report_config.get('report_type')
            download_folder_path = kwargs.get('download_folder_path')

            logging.info("Account id {}".format(account_id))
            logging.info("date_range is {}".format(date_range_to_select))

            # get selenium object
            selenium_object = SeleniumUtils(download_path=download_folder_path)
            # initialize LinkedIn utils object
            linkedin_utils_object = LinkedInUtils(selenium_object=selenium_object)
            # login function
            linkedin_utils_object.login(username, password)
            # choose account to download the report
            linkedin_utils_object.load_account_page(account_id)
            # select ads for account
            linkedin_utils_object.select_account_ads()
            # check if any ads present for account
            # IF no ads present then we can not generate report for that account
            account_ads_result = linkedin_utils_object.check_account_ads()
            if not account_ads_result:
                logging.info("report not generated for this account")
                return False
            # if platform is weekly report
            if platform_type == PLATFORMTYPE.LINKEDINWEEKLY:
                logging.info("selecting the date range")
                date_format = DATE.WEEKLY_REPORT_MDY_DATE_FORMAT
                linkedin_utils_object.select_date_range_to_generate_report(date_range_to_select, date_format)
            # Generate report for all specified report type
            for each_report_type in report_type_list:
                # generate report
                linkedin_utils_object.generate_report(each_report_type)
                # click on report download
                linkedin_utils_object.download_report()

            # Check download is sucess and files are present in download folder
            linkedin_utils_object.check_download_status(len(report_type_list))
            logging.info("All reports are downloaded")
            # click on logout
            linkedin_utils_object.logout()
            # exit by closing browser
            selenium_object.exit()
            logging.info('LinkedIn report download done')

            return True
        except Exception as e:
            logging.error("Error in downloading the report")
            if selenium_object:
                selenium_object.exit()
            raise Exception(e)

    def download_combine_and_upload_reports(self, **kwargs):
        """ Function that calls download and then combine csv files using pandas and upload the report into FTP server
            INPUT:
                ftp_object: ftp object
                directory_on_ftp: directory on FTP to upload files
                json config file
                    {
                        "account_name": "Ad account name",
                        "account_id": "Ad account id",
                        "date_range": "date range",
                        "platform_type": "platform type (daily/weekly)",
                        "report_type": "list of report type to be downloaded for an account"
                    }
            OUTPUT:
                upload csv file on FTP.
        """
        try:
            logging.info("Pulling values from xcom")
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_linkedin_directory_on_ftp_task')

            ftp_object = None
            directory_on_ftp = None
            if xcom_values:
                # Pull ftp_object and directory on ftp.
                ftp_object = xcom_values['ftp_object']
                directory_on_ftp = xcom_values['directory_on_ftp']
            # Fetch account name and platform type from config file
            account_id = kwargs.get('report_config').get('account_id')
            account_name = kwargs.get('report_config').get('account_name')
            platform_type = kwargs.get('report_config').get('platform_type')
            custom_report_file_prefix = kwargs.get('report_config').get('custom_report_file_prefix', None)
            logging.info("platform type is {}".format(platform_type))

            # report destination
            if platform_type == PLATFORMTYPE.LINKEDINDAILY:
                report_destination = directory_on_ftp
                linked_in_platform_type = PLATFORM.LINKEDINDAILY
            elif platform_type == PLATFORMTYPE.LINKEDINWEEKLY:
                report_destination = os.environ['LINKEDIN_ETRADE_WEEKLY_REPORT_EMAIL_TO']
                linked_in_platform_type = PLATFORM.LINKEDINWEEKLY
            else:
                logging.error("Invalid platform type specified")
                return

            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(linked_in_platform_type)

            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]
            kwargs = {'download_folder_path': download_folder, 'report_config': kwargs.get('report_config')}
            # Function to download the report
            download_res = self.download_report(**kwargs)
            if not download_res:
                logging.info("Report not generated")
                # remove the created temp directory
                temp_folder_object.clean_up()
                return

            # Initialise Pandas Utils class
            panadas_utils_object = PandasUtils()

            if platform_type == PLATFORMTYPE.LINKEDINDAILY:
                report_location = REPORTLOCATION.FTP
                formated_current_date = datetime.today().strftime(DATE.YMD_DATE_FORMAT)

                if custom_report_file_prefix:
                    standard_report_downloaded_file_name = custom_report_file_prefix + FILENAME.AD_PERFORMANCE_FILE_SUFFIX
                    conversion_report_downloaded_file_name = custom_report_file_prefix + FILENAME.CONVERSION_AD_PERFORMANCE_FILE_SUFFIX
                else:
                    standard_report_downloaded_file_name = FILENAME.STANDARD_REPORT_DOWNLOADED_FILE_NAME.format(account_id=account_id)
                    conversion_report_downloaded_file_name = FILENAME.CONVERSION_REPORT_DOWNLOADED_FILE_NAME.format(account_id=account_id)

                # downloaded files full path with name
                downloaded_file_name = {
                    "standard_report": download_folder + standard_report_downloaded_file_name,
                    "conversion_report": download_folder + conversion_report_downloaded_file_name
                }

                # Files name and path to be uploaded into FTP
                ouput_file_name = {
                    "standard_report": FILENAME.STANDARD_REPORT_OUTPUT_FILE_NAME.format(account_id=account_id, today_date=formated_current_date),
                    "conversion_report": FILENAME.CONVERSION_REPORT_OUTPUT_FILE_NAME.format(account_id=account_id, today_date=formated_current_date)
                }
                # For each downloaded report, check if data present and if present upload that into FTP
                for report_type, downloaded_file_path_with_name in downloaded_file_name.items():
                    # report upload location
                    # Upload all the downloaded csv files into FTP
                    logging.info("Uploading {} csv file into FTP".format(report_type))
                    ftp_object.put(downloaded_file_path_with_name, directory_on_ftp + ouput_file_name[report_type])

            elif platform_type == PLATFORMTYPE.LINKEDINWEEKLY:
                logging.info("weekly report")
                report_location = REPORTLOCATION.EMAIL
                formated_current_date = datetime.today().strftime(DATE.WEEKLY_REPORT_OUTPUT_FILE_DATE_FORMAT)
                downloaded_csv_file_path_with_name = download_folder + FILENAME.STANDARD_REPORT_DOWNLOADED_FILE_NAME.format(account_id=account_id)
                output_file_path_and_name = download_folder + FILENAME.WEEKLY_REPORT_OUTPUT_FILE_NAME.format(current_date=formated_current_date)

                logging.info("Reading csv file")
                # read csv file into dataframe
                report_dataframe = panadas_utils_object.get_dataframe_from_csv(downloaded_csv_file_path_with_name,
                                                                               file_encoding=PANDASSETTINGS.CSV_FILE_ENCODING_TYPE,
                                                                               number_of_starting_rows_to_skip=PANDASSETTINGS.WEEKLY_REPORT_NUMBER_OF_ROWS_TO_SKIP,
                                                                               delimiter_to_use=PANDASSETTINGS.DELIMITER_TO_READ_CSV)
                # Insert account id as column
                dataframe_after_insert = panadas_utils_object.insert_column_in_csv(report_dataframe,
                                                                                   PANDASSETTINGS.WEEKLY_REPORT_INSERT_COLUMN_INDEX,
                                                                                   PANDASSETTINGS.WEEKLY_REPORT_COLUMN_NAME_TO_INSERT,
                                                                                   value_to_insert=account_id)

                dataframe_after_renaming_columns = panadas_utils_object.rename_columns_in_dataframe(dataframe_after_insert, PANDASSETTINGS.WEEKLY_REPORT_RENAME_COLUMNS)
                # Select and re arrange columns from dataframe
                ordered_dataframe = panadas_utils_object.select_and_rearrange_columns_from_dataframe(dataframe_after_renaming_columns, PANDASSETTINGS.WEEKLY_REPORTS_COLUMNS_TO_SELECT)
                # convert dataframe into csv file
                panadas_utils_object.create_csv_file_from_dataframe(ordered_dataframe, output_file_path_and_name)

                # Fetch username, password and host from AWS secret
                aws_secret_manager_object = AwsSecretManager()
                email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

                logging.info("Sending email")
                # Message body for sending email
                email_message_body = '\n\n'.join(EMAIL.LINKEDIN_WEEKLY_REPORT_MESSAGE_BODY.split('|'))
                # Get email Utils object by passing host, username, password
                email_utils_object = EmailUtils(email_config)
                email_utils_object.send(
                    os.environ['LINKEDIN_ETRADE_WEEKLY_REPORT_EMAIL_TO'],
                    EMAIL.LINKEDIN_WEEKLY_REPORT_MESSAGE_SUBJECT,
                    email_message_body,
                    msg_attachments=[output_file_path_and_name]
                )
            else:
                raise Exception('Invalid platform type specified')

            logging.info("deleting temp folder")
            # remove the created temp directory
            temp_folder_object.clean_up()
            logging.info("temp folder deleted")
            # Create and Upload report status
            # Writing success details into success report and uplod into s3
            upload_report_status_res = create_report_object.success_report_status_to_s3(
                account_name,
                report_destination,
                location=report_location)
            logging.info("result {}".format(upload_report_status_res))
            logging.info("completed")

        except Exception as e:
            logging.error("Error in download_combine_and_upload_reports function")
            try:
                logging.info("Removing temp directory")
                temp_folder_object.clean_up()
            except Exception:
                logging.error("Error in removing temp directory")

            # Writing error details into failure report and uplod into s3
            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                account_name,
                e,
                report_destination,
                location=report_location)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
