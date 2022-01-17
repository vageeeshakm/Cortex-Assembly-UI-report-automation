import os
import json
import logging
from datetime import datetime
from config.yahoo_gemini_config import (
    PATH,
    DATE,
    AWS_SECRET_MANAGER,
    PLATFORMTYPE,
    PANDASSETTINGS,
    EMAIL
)
from config.report_status_config import REPORTLOCATION
from config.ftp_config import FTPSECRET
from config.email_config import EMAILSECRET
from config.file_config import FILETYPE
from utils.common.data_validations import DataValidationsUtils
from utils.datasources.yahoo.yahoo_gemini_utils import YahooGeminiUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.ftp_utils import FtpUtils
from utils.common.email_utils import EmailUtils
from utils.common.pandas_utils import PandasUtils
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.time_utils import date_range
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.selenium.selenium_utils import SeleniumUtils
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class YahooGeminiReport():
    """ class with function to download Google analytics report"""

    def create_directory_on_ftp(self, **kwargs):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_created
        """
        logging.info("fetching ftom secret manager")
        platform_type = kwargs.get('platform_type')
        formated_current_date = datetime.today().strftime(DATE.DATE_FORMAT)
        aws_secret_manager_object = AwsSecretManager()
        ftp_secret_json = json.loads(aws_secret_manager_object.get_secret(FTPSECRET.FTP_SERCRET_NAME))
        logging.info("creating directory on ftp")
        ftp_object = FtpUtils(ftp_secret_json.get('host'), ftp_secret_json.get('username'), ftp_secret_json.get('password'))

        if platform_type == PLATFORMTYPE.YAHOO_GEMINI_VERIZON:
            parent_directory_to_upload = PATH.FTP_PARENT_DIRECTORY_TO_YAHOO_VERIZON_FILES.format(formated_current_date)
        elif platform_type == PLATFORMTYPE.YAHOO_GEMINI_MAPPING:
            parent_directory_to_upload = PATH.FTP_PARENT_DIRECTORY_TO_YAHOO_MAPPING_FILES
        else:
            raise Exception('Error in finding the FTP upload directory')
        directory_on_ftp = parent_directory_to_upload
        # This should be replaced later. For testing , kept parent directory as test_dir. Later above line should be uncommented
        # directory_on_ftp = '/test_dir_yahoo_gemini/' + formated_current_date + '/'
        ftp_object.create_nested_directory(directory_on_ftp)
        return {
            "ftp_object": ftp_object,
            "directory_on_ftp": directory_on_ftp
        }

    def download_report(self, **kwargs):
        """
        Download gogle_analytics report function
        INPUT : {dict}
                download_folder_path - path to the download foleder
                report_config(json file)
                    {
                        "report_name": "name of the report",
                        "account_id": "id of the account"
                    }
        OUTPUT: Download fiile in csv format in specified folder.
        """
        try:
            selenium_object = None
            logging.info('Yahoo Gemini report download started')

            logging.info("fetching from secret manager")

            # Fetch username and password from AWS secret
            aws_secret_manager_object = AwsSecretManager()
            yahoo_secret_json = json.loads(aws_secret_manager_object.get_secret(AWS_SECRET_MANAGER.YAHOO_SECRET_NAME))
            username = yahoo_secret_json.get('username')
            password = yahoo_secret_json.get('password')

            report_config = kwargs.get('report_config')
            download_folder_path = kwargs.get('download_folder_path')

            account_name = report_config.get('account_name')
            date_range_to_choose = report_config.get('date_range')
            column_set = report_config.get('column_set')
            report_type = report_config.get('report_type')

            logging.info("Account name is {}".format(account_name))
            logging.info("column set is {}".format(column_set))
            logging.info("date_range is {}".format(date_range_to_choose))

            # get selenium object
            selenium_object = SeleniumUtils(download_path=download_folder_path)
            # initialize Yahoo Gemini utils object
            yahoo_gemini_utils_object = YahooGeminiUtils(selenium_object=selenium_object)
            # login function
            yahoo_gemini_utils_object.login(username, password)
            # create new report
            yahoo_gemini_utils_object.create_new_report(report_type)
            # select Account in report to be generated
            yahoo_gemini_utils_object.select_account(account_name)
            # select date range in report to be downloaded
            yahoo_gemini_utils_object.select_date_range(date_range_to_choose)
            # select report columns(Metrics and dimention)
            yahoo_gemini_utils_object.select_report_columns(column_set)
            # generate report
            yahoo_gemini_utils_object.generate_report()
            # check report is processed and avilable for download
            report_status = yahoo_gemini_utils_object.check_report_processing_status()
            if not report_status:
                raise Exception("Report processing failed")
            # click on report download
            yahoo_gemini_utils_object.download_report()
            # Check download is sucess and files are present in download folder
            yahoo_gemini_utils_object.check_download_status()
            # click on logout
            yahoo_gemini_utils_object.logout()
            # exit by closing browser
            selenium_object.exit()
            logging.info('Yahoo Gemini report download done')

            return True
        except Exception as e:
            logging.error("Error in downloading the report")
            if selenium_object:
                selenium_object.exit()
            raise Exception(e)

    def download_combine_and_upload_reports(self, **kwargs):
        """ Function that calls download and then combine csv files using pandas and upload the report into FTP server
            INPUT:
                json config file with account_name, account_id and profile_id
            OUTPUT:
                upload csv file on FTP.
        """
        try:
            logging.info("Pulling values from xcom")
            # Pull values from xcom
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_yahoo_gemini_directory_on_ftp_task')

            ftp_object = None
            directory_on_ftp = None
            if xcom_values:
                # Pull ftp_object and directory on ftp.
                ftp_object = xcom_values['ftp_object']
                directory_on_ftp = xcom_values['directory_on_ftp']
            # Fetch account name and platform type from config file
            account_name = kwargs.get('report_config').get('account_name')
            platform_type = kwargs.get('platform_type')
            logging.info("platform type is {}".format(platform_type))

            if platform_type == PLATFORMTYPE.YAHOO_GEMINI_VERIZON:
                output_file_name = PANDASSETTINGS.YAHOO_GEMINI_VERIZON_OUTPUT_FILE_NAME.format(account_name)
                report_destination_path = directory_on_ftp
                report_location = REPORTLOCATION.FTP
            elif platform_type == PLATFORMTYPE.YAHOO_GEMINI_MAPPING:
                # get the formated from and to date to upload the files into ftp server
                date_res, formated_from_date, formated_to_date = date_range(DATE.YAHOO_GEMINI_MAPPING_DOWNLOAD_DATE_RANGE, DATE.DATE_FORMAT)
                if not date_res:
                    logging.error("Error while finding the date_range")
                    return
                output_file_name = PANDASSETTINGS.YAHOO_GEMINI_MAPPING_OUTPUT_FILE_NAME.format(account_name=account_name, from_date=formated_from_date, to_date=formated_to_date)
                report_destination_path = directory_on_ftp
                report_location = REPORTLOCATION.FTP
            elif platform_type == PLATFORMTYPE.YAHOO_GEMINI_WEEKLY_ETRADE:
                formated_current_date = datetime.today().strftime(DATE.DATE_FORMAT)
                output_file_name = PANDASSETTINGS.YAHOO_GEMINI_WEEKLY_ETRADE_OUTPUT_FILE_NAME.format(formated_current_date)
                report_destination_path = os.environ['YAHOO_GEMINI_ETRADE_WEEKLY_REPORT_EMAIL_TO']
                report_location = REPORTLOCATION.EMAIL
            else:
                logging.error("Error in finding the FTP upload directory")
                return
            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(platform_type)
            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]
            kwargs = {'download_folder_path': download_folder, 'report_config': kwargs.get('report_config')}
            # Function to download the report
            download_res = self.download_report(**kwargs)
            if not download_res:
                raise Exception("Error in download_report function")

            # initialising pandas by passing download folder and full path to output file
            output_file_path_and_name = download_folder + output_file_name

            panadas_utils_object = PandasUtils()
            # Get all csv files in folder
            csv_files_path_list = FileUtils.get_all_files_in_folder(download_folder, FILETYPE.CSV_FILE_TYPE)
            csv_dataframe = panadas_utils_object.combine_files_to_dataframe(csv_files_path_list)
            # Select only required columns from csv and Re_order the columns.
            if platform_type == PLATFORMTYPE.YAHOO_GEMINI_WEEKLY_ETRADE:
                # Choose the required columns in order from the dataframe
                csv_dataframe = panadas_utils_object.select_and_rearrange_columns_from_dataframe(csv_dataframe, PANDASSETTINGS.YAHOO_GEMINI_WEEKLY_ETRADE_COLUMN_SET)

            # IF NO data is present in csv file then no need to upload that into FTP server
            if len(csv_dataframe) == 0:
                # remove the created temp directory
                temp_folder_object.clean_up()
                logging.info("Data is not present in the csv file")
                logging.info("since data is not present, file will not be uploaded into ftp")
                return

            if platform_type in [PLATFORMTYPE.YAHOO_GEMINI_VERIZON, PLATFORMTYPE.YAHOO_GEMINI_MAPPING]:
                # create csv file from dataframe
                csv_dataframe_after_formating_date = panadas_utils_object.convert_date_format_in_dataframe(
                    csv_dataframe,
                    PANDASSETTINGS.DAILY_AND_MAPPING_REPORT_DATE_COLUMN_NAME,
                    PANDASSETTINGS.DAILY_AND_MAPPING_REPORT_DATE_FORMAT)
                panadas_utils_object.create_xls_sheet_from_dataframe(csv_dataframe_after_formating_date, output_file_path_and_name)

                # validate the data for file to be uploaded
                final_dataframe = csv_dataframe_after_formating_date
                s3_file_name = output_file_name
                if platform_type == PLATFORMTYPE.YAHOO_GEMINI_VERIZON:
                    input_file_val_schema_path = PATH.YAHOO_GEMINI_VERIZON_SCHEMA
                else:
                    input_file_val_schema_path = PATH.YAHOO_GEMINI_MAPPING_SCHEMA
                dv_obj = DataValidationsUtils(platform=platform_type, file_name_for_s3=s3_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

                ftp_object.put(output_file_path_and_name, directory_on_ftp + output_file_name)
                logging.info("Files are uploaded into FTP server")
            elif platform_type == PLATFORMTYPE.YAHOO_GEMINI_WEEKLY_ETRADE:
                # create csv file from dataframe
                panadas_utils_object.create_csv_file_from_dataframe(csv_dataframe, output_file_path_and_name)

                # validate the data for file to be uploaded
                final_dataframe = csv_dataframe
                s3_file_name = output_file_name
                input_file_val_schema_path = PATH.YAH00_GEMINI_ETRADE_WEEKLY_SCHEMA
                dv_obj = DataValidationsUtils(platform=platform_type, file_name_for_s3=s3_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

                # Fetch username, password and host from AWS secret
                aws_secret_manager_object = AwsSecretManager()
                email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

                logging.info("Sending email")

                # Message body for sending email
                email_message_body = '\n\n'.join(EMAIL.YAHOO_GEMINI_WEEKLY_ETRADE_MESSAGE_BODY.split('|'))

                # Get email Utils object by passing host, username, password
                email_utils_object = EmailUtils(email_config)
                email_utils_object.send(
                    os.environ['YAHOO_GEMINI_ETRADE_WEEKLY_REPORT_EMAIL_TO'],
                    EMAIL.YAHOO_GEMINI_WEEKLY_ETRADE_MESSAGE_SUBJECT,
                    email_message_body,
                    msg_attachments=[output_file_path_and_name]
                )

            else:
                raise Exception('Error in getting the platform type')

            logging.info("deleting temp folder")
            # remove the created temp directory
            temp_folder_object.clean_up()
            logging.info("completed")

            # Create and Upload report status
            # Writing success details into success report and uplod into s3
            upload_report_status_res = create_report_object.success_report_status_to_s3(
                account_name,
                report_destination_path,
                location=report_location)
            logging.info("result {}".format(upload_report_status_res))
            logging.info("completed")

        except Exception as e:
            logging.error("Error in download_combine_and_upload_reports function")
            try:
                logging.info("Removing temp directory")
                temp_folder_object.clean_up()
            except Exception:
                pass
            # Writing error details into failure report and uplod into s3
            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                account_name,
                e,
                report_destination_path,
                location=report_location)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
