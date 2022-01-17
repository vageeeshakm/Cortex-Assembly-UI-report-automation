import os
import json
import logging
from random import randrange
from time import sleep
from datetime import datetime
from config.pinterest_config import PATH, DATE, PANDASSETTINGS, TEXT
from config.report_status_config import PLATFORM
from config.ftp_config import FTPSECRET
from config.file_config import FILETYPE
from config.email_config import EMAILSECRET
from utils.common.data_validations import DataValidationsUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.pandas_utils import PandasUtils
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.ftp_utils import FtpUtils
from utils.common.file_read_write import read_json_file
from utils.common.email_utils import EmailUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus
from utils.datasources.pinterest.pinterest_utils import PinterestUtils
from utils.selenium.selenium_utils import SeleniumUtils

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class PinterestReport():
    """ class with function to download and upload Pinterest report"""

    def create_directory_on_ftp(self):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_created
        """
        logging.info("fetching from secret manager")

        logging.info("TIME {}".format(datetime.now()))
        # Fetch secret from AWS secret manager
        aws_secret_manager_object = AwsSecretManager()
        ftp_secret_json = json.loads(aws_secret_manager_object.get_secret(FTPSECRET.FTP_SERCRET_NAME))

        logging.info("creating directory on ftp")
        # initialise by calling Ftp utils
        # pass host, username and password
        ftp_object = FtpUtils(ftp_secret_json.get('host'), ftp_secret_json.get('username'), ftp_secret_json.get('password'))

        # Main directory to upload csv files
        parent_directory_to_upload = PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES
        formated_current_date = datetime.today().strftime(DATE.YMD_DATE_FORMAT)
        directory_on_ftp = parent_directory_to_upload + formated_current_date + '/'

        # This should be replaced later. For testing , kept parent directory as test_dir. Later above line should be uncommented
        # directory_on_ftp = '/test_dir_pinterest/' + formated_current_date + '/'

        # create directory if not exist
        ftp_object.create_nested_directory(directory_on_ftp)
        # returns ftp_object and directory which is created to upload the csv files
        return {
            "ftp_object": ftp_object,
            "directory_on_ftp": directory_on_ftp
        }

    def download_report(self, **kwargs):
        """
        Download pinterest report function
        INPUT : {dict}
                template_name_and_url_list - Template name and url list
                download_folder_path - path to download the reports
                create_report_object - object to write report status
                directory_on_ftp - directory on FTP to where report will be uploaded
        OUTPUT: Download fiile in csv format.
        """
        try:
            selenium_object = None
            logging.info('Pinterest report download started')

            # Fetch download path and url
            download_folder_path = kwargs.get('download_folder_path')
            template_name_and_url_list = kwargs.get("template_name_and_url_list")

            # Report Object to upload Report Status
            create_report_object = kwargs.get("create_report_object")
            # directory on FTP
            directory_on_ftp = kwargs.get("directory_on_ftp")

            selenium_object = SeleniumUtils(download_path=download_folder_path)
            pinterest_utils_obj = PinterestUtils(selenium_object=selenium_object)

            number_of_files_to_present = 0

            # Download Report one by one using Link
            for each_url_and_name_dict in template_name_and_url_list:

                template_name = each_url_and_name_dict.get('template_name')
                download_url = each_url_and_name_dict.get('download_url')

                try:
                    pinterest_utils_obj.download_report(download_url)
                    number_of_files_to_present += 1
                    # check if file downloaded
                    pinterest_utils_obj.check_download_status(number_of_files_to_present)
                except Exception as e:
                    # Exception
                    logging.error(f"Error While downloading the report --> {e}")

                    upload_report_status_res = create_report_object.failure_report_status_to_s3(
                        template_name,
                        "Report not downloaded using the provided url",
                        directory_on_ftp)

                    # Since This report is not downloaded remove one count which was added previously
                    number_of_files_to_present -= 1

            # exit and close browser after all the reports are downloaded
            selenium_object.exit()
            logging.info('Pinterest report download done')
            return True
        except Exception as e:
            logging.error("Error in downloading the report")
            if selenium_object:
                selenium_object.exit()
            raise Exception(e)

    def fetch_download_url_and_name_from_email_body(self, email_body):
        """ Function to get download url from body text and return that url"""
        # Get body text
        email_body_text = email_body.get('text')
        # decode bytes to string
        email_body_text = email_body_text.decode("utf-8")
        # Split the body text using predecided string value
        text_list = email_body_text.split(TEXT.STRING_TO_SPLIT)
        # Get the download URl
        download_url = text_list[-1].split("\n")[0]

        template_name_list = email_body_text.split(TEXT.DOWNLAOD_TEXT)
        template_name = template_name_list[-1].split(TEXT.REPORT_TEXT)[0].strip('"')

        template_name_and_url_dict = {
            "template_name": template_name,
            "download_url": download_url
        }
        # Return name and download url dict
        return template_name_and_url_dict

    def download_combine_and_upload_reports(self, **kwargs):
        """ Function that calls download and then combine csv files using pandas and upload the report into FTP server
            INPUT:
                json config file download duration, report_name and account_id
            OUTPUT:
                upload csv file on FTP server
        """
        try:
            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(PLATFORM.PINTEREST)
            logging.info("Pulling values from xcom")
            account_name = ""

            # Pull values from xcom
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_pinterest_directory_on_ftp_task')

            # Pull ftp_object and directory on ftp.
            ftp_object = xcom_values['ftp_object']
            directory_on_ftp = xcom_values['directory_on_ftp']

            # Email subject
            email_subject = TEXT.EMAIL_SUBJECT

            # Get email config from secret manager
            aws_secret_manager_object = AwsSecretManager()
            email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

            # Get Download link from email
            # Initialise email ustils
            email_utils_object = EmailUtils(email_config)
            # Search for link in email by passing from and email subject
            serach_res_list = email_utils_object.search(
                os.environ['PINTEREST_FROM_EMAIL_TO_SEARCH'],
                email_subject
            )
            # Download url list
            template_name_and_url_list = []

            for each_result in serach_res_list:
                template_name_and_url_dict = self.fetch_download_url_and_name_from_email_body(each_result)
                template_name_and_url_list.append(template_name_and_url_dict)

            # get platform name
            platform = kwargs.get('platform')

            # Current date in YMD format
            formated_current_date = datetime.today().strftime(DATE.YMD_DATE_FORMAT)

            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]

            # Arguments
            kwargs = {'download_folder_path': download_folder,
                      'template_name_and_url_list': template_name_and_url_list,
                      'directory_on_ftp': directory_on_ftp,
                      'create_report_object': create_report_object}

            download_res = self.download_report(**kwargs)
            if not download_res:
                temp_folder_object.clean_up()
                raise Exception("Error in download_report function")

            sleep(5)

            # Input schema path
            input_file_val_schema_path = PATH.PINTEREST_SCHEMA

            # initialising pandas by passing download folder and full path to output file
            panadas_utils_object = PandasUtils()

            input_config_list = read_json_file(file_path=PATH.JSON_FILE_PATH)

            for ecah_account_details in input_config_list:

                # Format, Validate and Upload reports one by one
                account_id = ecah_account_details.get("account_id")
                logging.info(f"account id is --> {account_id}")

                account_name = ecah_account_details.get("report_name")
                logging.info(f"account name is --> {account_name}")

                # Fetch Downlaoded file name using Template name
                downloaded_csv_file_name = ecah_account_details.get("template_name") + FILETYPE.CSV_FILE_TYPE
                logging.info(f"downloaded csv file name is --> {downloaded_csv_file_name}")

                # specifies the output file name after modification by pandas
                output_file_name = PANDASSETTINGS.OUTPUT_FILE_NAME.format(account_id, account_name, formated_current_date)
                output_file_path_and_name = download_folder + output_file_name

                try:
                    # Get csv file
                    csv_dataframe = panadas_utils_object.get_dataframe_from_csv(download_folder + downloaded_csv_file_name)

                    # Rename columns in dataframe
                    csv_dataframe = panadas_utils_object.rename_columns_in_dataframe(csv_dataframe, PANDASSETTINGS.COLUMNS_TO_RENAME_DICT)

                    # select columns from dataframe in specified order
                    csv_dataframe_after_ordering_columns = panadas_utils_object.select_and_rearrange_columns_from_dataframe(csv_dataframe, PANDASSETTINGS.COLUMNS_TO_SELECT_IN_ORDER)

                    # change the date format in dataframe
                    csv_dataframe_after_formating_date = panadas_utils_object.convert_date_format_in_dataframe(
                        csv_dataframe_after_ordering_columns,
                        PANDASSETTINGS.COLUMNS_TO_SELECT_IN_ORDER[6],
                        PANDASSETTINGS.DATE_FORMAT_IN_REPORT)

                    # create csv file from dataframe
                    panadas_utils_object.create_csv_file_from_dataframe(csv_dataframe_after_formating_date, output_file_path_and_name)

                    # validate the data for file to be uploaded
                    final_dataframe = csv_dataframe_after_formating_date.copy()
                    ftp_file_name = output_file_name

                    dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=ftp_file_name)
                    dv_obj.validate_data(input_df=final_dataframe, input_file_name=ftp_file_name, input_file_val_schema_path=input_file_val_schema_path)
                    dv_obj.write_validations_log_report()

                    # Upload the local file into ftp.
                    ftp_object.put(output_file_path_and_name, directory_on_ftp + output_file_name)

                    # Create and Upload report status
                    upload_report_status_res = create_report_object.success_report_status_to_s3(
                        account_name,
                        directory_on_ftp)
                    logging.info("result {}".format(upload_report_status_res))

                    logging.info("Files are uploaded into FTP server")
                    logging.info("deleting temp folder")
                    logging.info("Temp folder deleted")
                except Exception as e:
                    logging.error(f"Error While finding or processing the report for account {account_name}")
                    logging.error(f" Error is {e}")
                    upload_report_status_res = create_report_object.failure_report_status_to_s3(
                        account_name,
                        e,
                        directory_on_ftp)
                    logging.info("result {}".format(upload_report_status_res))

            # Delete the temporary directory
            temp_folder_object.clean_up()

        except Exception as e:
            logging.error("Error in download_combine_and_upload_reports function")
            try:
                logging.info("Removing temp directory")
                temp_folder_object.clean_up()
            except Exception:
                logging.error("Error in removing temp directory")

            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                account_name,
                e,
                directory_on_ftp)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
