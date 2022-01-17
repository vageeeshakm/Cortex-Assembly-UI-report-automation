import os
import json
import logging
from datetime import datetime
from config.dbm_config import (
    DATE,
    PATH,
    PANDASSETTINGS,
    ADVERTISERDETAILS
)
from config.email_config import EMAILSECRET
from config.file_config import FILETYPE
from config.ftp_config import FTPSECRET
from utils.common.data_validations import DataValidationsUtils
from utils.common.general_utils import split_function
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.email_utils import EmailUtils
from utils.common.ftp_utils import FtpUtils
from utils.common.pandas_utils import PandasUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class DbmReport():
    """ class with function to download DBM report"""

    def create_directory_on_ftp(self, **kwargs):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_on_ftp_for_dbm_report
        """
        logging.info("fetching ftom secret manager")
        formated_current_date = datetime.today().strftime(DATE.DATE_FORMAT)

        # Get FTP secret from secret manager
        aws_secret_manager_object = AwsSecretManager()
        ftp_secret_json = json.loads(aws_secret_manager_object.get_secret(FTPSECRET.FTP_SERCRET_NAME))
        logging.info("creating directory on ftp")
        ftp_object = FtpUtils(ftp_secret_json.get('host'), ftp_secret_json.get('username'), ftp_secret_json.get('password'))

        # Main directory to upload csv files
        parent_directory_to_upload = PATH.FTP_PARENT_DIRECTORY

        # This should be replaced later. For testing , kept parent directory as test_dir. Later above line should be uncommented
        # parent_directory_to_upload = '/test_dbm_report/'

        directory_on_ftp_for_assembly_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_DBM_ASSEMBLY_REPORT.format(current_date=formated_current_date)
        directory_on_ftp_for_sophie_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_DBM_SOPHIE_REPORT.format(current_date=formated_current_date)

        ftp_object.create_nested_directory(directory_on_ftp_for_assembly_report)
        ftp_object.create_nested_directory(directory_on_ftp_for_sophie_report)
        return {
            "ftp_object": ftp_object,
            "directory_on_ftp_for_assembly_report": directory_on_ftp_for_assembly_report,
            "directory_on_ftp_for_sophie_report": directory_on_ftp_for_sophie_report
        }

    def download_segregate_and_upload_reports(self, **kwargs):
        """ Function that downloads zip file from email, extract, process and upload into FTP
            INPUT:
                ftp_object: ftp object
                directory_on_ftp: directory on FTP to upload files
                json config file
                    "advertiser": Advertiser type
                    "email_subject": Email Subject to find and download the report
            OUTPUT:
                upload csv files into FTP.
        """
        try:
            # Create object to Create and Upload Validation Report class
            platform = kwargs.get('platform')
            create_report_object = CreateAndUploadReportStatus(platform)

            logging.info("Pulling values from xcom")
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_dbm_directory_on_ftp')

            ftp_object = None

            # Pull ftp_object and directory on ftp.
            ftp_object = xcom_values['ftp_object']
            directory_on_ftp_for_assembly_report = xcom_values['directory_on_ftp_for_assembly_report']
            directory_on_ftp_for_sophie_report = xcom_values['directory_on_ftp_for_sophie_report']

            # Fetch account name and platform type from config file
            advertiser = kwargs.get('report_config').get('advertiser')
            email_subject = kwargs.get('report_config').get('email_subject')
            logging.info("advertiser type is {}".format(advertiser))

            # Perform operation based on Advertiser type
            if advertiser == ADVERTISERDETAILS.ASSEMBLY_ADVERTISER_TYPE:
                logging.info("DBM ASSEMBLY advertiser")
                index_number_to_insert_rows = 0
                # Get row details to add into dataframe
                partner_id_name_mapping_details = PANDASSETTINGS.ASSEMBLY_PARTNER_NAME_ID_MAPPING_DICT.items()
                # FTP path to upload files
                ftp_upload_directory = directory_on_ftp_for_assembly_report

            elif advertiser == ADVERTISERDETAILS.SOPHIE_ADVERTISER_TYPE:
                logging.info("DBM SOPHIE advertiser")
                # Get row details to add into dataframe
                partner_id_name_mapping_details = PANDASSETTINGS.SOPHIE_PARTNER_NAME_ID_MAPPING_DICT.items()
                # FTP path to upload files
                ftp_upload_directory = directory_on_ftp_for_sophie_report

            else:
                ftp_upload_directory = None
                # If unknown advertiser in json config file
                logging.error("Unknown Advertiser specified in the input config file")
                return

            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]

            # String used to split the downloaded report name and get filename to be uploaded into FTP
            substring_to_split = 'automation_'

            # Get email config from secret manager
            aws_secret_manager_object = AwsSecretManager()
            email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

            # Download zip file and extract it into local directory
            # Initialise email ustils
            email_utils_object = EmailUtils(email_config)
            # Search for dbm report in email by passing from address and email subject and save attachement into local directory
            result_dict = email_utils_object.search_and_download(
                os.environ['DBM_DAILY_REPORT_EMAIL_ADDRESS_TO_SEARCH'],
                email_subject,
                local_path_to_save_file=download_folder
            )
            # Get downlaoded csv file name
            downloaded_csv_file_name = result_dict.get('output_file_name')

            # Get the filename which will be in second part of list after split
            output_csv_file_name = split_function(downloaded_csv_file_name, substring_to_split)[1]
            # Full name of csv file with path
            output_file_name_with_path = download_folder + output_csv_file_name

            # Initialise Pandas utils
            panadas_utils_object = PandasUtils()
            # Get csv file names in download folder
            csv_files_path_list = FileUtils.get_all_files_in_folder(download_folder, FILETYPE.CSV_FILE_TYPE)
            csv_file_name_with_full_path = csv_files_path_list[0]
            # Read csv file and get dataframe
            dataframe = panadas_utils_object.read_csv_using_python_engine(csv_file_name_with_full_path, number_of_footer_lines_to_skip=PANDASSETTINGS.NUMBER_OF_FOOTER_LINES_TO_SKIP)

            # Change the date format in the dataframe
            dataframe = panadas_utils_object.convert_date_format_in_dataframe(
                dataframe,
                PANDASSETTINGS.REPORT_DATE_COLUMN_NAME,
                PANDASSETTINGS.REPORT_DATE_FORMAT
            )

            # Insert new column into dataframe
            index_number_to_insert_column = 0
            for each_column in PANDASSETTINGS.COLUMN_NAMES_LIST_TO_INSERT:
                # Insert columns into dataframe
                dataframe = panadas_utils_object.insert_column_in_csv(dataframe, index_number_to_insert_column, each_column, value_to_insert='')
                # Increase index number to insert new column
                index_number_to_insert_column += 1

            index_number_to_insert_rows = 0

            # Insert rows to partner id, and name columns
            for partner_name, partner_id in partner_id_name_mapping_details:
                dataframe = panadas_utils_object.insert_row_in_dataframe(dataframe, index_number_to_insert_rows, partner_name, PANDASSETTINGS.COLUMN_NAMES_LIST_TO_INSERT[0])
                dataframe = panadas_utils_object.insert_row_in_dataframe(dataframe, index_number_to_insert_rows, partner_id, PANDASSETTINGS.COLUMN_NAMES_LIST_TO_INSERT[1])
                index_number_to_insert_rows += 1

            # Get index of last line in the dataframe
            total_number_of_rows, total_number_of_columns = panadas_utils_object.get_total_number_of_rows_and_columns_in_dataframe(dataframe)
            # Insert report filter details at the end of report
            for each_filter in PANDASSETTINGS.REPORT_FILTERS_LIST:
                dataframe = panadas_utils_object.insert_row_in_dataframe(dataframe,
                                                                         total_number_of_rows,
                                                                         'Filter by {filter_value}'.format(filter_value=each_filter),
                                                                         PANDASSETTINGS.REPORT_COLUMN_NAME_TO_ADD_FILTER_DETAILS)
                # After inserting new row, increment total number of rows
                total_number_of_rows += 1

            # Convert float into int datatype for required columns
            dataframe = panadas_utils_object.convert_float_into_int_datatype(dataframe, PANDASSETTINGS.COLUMNS_TO_CONVERT_INTO_INT)

            # convert dataframe into csv and save
            panadas_utils_object.create_csv_file_from_dataframe(dataframe, output_file_name_with_path)

            # validate the data for file to be uploaded
            final_dataframe = panadas_utils_object.read_csv_using_python_engine(output_file_name_with_path,
                                                                                number_of_footer_lines_to_skip=PANDASSETTINGS.NUMBER_OF_FOOTER_LINES_TO_SKIP_FOR_DATA_VALIATION)
            s3_file_name = output_csv_file_name
            input_file_val_schema_path = PATH.DBM_SCHEMA
            dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=s3_file_name)
            dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
            dv_obj.write_validations_log_report()

            # Upload file into FTP
            ftp_object.put(output_file_name_with_path, ftp_upload_directory + output_csv_file_name)

            logging.info("deleting temp folder")
            # remove the created temp directory
            temp_folder_object.clean_up()
            logging.info("completed")

            # Create and Upload report status
            # Writing success details into success report and uplod into s3
            upload_report_status_res = create_report_object.success_report_status_to_s3(
                advertiser,
                ftp_upload_directory)
            logging.info("result {}".format(upload_report_status_res))

        except Exception as e:
            logging.error("Error in download_combine_and_upload_reports function")
            try:
                logging.info("Removing temp directory")
                temp_folder_object.clean_up()
            except Exception:
                logging.error("Error in removing temp directory")
            # Writing error details into failure report and uplod into s3
            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                advertiser,
                e,
                ftp_upload_directory)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
