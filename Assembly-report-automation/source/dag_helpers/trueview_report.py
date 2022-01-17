import os
import json
import logging
from datetime import datetime
from config.trueview_config import (
    DATE,
    PATH,
    PANDASSETTINGS,
    REPORTTYPE,
    FILENAME
)
from config.email_config import EMAILSECRET
from config.file_config import FILETYPE
from config.ftp_config import FTPSECRET
from utils.common.general_utils import replace_function
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.email_utils import EmailUtils
from utils.common.ftp_utils import FtpUtils
from utils.common.pandas_utils import PandasUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus
from utils.common.data_validations import DataValidationsUtils

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class TrueviewReport():
    """ class with function to download Trueview report"""

    def create_directory_on_ftp(self, **kwargs):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_on_ftp_for_trueview_report
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
        # parent_directory_to_upload = '/test_trueview_report/'

        directory_on_ftp_for_trueview_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_TRUEVIEW_REPORT.format(current_date=formated_current_date)
        directory_on_ftp_for_trueview_conversion_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_CONVERSION_REPORT.format(current_date=formated_current_date)

        ftp_object.create_nested_directory(directory_on_ftp_for_trueview_report)
        ftp_object.create_nested_directory(directory_on_ftp_for_trueview_conversion_report)
        return {
            "ftp_object": ftp_object,
            "directory_on_ftp_for_trueview_report": directory_on_ftp_for_trueview_report,
            "directory_on_ftp_for_trueview_conversion_report": directory_on_ftp_for_trueview_conversion_report
        }

    def download_segregate_and_upload_reports(self, **kwargs):
        """ Function that downloads zip file from email, extract, process and upload into FTP
            INPUT:
                ftp_object: ftp object
                directory_on_ftp: directory on FTP to upload files
                json config file
                    "report_type": report type
                    "email_subject": Email Subject to find and download the report
            OUTPUT:
                upload csv files into FTP.
        """
        try:
            # Fetch platform type details from DAG
            platform = kwargs.get('platform')
            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(platform)
            advertiser_name = ''
            logging.info("Pulling values from xcom")
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_trueview_directory_on_ftp')

            # Pull ftp_object and directory on ftp.
            ftp_object = xcom_values['ftp_object']
            directory_on_ftp_for_trueview_report = xcom_values['directory_on_ftp_for_trueview_report']
            directory_on_ftp_for_trueview_conversion_report = xcom_values['directory_on_ftp_for_trueview_conversion_report']

            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]

            # Fetch account name and platform type from config file
            report_type = kwargs.get('report_config').get('report_type')
            email_subject = kwargs.get('report_config').get('email_subject')
            logging.info("report_type type is {}".format(report_type))

            formated_current_date = datetime.today().strftime(DATE.DATE_FORMAT)

            if report_type == REPORTTYPE.TRUEVIEW_REPORT_TYPE:
                # columns to rename in dataframe
                columns_to_rename = PANDASSETTINGS.TRUEVIEW_COLUMNS_TO_RENAME_DICT
                column_names_to_find_sum = PANDASSETTINGS.TRUEVIEW_COLUMNS_NAME_LIST_TO_FIND_SUM
                ftp_upload_directory = directory_on_ftp_for_trueview_report
                column_names_list_to_cast_data_type = PANDASSETTINGS.TRUEVIEW_COLUMNS_TO_CONVERT_INTO_INT
                input_file_val_schema_path = PATH.TRUEVIEW_SCHEMA

            elif report_type == REPORTTYPE.CONVERSION_REPORT_TYPE:
                # columns to rename in dataframe
                columns_to_rename = PANDASSETTINGS.CONVERSION_COLUMNS_TO_RENAME_DICT
                column_names_to_find_sum = PANDASSETTINGS.CONVERSION_COLUMNS_NAME_LIST_TO_FIND_SUM
                ftp_upload_directory = directory_on_ftp_for_trueview_conversion_report
                column_names_list_to_cast_data_type = PANDASSETTINGS.CONVERSION_COLUMNS_TO_CONVERT_INTO_INT
                input_file_val_schema_path = PATH.TRUEVIEW_CONVERSION_SCHEMA

            else:
                # If unknown REPORTTYPE in json config file
                logging.error("Unknown Report Type specified in the input config file")
                raise Exception("Unknown Report Type")

            # Get email config from secret manager
            aws_secret_manager_object = AwsSecretManager()
            email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

            # Download csv file into local directory
            # Initialise email ustils
            email_utils_object = EmailUtils(email_config)
            # Search for trueview report in email by passing from address and email subject and save attachement into local directory
            email_utils_object.search_and_download(
                os.environ['TRUEVIEW_DAILY_REPORT_EMAIL_ADDRESS_TO_SEARCH'],
                email_subject,
                local_path_to_save_file=download_folder
            )

            # Initialise Pandas utils
            panadas_utils_object = PandasUtils()
            # Get csv file names in download folder
            csv_files_path_list = FileUtils.get_all_files_in_folder(download_folder, FILETYPE.CSV_FILE_TYPE)
            csv_file_name_with_full_path = csv_files_path_list[0]

            # Read csv file and get dataframe
            dataframe = panadas_utils_object.read_csv_using_python_engine(csv_file_name_with_full_path, number_of_footer_lines_to_skip=PANDASSETTINGS.NUMBER_OF_FOOTER_LINES_TO_SKIP)

            # Rename columns in dataframe
            dataframe = panadas_utils_object.rename_columns_in_dataframe(dataframe, columns_to_rename)

            # Get index of last line in the dataframe
            total_number_of_rows, total_number_of_columns = panadas_utils_object.get_total_number_of_rows_and_columns_in_dataframe(dataframe)
            # Read footer lines in csv file
            common_footer_lines_in_dataframe = panadas_utils_object.get_dataframe_from_csv(csv_file_name_with_full_path, csv_header=None, number_of_starting_rows_to_skip=total_number_of_rows + 1)
            # drop first line in footer dataframe (Summarry line)
            common_footer_lines_in_dataframe = panadas_utils_object.select_rows_in_dataframe_using_index(
                common_footer_lines_in_dataframe,
                starting_index_value=PANDASSETTINGS.NUMBER_OF_STARTING_ROWS_TO_SKIP)
            # Add blanck row to footer with no data
            common_footer_lines_in_dataframe = panadas_utils_object.insert_row_in_dataframe(common_footer_lines_in_dataframe, len(common_footer_lines_in_dataframe) + 1, None)
            # Get column list in dataframe
            column_list_in_dataframe = panadas_utils_object.get_columns_in_dataframe(dataframe)
            # Specify column for footer lines
            footer_lines_dataframe_with_columns = panadas_utils_object.specify_columns_for_dataframe(common_footer_lines_in_dataframe, column_list_in_dataframe)
            # Get unique advertiser in report
            unique_advertisers = panadas_utils_object.get_unique_values_list_from_dataframe(dataframe, PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER)

            # Create sep report for each advertiser and upload that into FTP
            for each_advertiser in unique_advertisers:
                # Replace spaces with underscore
                advertiser_name = replace_function(each_advertiser, ' ', '_')
                logging.info("advertiser {}".format(advertiser_name))
                # Based on report type find ftp directory and filename to be uploaded on FTP
                # If Trueview Report type
                if report_type == REPORTTYPE.TRUEVIEW_REPORT_TYPE:
                    output_report_name = FILENAME.TRUEVIEW_REPORT_OUTPUT_FILENAME.format(advertiser=advertiser_name, current_date=formated_current_date)
                elif report_type == REPORTTYPE.CONVERSION_REPORT_TYPE:
                    output_report_name = FILENAME.CONVERSION_REPORT_OUTPUT_FILENAME.format(advertiser=advertiser_name, current_date=formated_current_date)
                else:
                    # If unknown REPORTTYPE in json config file
                    logging.error("Unknown REPORTTYPE specified in the input config file")
                    return
                output_file_name_with_path = download_folder + output_report_name
                logging.info("output_report_name is {}".format(output_report_name))

                each_advertiser_dataframe_after_filtering = dataframe[dataframe[PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER] == each_advertiser]

                # Find sum and append summarry row into dataframe
                column_with_sum_dict = panadas_utils_object.find_sum_for_columns(each_advertiser_dataframe_after_filtering, column_names_to_find_sum)

                # Append sum for summary row to dataframe
                dataframe_after_appending_summary_row = panadas_utils_object.append_to_datframe(each_advertiser_dataframe_after_filtering, column_with_sum_dict)

                # Convert float into int datatypes
                dataframe_after_appending_summary_row = panadas_utils_object.convert_float_into_int_datatype(dataframe_after_appending_summary_row, column_names_list_to_cast_data_type)

                # Append footer lines to dataframe
                dataframe_with_footer_lines = panadas_utils_object.append_to_datframe(dataframe_after_appending_summary_row, footer_lines_dataframe_with_columns)

                # Drop column Advertiser from dataframe
                dataframe_after_droping_advertiser_name = panadas_utils_object.drop_columns_from_dataframe(dataframe_with_footer_lines, [PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER])

                # convert dataframe into csv and save
                panadas_utils_object.create_csv_file_from_dataframe(dataframe_after_droping_advertiser_name, output_file_name_with_path)

                logging.info("Uploading file into FTP")

                # Upload into FTP
                ftp_object.put(output_file_name_with_path, ftp_upload_directory + output_report_name)
                logging.info("Files uploaded into FTP")

                # validate the data for file to be uploaded
                final_dataframe = panadas_utils_object.read_csv_using_python_engine(output_file_name_with_path, number_of_footer_lines_to_skip=PANDASSETTINGS.NUMBER_OF_FOOTER_LINES_TO_SKIP)

                ftp_file_name = output_report_name
                dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=ftp_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=ftp_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

                # Create and Upload report status
                upload_report_status_res = create_report_object.success_report_status_to_s3(
                    report_type,
                    ftp_upload_directory,
                    report_type + advertiser_name)
                logging.info("result {}".format(upload_report_status_res))

            logging.info("deleting temp folder")
            # remove the created temp directory
            temp_folder_object.clean_up()
            logging.info("completed")

        except Exception as e:
            logging.error("Error in download_combine_and_upload_reports function")
            try:
                logging.info("Removing temp directory")
                temp_folder_object.clean_up()
            except Exception:
                logging.error("Error while removing temp directory")
            # Writing error details into failure report and uplod into s3
            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                report_type,
                e,
                ftp_upload_directory,
                report_type + advertiser_name)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
