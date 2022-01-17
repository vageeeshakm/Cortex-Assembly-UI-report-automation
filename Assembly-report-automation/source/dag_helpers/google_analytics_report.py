import os
import json
import logging
from datetime import datetime
from config.selenium_config import BROWSERTYPE
from config.google_analytics_config import (PATH, DATE, PANDASSETTINGS, AWS_SECRET_MANAGER)
from config.client_config import CLIENT_NAME
from config.ftp_config import FTPSECRET
from config.file_config import FILETYPE
from config.report_status_config import PLATFORM
from utils.common.time_utils import date_range
from utils.datasources.google.google_analytics_utils import GoogleAnalyticsUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.pandas_utils import PandasUtils
from utils.common.data_validations import DataValidationsUtils
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.ftp_utils import FtpUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.selenium.selenium_utils import SeleniumUtils
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class GoogleAnalyticsReport():
    """ class with function to download Google analytics report"""

    def create_directory_on_ftp(self, **kwargs):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_created
        """
        logging.info("fetching from secret manager")

        aws_secret_manager_object = AwsSecretManager()
        ftp_secret_json = json.loads(aws_secret_manager_object.get_secret(FTPSECRET.FTP_SERCRET_NAME))
        logging.info("creating directory on ftp")
        ftp_object = FtpUtils(ftp_secret_json.get('host'), ftp_secret_json.get('username'), ftp_secret_json.get('password'))

        # parent_directory_to_upload = PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES_PULTE
        parent_directory_to_upload = kwargs.get('parent_ftp_directory', PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES_PULTE)
        formated_current_date = datetime.today().strftime(DATE.DATE_FORMAT)
        directory_on_ftp = parent_directory_to_upload + formated_current_date + '/'

        # This should be replaced later. For testing , kept parent directory as test_dir. Later above line should be uncommented
        # directory_on_ftp = '/test_dir_google_analytics/' + formated_current_date + '/'
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
            logging.info('Google anlytics report download started')

            logging.info("fetching from secret manager")
            # Call to AWS secret manager to fetch credentials
            aws_secret_manager_object = AwsSecretManager()
            google_analytics_secret_json = json.loads(aws_secret_manager_object.get_secret(AWS_SECRET_MANAGER.GOOGLE_ANALYTICS_SECRET_NAME))
            # Fetch username and password from AWS secret
            username = google_analytics_secret_json.get('username')
            password = google_analytics_secret_json.get('password')

            download_folder_path = kwargs.get('download_folder_path')
            report_config = kwargs.get('report_config')
            account_id = report_config.get('report_id')
            profile_id = report_config.get('profile_id')
            report_type = report_config.get('report_type')
            report_template_name = report_config.get('report_template_name')
            platform = kwargs.get('platform')

            # By default, the date range is last 7 days.
            # If different date range, data needs to be downloaded, custom date range must be chosen.
            date_range = report_config.get('date_range')
            change_date_range = date_range if date_range != 'D:7' else None

            logging.info("Report id is -- {}".format(account_id))
            # Maximum number of rows to be selected in a page while downloading.
            maximim_rows_to_select_in_page = 5000
            # get selenium object

            selenium_object = SeleniumUtils(download_path=download_folder_path)

            # initialize GoogleAnalyticsUtils object
            google_analytics_utils_obj = GoogleAnalyticsUtils(selenium_object=selenium_object)
            # login function
            google_analytics_utils_obj.login(username, password)
            # click on customization section on google analytics report
            google_analytics_utils_obj.report_customization_section()
            # select report type
            google_analytics_utils_obj.select_report_type(report_type)
            # select account type to be download
            google_analytics_utils_obj.select_analytics_account(account_id, profile_id)
            # load report that to be downloaded
            google_analytics_utils_obj.load_custom_report(report_template_name, report_type)

            # select number of rows that to be displayed on page
            google_analytics_utils_obj.report_dispaly_rows_setting(maximim_rows_to_select_in_page)

            # Choose custom dates if needed
            if change_date_range:
                google_analytics_utils_obj.select_custom_date_range(date_range)

            # paginartion function to iterate and download all the pages
            google_analytics_utils_obj.paginate_and_download_report(maximim_rows_to_select_in_page, download_folder_path)
            # switch out of iframe
            google_analytics_utils_obj.switch_out_iframe()
            # click on google account setting
            google_analytics_utils_obj.open_google_analytics_account_setting()
            # Check download is sucess and files are present in download folder
            google_analytics_utils_obj.check_download_status()
            # click on logout
            google_analytics_utils_obj.logout()
            # exit by closing browser
            selenium_object.exit()
            logging.info('Google analytics report download done')

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
            # Get platform type
            platform = kwargs.get('platform')
            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(platform)

            logging.info("Pulling values from xcom")

            # Pull values from xcom
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_google_directory_on_ftp_task')

            # Pull ftp_object and directory on ftp.
            ftp_object = xcom_values['ftp_object']
            directory_on_ftp = xcom_values['directory_on_ftp']

            # get account_name and profile id
            account_name = kwargs.get('report_config').get('report_name')
            profile_id = kwargs.get('report_config').get('profile_id')
            host_name = kwargs.get('report_config').get('host_name', None)
            download_date_range = kwargs.get('report_config').get('date_range')
            client_name = kwargs.get('report_config').get('client')
            logging.info("Host name to filter is {}".format(host_name))

            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]

            kwargs = {'download_folder_path': download_folder, 'report_config': kwargs.get('report_config'), 'platform': platform}
            # Function to download the report
            download_res = self.download_report(**kwargs)
            if not download_res:
                raise Exception("Error in download_report function")

            # specifies the output file name after modification by pandas
            if client_name == CLIENT_NAME.PULTE:
                # Get from_date and to_date which will be used in output report name
                date_res, formated_from_date, formated_to_date = date_range(download_date_range, DATE.DATE_FORMAT)
                output_file_name = PANDASSETTINGS.PULTE_OUTPUT_FILE_NAME.format(account_name=account_name, from_date=formated_from_date, to_date=formated_to_date)
            elif client_name == CLIENT_NAME.INNOVAGE:
                # Get from_date and to_date that will be used in output report name
                date_res, formated_from_date, formated_to_date = date_range(download_date_range, DATE.DATE_FORMAT, consider_current_date_as_end_date=True)
                output_file_name = PANDASSETTINGS.INNOVAGE_OUTPUT_FILE_NAME.format(account_name=account_name, from_date=formated_from_date, to_date=formated_to_date)
            output_file_path_and_name = download_folder + output_file_name

            # initialising pandas by passing download folder and full path to output file
            panadas_utils_object = PandasUtils()
            # get all csv files in folder
            csv_files_path_list = FileUtils.get_all_files_in_folder(download_folder, FILETYPE.CSV_FILE_TYPE)

            # Read pandas dataframe from csv file
            starting_rows_in_csv_file_dataframe = panadas_utils_object.get_dataframe_from_csv(
                csv_files_path_list[0],
                csv_header=None,
                number_of_starting_rows_to_consider=PANDASSETTINGS.NUMBER_OF_ROWS_TO_SKIP - 1)
            # Insert empty columns into dataframe
            for each_column in range(0, PANDASSETTINGS.NUMBER_OF_EMPTY_COLUMNS_TO_INSERT):
                starting_rows_in_csv_file_dataframe = panadas_utils_object.insert_column_in_csv(starting_rows_in_csv_file_dataframe, each_column + 1, each_column + 1, None)

            # Insert an empty row at the end of dataframe
            starting_rows_in_csv_file_dataframe = panadas_utils_object.insert_row_in_dataframe(starting_rows_in_csv_file_dataframe, len(starting_rows_in_csv_file_dataframe), None)
            if len(csv_files_path_list) > 1:
                # Skip starting rows in csv file
                csv_dataframe_with_full_data = panadas_utils_object.combine_files_to_dataframe(csv_files_path_list, PANDASSETTINGS.NUMBER_OF_ROWS_TO_SKIP)
            else:
                csv_dataframe_with_full_data = panadas_utils_object.get_dataframe_from_csv(csv_files_path_list[0], number_of_starting_rows_to_skip=PANDASSETTINGS.NUMBER_OF_ROWS_TO_SKIP - 1)

            if client_name == CLIENT_NAME.PULTE:
                # Insert profile id
                dataframe_after_insert = panadas_utils_object.insert_column_in_csv(csv_dataframe_with_full_data,
                                                                                   PANDASSETTINGS.INSERT_COLUMN_INDEX,
                                                                                   PANDASSETTINGS.COLUMN_NAME_TO_INSERT,
                                                                                   value_to_insert=profile_id)

                # Filter host names
                dataframe_after_filter = panadas_utils_object.filter_value_from_dataframe(
                    dataframe_after_insert,
                    PANDASSETTINGS.HOST_COLUMN_NAME_TO_FILTER,
                    host_name)

                # create a new csv file after processing using pandas.
                panadas_utils_object.create_csv_file_from_dataframe(starting_rows_in_csv_file_dataframe, output_file_path_and_name, require_header=False)
                panadas_utils_object.create_csv_file_from_dataframe(dataframe_after_filter, output_file_path_and_name, writing_mode='a')

                # validate the data for file to be uploaded
                final_dataframe = panadas_utils_object.get_dataframe_from_csv(output_file_path_and_name, number_of_starting_rows_to_skip=PANDASSETTINGS.NUMBER_OF_ROWS_TO_SKIP)

                input_file_val_schema_path = PATH.GOOGLE_ANALYTICS_PULTE_SCHEMA
                dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=output_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=output_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

            elif client_name == CLIENT_NAME.INNOVAGE:

                # create a new csv file after processing using pandas.
                panadas_utils_object.create_csv_file_from_dataframe(starting_rows_in_csv_file_dataframe, output_file_path_and_name, require_header=False)
                panadas_utils_object.create_csv_file_from_dataframe(csv_dataframe_with_full_data, output_file_path_and_name, writing_mode='a')

                # validate the data for file to be uploaded
                final_dataframe = panadas_utils_object.get_dataframe_from_csv(output_file_path_and_name, number_of_starting_rows_to_skip=PANDASSETTINGS.NUMBER_OF_ROWS_TO_SKIP)

                input_file_val_schema_path = PATH.GOOGLE_ANALYTICS_INNOVAGE_SCHEMA_MAPPING[account_name]
                dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=output_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=output_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

            logging.info("uploading csv file into ftp server - " + str(directory_on_ftp + output_file_name))
            # Upload the final csv file into ftp server
            ftp_object.put(output_file_path_and_name, directory_on_ftp + output_file_name)

            logging.info("csv file uploaded into ftp server")
            logging.info("deleting temp folder")

            # remove the created temp directory
            temp_folder_object.clean_up()

            # Create and Upload report status
            # Writing success details into success report and uplod into s3
            upload_report_status_res = create_report_object.success_report_status_to_s3(
                account_name,
                directory_on_ftp)
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
                directory_on_ftp)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
