import os
import json
import logging

from datetime import datetime
from config.zillow_pulte_config import (
    AWS_SECRET_MANAGER,
    DATE,
    PATH,
    PANDASSETTINGS
)
from config.file_config import FILETYPE
from config.report_status_config import PLATFORM, REPORTLOCATION
from utils.common.data_validations import DataValidationsUtils
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.pandas_utils import PandasUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.aws_s3_utils import AwsS3Utils
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus
from utils.datasources.zillow_pulte.zillow_pulte_utils import ZillowPulteUtils
from utils.selenium.selenium_utils import SeleniumUtils

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class ZillowPulteReport():
    """ class with functions to download and upload report"""

    def download_report(self, download_folder_path, params):
        """downloads the report from the zillow UI
        INPUT : download_folder_path - path to the download folder
        """
        try:
            logging.info("fetching credentials from secret manager")

            # Fetch username and password from AWS secret
            aws_secret_manager_object = AwsSecretManager()
            zillow_pulte_secret_json = json.loads(aws_secret_manager_object.get_secret(AWS_SECRET_MANAGER.SECRET_NAME))
            username = zillow_pulte_secret_json.get('username')
            password = zillow_pulte_secret_json.get('password')

            # get selenium object
            selenium_object = SeleniumUtils(download_path=download_folder_path)
            # initialize LinkedIn utils object
            zillow_pulte_utils_object = ZillowPulteUtils(selenium_object=selenium_object)
            # login function
            zillow_pulte_utils_object.login(username, password)
            # select filters in UI and download report
            zillow_pulte_utils_object.select_filters_and_download(params)
            # checks the status of the report
            zillow_pulte_utils_object.check_download_status()
            # logs out of the zillow platform
            zillow_pulte_utils_object.logout()
            # exit by closing browser
            selenium_object.exit()

            logging.info('Zillow Pulte report download done')

        except Exception as e:
            logging.error("Error in downloading the report")
            if selenium_object:
                selenium_object.exit()

            raise Exception(e)

    def format_report_data(self, download_folder_path):
        """ formats the data present in the report
        INPUT : download_folder_path - path of the downloaded folder
        OUTPUT: returns dataframe if successful else raises exception
        """
        try:
            pandas_utils_object = PandasUtils()

            # gets the downloaded file from the folder
            xls_files_path_list = FileUtils.get_all_files_in_folder(download_folder_path, FILETYPE.EXCEL_FILE_TYPE)

            # gets the dataframe from the excel sheet
            report_dataframe = pandas_utils_object.get_dataframe_from_excel(xls_files_path_list[0], PANDASSETTINGS.SHEET_NAME, PANDASSETTINGS.DATE_COLUMNS_LIST_IN_SHEET)
            # filter the data
            report_dataframe = pandas_utils_object.filter_value_from_dataframe(report_dataframe, PANDASSETTINGS.CHANNEL_FILTER_COLUMN, PANDASSETTINGS.CHANNEL_FILTER_VALUE)
            # rounds decimal in the data
            report_dataframe = pandas_utils_object.round_decimal_value_for_columns(report_dataframe, PANDASSETTINGS.COLUMNS_TO_ROUND_OFF, PANDASSETTINGS.DECIMAL_DIGITS)

            return report_dataframe

        except Exception as e:
            logging.error("Error in formatting zillow report data")
            raise Exception(e)

    def upload_file_to_s3(self, report_dataframe, s3_bucket, s3_file_path):
        """ saves dataframe as csv and uploads the file to s3
        INPUT : report_dataframe - dataframe of the report
        """
        try:
            aws_s3_object = AwsS3Utils()
            aws_s3_object.dataframe_to_s3(report_dataframe, s3_bucket, s3_file_path, FILETYPE.CSV_FILE_TYPE)

        except Exception as e:
            logging.error("Error in uploading the file to s3")
            raise Exception(e)

    def download_and_upload_report(self, **kwargs):
        """ main function to download iui report and upload the it to S3"""
        try:
            # create the temp folder to download the file
            temp_folder_object = HandleTempDirectory()
            download_folder_path = temp_folder_object.generate_dirs()[0]

            params = kwargs['report_config']

            # Function to download the report
            logging.info("calling the download_report function")
            self.download_report(download_folder_path, params)

            # format the data in the downloaded report
            logging.info("calling the format_report_data function")
            report_dataframe = self.format_report_data(download_folder_path)

            # validate the data for file to be uploaded
            platform = kwargs.get('platform')
            final_dataframe = report_dataframe
            s3_file_name = params['s3_file_name']
            input_file_val_schema_path = PATH.ZILLOW_PULTE_SCHEMA
            dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=s3_file_name)
            dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
            dv_obj.write_validations_log_report()

            # upload the formatted report to s3
            s3_bucket = params['s3_bucket']
            s3_folder = params['s3_folder']
            current_date = datetime.today().strftime(DATE.DATE_FORMAT)
            s3_file_path = s3_folder + '/' + current_date + '/' + params['s3_file_name']
            logging.info("calling the upload_file_to_s3 function")
            self.upload_file_to_s3(report_dataframe, s3_bucket, s3_file_path)

            temp_folder_object.clean_up()

            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(PLATFORM.ZILLOW_PULTE)

            # Create and Upload success report status
            upload_report_status_res = create_report_object.success_report_status_to_s3(
                PLATFORM.ZILLOW_PULTE,
                s3_file_path,
                location=REPORTLOCATION.S3)
            logging.info("result {}".format(upload_report_status_res))

        except Exception as e:
            logging.error("Error in download_and_upload_report function")
            try:
                logging.info("Removing temp directory")
                temp_folder_object.clean_up()
            except Exception:
                pass

            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(PLATFORM.ZILLOW_PULTE)

            # Create and Upload failure report status
            s3_file_path = params['s3_folder'] + '/' + datetime.today().strftime(DATE.DATE_FORMAT) + '/' + params['s3_file_name']
            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                PLATFORM.ZILLOW_PULTE,
                PLATFORM.ZILLOW_PULTE,
                e,
                s3_file_path,
                location=REPORTLOCATION.S3)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
