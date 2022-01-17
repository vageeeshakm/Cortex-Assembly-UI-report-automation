import logging

from config.report_status_config import (
    PATH,
    S3,
    S3DATE,
    FAILUREREPORTCOLUMNS,
    SUCCESSREPORTCOLUMNS,
    REPORTLOCATION
)
from utils.common.time_utils import get_todays_date_string, get_current_datetime
from utils.common.pandas_utils import PandasUtils
from utils.common.aws_s3_utils import AwsS3Utils


class CreateAndUploadReportStatus:
    """ Create and upload validation report class"""

    def __init__(self, platform_name):
        """ Initialise"""
        self.s3_utils_object = AwsS3Utils()
        self.panadas_utils_object = PandasUtils()
        self.formated_current_date = get_todays_date_string(S3DATE.DATE_FORMAT)
        self.main_folder_on_s3 = PATH.MAIN_FOLDER_ON_S3.format(current_date=self.formated_current_date, platform=platform_name)
        self.bucket_name = S3.BUCKET_NAME

    def success_report_status_to_s3(self, account_name, uploaded_location, advertiser_name=None, location=REPORTLOCATION.FTP):
        """ Function to create and upload success reports"""
        try:
            if not advertiser_name:
                advertiser_name = account_name
            path_to_upload_success_reports = self.main_folder_on_s3 + PATH.SUCCESS_VALIDATION_REPORTS_FOLDER + PATH.REPORT_NAME.format(advertiser=advertiser_name)
            logging.info("path to upload report status {}".format(path_to_upload_success_reports))

            # Create dictionory with Header and values
            report_summary_dict = {
                SUCCESSREPORTCOLUMNS.ADVERTISER: [advertiser_name],
                SUCCESSREPORTCOLUMNS.ACCOUNT: [account_name],
                SUCCESSREPORTCOLUMNS.UPLOAD_PATH: [uploaded_location],
                SUCCESSREPORTCOLUMNS.LOCATION: location,
                SUCCESSREPORTCOLUMNS.REPORT_GENERATED_TIME: [str(get_current_datetime())]}

            # Convert report summary dict into dataframe
            dataframe = self.panadas_utils_object.convert_dictionry_into_dataframe(report_summary_dict)
            # Upload report summary detauls into s3
            self.s3_utils_object.dataframe_to_s3(dataframe, self.bucket_name, path_to_upload_success_reports)
            # return True if success
            return True
        except Exception as e:
            logging.error("Error: {}".format(e))
            return False

    def failure_report_status_to_s3(self, advertiser_name, error_message, path_to_upload, account_name=None, location=REPORTLOCATION.FTP):
        """ Function to create and upload Failed reports"""
        try:
            if not account_name:
                account_name = advertiser_name
            path_to_upload_failed_reports = self.main_folder_on_s3 + PATH.FAILURE_VALIDATION_REPORTS_FOLDER + PATH.REPORT_NAME.format(advertiser=advertiser_name)
            logging.info("path to upload report status {}".format(path_to_upload_failed_reports))

            # Create dictionory with Header and values
            report_summary_dict = {
                FAILUREREPORTCOLUMNS.ADVERTISER: advertiser_name,
                FAILUREREPORTCOLUMNS.ACCOUNT: account_name,
                SUCCESSREPORTCOLUMNS.UPLOAD_PATH: path_to_upload,
                FAILUREREPORTCOLUMNS.ERROR: error_message,
                FAILUREREPORTCOLUMNS.LOCATION: location,
                FAILUREREPORTCOLUMNS.REPORT_GENERATED_TIME: [str(get_current_datetime())]}

            # Convert report summary dict into dataframe
            dataframe = self.panadas_utils_object.convert_dictionry_into_dataframe(report_summary_dict)
            # Upload report summary detauls into s3
            self.s3_utils_object.dataframe_to_s3(dataframe, self.bucket_name, path_to_upload_failed_reports)
            # return True if success
            return True
        except Exception as e:
            logging.error("Error: {}".format(e))
            return False
