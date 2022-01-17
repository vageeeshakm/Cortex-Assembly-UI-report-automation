import json
import logging
import os

from config.report_status_config import EMAIL, S3, PATH
from config.email_config import EMAILSECRET
from config.file_config import FILETYPE
from config.report_status_config import PATH as S3PATH, S3DATE
from utils.common.pandas_utils import PandasUtils
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.aws_s3_utils import AwsS3Utils
from utils.common.email_utils import EmailUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.time_utils import get_todays_date_string


class ConsolidateAndValidateReports:
    """combines the reports and uploads to S3"""
    def __init__(self):
        # Initialise
        self.panadas_utils_object = PandasUtils()
        self.formated_current_date = get_todays_date_string(S3DATE.DATE_FORMAT)

    def download_individual_report_and_consolidate_for_platform(self, **kwargs):
        """ Downloads individual reports and consolidates it and upload report into S3"""

        try:
            self.aws_s3_utils_object = AwsS3Utils()
            platform = kwargs['platform']
            main_folder_on_s3 = S3PATH.MAIN_FOLDER_ON_S3.format(current_date=self.formated_current_date, platform=platform)
            reports_path_dict = {
                S3PATH.SUCCESS_VALIDATION_REPORTS_FOLDER: S3PATH.CONSOLIDATED_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_SUCCESS_REPORT_NAME,
                S3PATH.FAILURE_VALIDATION_REPORTS_FOLDER: S3PATH.CONSOLIDATED_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_FAILURE_REPORT_NAME,
                S3PATH.DATA_VALIDATION_REPORT_DETAILS: S3PATH.CONSOLIDATED_DATA_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_REPORT_DETAILS_NAME,
                S3PATH.DATA_VALIDATION_REPORT_SUMMARY: S3PATH.CONSOLIDATED_DATA_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_REPORT_SUMMARY_NAME
            }

            # loop through each s3 path to download reports and combine
            for download_path, upload_path in reports_path_dict.items():
                try:
                    status_res = self.download_combine_and_upload_report_into_s3(
                        main_folder_on_s3 + download_path,
                        main_folder_on_s3 + upload_path,
                        platform
                    )
                except Exception as e:
                    logging.info("No files found in path " + main_folder_on_s3 + download_path)
                    continue

                logging.info("Status {}".format(status_res))
            logging.info("All Consolidation reports are created and uploaded into s3")

            # delete the temporary s3 folders
            logging.info("Deleting all temporary files in s3")
            path_list_of_s3_folders = []
            for each in reports_path_dict:
                path_list_of_s3_folders.append(main_folder_on_s3 + each)
            self.clear_s3_directories(path_list_of_s3_folders)

        except Exception as e:
            logging.error("Error in creating and uploading consolidated report")
            raise e

    def download_consolidated_report_and_send_email(self, **kwargs):
        """ Downloads the consolidated report from s3 and send Email"""
        self.aws_s3_utils_object = AwsS3Utils()
        platform = kwargs['platform']
        main_folder_on_s3 = S3PATH.MAIN_FOLDER_ON_S3.format(current_date=self.formated_current_date, platform=platform)
        reports_path_list_on_s3 = [
            main_folder_on_s3 + S3PATH.CONSOLIDATED_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_SUCCESS_REPORT_NAME,
            main_folder_on_s3 + S3PATH.CONSOLIDATED_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_FAILURE_REPORT_NAME,
            main_folder_on_s3 + S3PATH.CONSOLIDATED_DATA_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_REPORT_DETAILS_NAME,
            main_folder_on_s3 + S3PATH.CONSOLIDATED_DATA_VALIDATION_REPORTS_FOLDER + S3PATH.CONSOLIDATED_REPORT_SUMMARY_NAME
        ]
        email_result = self.download_from_s3_and_send_consolidated_report_email(reports_path_list_on_s3, platform)
        if email_result:
            logging.info("Consolidation email sent")
        else:
            logging.error("Error in sending Consolidation")
        logging.info("download_and_send_consolidated_report_email function completed")

    def get_previous_report_status(self, bucket_name, file_path):
        """ Function to get dataframe from previous existing reports on S3
            INPUT:
                file_path - path to file on s3 to read
            OUTPUT:
                boolean True if fie present on S3 else False
                dataframe - pandas dataframe after reading file from S2
        """
        # get previous file data
        file_bytes = self.aws_s3_utils_object.get_bytes_from_s3_file(bucket_name, file_path)
        if file_bytes:
            logging.info("file present and creating dataframe out of it")
            existing_file_dataframe = self.panadas_utils_object.get_dataframe_from_bytes(file_bytes)
            return True, existing_file_dataframe
        else:
            logging.info("file not present")
            return False, None

    def download_combine_and_upload_report_into_s3(self, s3_file_path_to_download_individual_reports, s3_path_to_upload_consolidated_report, platform):
        """ downloads reports from a location, combine the reports to a single s3 file and uploads """

        try:
            # Create temp directory to download files from s3
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            path_to_download_files = created_folder_list[0]
            logging.info("Downloading reports from s3")

            # Download files from s3 into local
            self.aws_s3_utils_object.download_all_files(S3.BUCKET_NAME, s3_file_path_to_download_individual_reports, path_to_download_files)
            csv_files_path_list = FileUtils.get_all_files_in_folder(path_to_download_files, FILETYPE.CSV_FILE_TYPE)
            csv_dataframe_with_full_data = self.panadas_utils_object.combine_files_to_dataframe(csv_files_path_list)

            # check if any previous reports present and get data if present
            # Get dataframe from previously existing file on S3
            result, previous_file_dataframe = self.get_previous_report_status(S3.BUCKET_NAME, s3_path_to_upload_consolidated_report)
            if result:
                # Append data in previous dataframe if dataframe is not NULL
                csv_dataframe_with_full_data = self.panadas_utils_object.append_to_datframe(csv_dataframe_with_full_data, previous_file_dataframe)

            # Upload consolidated report into s3
            logging.info("Uploading report into s3")
            self.aws_s3_utils_object.dataframe_to_s3(csv_dataframe_with_full_data, S3.BUCKET_NAME, s3_path_to_upload_consolidated_report)

            # Removing temp directory created
            temp_folder_object.clean_up()
            logging.info("Temp folder deleted")

        except Exception as e:
            logging.error("Error {}".format(e))
            # Removing temp directory created
            temp_folder_object.clean_up()
            logging.info("Temp folder deleted")
            raise e

    def download_from_s3_and_send_consolidated_report_email(self, reports_path_list_on_s3, platform):
        """ downloads a file from s3 and send it via email attachment"""

        try:
            # Initialise AwsSecretManager
            aws_secret_manager_object = AwsSecretManager()
            # Get email configuration to send mail
            email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

            # Create temp directory to download files from s3
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            path_to_download_files = created_folder_list[0]
            attachments_list = []
            logging.info("Downloading reports from s3")
            # Download all the consolidated reports and append path to attachment list
            for each_consolidated_report in reports_path_list_on_s3:
                try:
                    self.aws_s3_utils_object.s3_download(path_to_download_files, S3.BUCKET_NAME, each_consolidated_report)
                    attachments_list.append(path_to_download_files + os.path.basename(each_consolidated_report))
                except Exception as e:
                    logging.info("No files found in " + each_consolidated_report)
                    continue

            # Initialsing email utils to send report
            email_utils_object = EmailUtils(email_config)
            logging.info("Sending Consolidated report email")
            # send email and attach the report
            email_subject = platform + ' - ' + EMAIL.EMAIL_SUBJECT
            email_utils_object.send(
                os.environ['CONSOLIDATED_REPORT_EMAIL_RECIPIENTS'],
                email_subject,
                EMAIL.EMAIL_BODY,
                msg_attachments=attachments_list)
            logging.info("Email sent !")
            # Removing temp directory created
            temp_folder_object.clean_up()
            logging.info("Temp folder deleted")
        except Exception as e:
            logging.error("Error {}".format(e))
            # Removing temp directory created
            temp_folder_object.clean_up()
            logging.info("Temp folder deleted")
            raise e

    def clear_s3_directories(self, path_list_of_s3_folders):
        """delete all the temporary folders and files in s3"""
        for each in path_list_of_s3_folders:
            try:
                self.aws_s3_utils_object.delete_all_files(S3.BUCKET_NAME, each)
            except Exception as e:
                logging.error("Could not found file from - " + each + '\n' + str(e))
                continue
