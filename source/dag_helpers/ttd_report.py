import os
import json
import logging
from datetime import datetime
from config.ttd_config import (
    DATE,
    PATH,
    FILENAME,
    PANDASSETTINGS,
    ADVERTISERDETAILS
)
from config.report_status_config import PLATFORM
from config.email_config import EMAILSECRET
from config.file_config import FILETYPE
from config.ftp_config import FTPSECRET
from utils.datasources.ttd.ttd_utils import TtdUtils
from utils.selenium.selenium_utils import SeleniumUtils
from utils.common.data_validations import DataValidationsUtils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.get_all_files_in_folder import FileUtils
from utils.common.email_utils import EmailUtils
from utils.common.general_utils import replace_function, split_function
from utils.common.ftp_utils import FtpUtils
from utils.common.pandas_utils import PandasUtils
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class TtdReport():
    """ class with function to download TTD report"""

    def create_directory_on_ftp(self, **kwargs):
        """ Function to create a directory on ftp server
            output:
                ftp_object
                directory_on_ftp_for_elevate_report
                directory_on_ftp_for_assembly_and_varick_report
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
        # parent_directory_to_upload = '/test_ttd_report/'

        directory_on_ftp_for_assembly_and_varick_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_ASSEMBLY_AND_VARICK.format(current_date=formated_current_date)
        directory_on_ftp_for_elevate_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_ELEVATE_REPORT.format(current_date=formated_current_date)
        directory_on_ftp_for_sophie_report = parent_directory_to_upload + PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_SOPHIE.format(current_date=formated_current_date)

        ftp_object.create_nested_directory(directory_on_ftp_for_elevate_report)
        ftp_object.create_nested_directory(directory_on_ftp_for_assembly_and_varick_report)
        ftp_object.create_nested_directory(directory_on_ftp_for_sophie_report)
        return {
            "ftp_object": ftp_object,
            "directory_on_ftp_for_elevate_report": directory_on_ftp_for_elevate_report,
            "directory_on_ftp_for_assembly_and_varick_report": directory_on_ftp_for_assembly_and_varick_report,
            "directory_on_ftp_for_sophie_report": directory_on_ftp_for_sophie_report
        }

    def download_report(self, **kwargs):
        """
        Download TTD report function
        INPUT : {dict}
                download_folder_path - path to the download foleder
                link_to_download - link to download the report
        OUTPUT: Download all fiiles in specified folder.
        """
        try:
            selenium_object = None
            logging.info('TTD report download started')

            # Fetch report config information
            download_folder_path = kwargs.get('download_folder_path')
            link_to_download = kwargs.get('download_link')
            # get selenium object
            selenium_object = SeleniumUtils(download_path=download_folder_path)
            # initialize TTD utils object
            ttd_utils_object = TtdUtils(selenium_object=selenium_object)
            # Download report function
            ttd_utils_object.download_report(link_to_download)
            # Check download is sucess and files are present in download folder
            ttd_utils_object.check_download_status()
            logging.info("All reports are downloaded")
            # exit by closing browser
            selenium_object.exit()
            logging.info('Report download finished!')
            return True
        except Exception as e:
            logging.error("Error in downloading the report")
            if selenium_object:
                selenium_object.exit()
            raise Exception(e)

    def download_segregate_and_upload_reports(self, **kwargs):
        """ Function that calls download by getting link from email and then segregate xls files using pandas and upload the report into FTP server
            INPUT:
                ftp_object: ftp object
                directory_on_ftp: directory on FTP to upload files
                json config file
                    "advertiser": Advertiser type
                    "email_subject": Email Subject to find the link to download the report
            OUTPUT:
                upload xls files on FTP.
        """
        try:
            # Create object to Create and Upload Validation Report class
            create_report_object = CreateAndUploadReportStatus(PLATFORM.TTD)

            logging.info("Pulling values from xcom")
            task_instance = kwargs['task_instance']
            xcom_values = task_instance.xcom_pull(task_ids='create_ttd_directory_on_ftp')

            ftp_object = None
            if xcom_values:
                # Pull ftp_object and directory on ftp.
                ftp_object = xcom_values['ftp_object']
                directory_on_ftp_for_elevate_report = xcom_values['directory_on_ftp_for_elevate_report']
                directory_on_ftp_for_assembly_and_varick_report = xcom_values['directory_on_ftp_for_assembly_and_varick_report']
                directory_on_ftp_for_sophie_report = xcom_values['directory_on_ftp_for_sophie_report']

            # Fetch account name and platform type from config file
            advertiser = kwargs.get('report_config').get('advertiser')
            email_subject = kwargs.get('report_config').get('email_subject')
            platform = kwargs.get('platform')
            logging.info("advertiser type is {}".format(advertiser))

            # Get ftp upload directory

            if advertiser == ADVERTISERDETAILS.MEDIA_ASSEMBLY_ADVERTISER_TYPE:
                logging.info("Media assembly advertiser")
                main_ftp_folder_to_upload = directory_on_ftp_for_assembly_and_varick_report
            elif advertiser == ADVERTISERDETAILS.ELEVATE_ADVERTISER_NAME_TYPE:
                logging.info("Elevate advertiser")
                main_ftp_folder_to_upload = directory_on_ftp_for_elevate_report
            elif advertiser == ADVERTISERDETAILS.VARICK_ADVERTISER_TYPE:
                logging.info("Varick advertiser")
                main_ftp_folder_to_upload = directory_on_ftp_for_assembly_and_varick_report
            elif advertiser == ADVERTISERDETAILS.SOPHIE_ADVERTISER_TYPE:
                logging.info("Sophie advertiser")
                main_ftp_folder_to_upload = directory_on_ftp_for_sophie_report
            else:
                logging.info("Media assembly advertiser")
                raise Exception("Unknown Advertiser specified in the input config file")

            substring_to_split = ' Download your report by clicking the link below:'

            # Get email config from secret manager
            aws_secret_manager_object = AwsSecretManager()
            email_config = json.loads(aws_secret_manager_object.get_secret(EMAILSECRET.EMAIL_SERCRET_NAME))

            # Get Download link from email
            # Initialise email ustils
            email_utils_object = EmailUtils(email_config)
            # Search for link in email by passing from and email subject
            serach_res_list = email_utils_object.search(
                os.environ['TTD_DAILY_REPORT_EMAIL_ADDRESS_TO_SEARCH'],
                email_subject
            )
            # get email body text
            email_body_text = email_utils_object.get_email_body_text_from_search_result(serach_res_list)

            # split using text and replace extra lines
            text_list = split_function(email_body_text, substring_to_split)
            download_link = replace_function(text_list[-1], '\r', '')
            download_link = replace_function(download_link, '\n', '').strip()

            # call function to create a temporary directory
            temp_folder_object = HandleTempDirectory()
            created_folder_list = temp_folder_object.generate_dirs()
            download_folder = created_folder_list[0]
            kwargs = {'download_folder_path': download_folder, 'download_link': download_link}
            # Function to download the report
            download_res = self.download_report(**kwargs)
            if not download_res:
                logging.info("Report not generated")
                # remove the created temp directory
                temp_folder_object.clean_up()
                return

            # Initialise Pandas utils
            panadas_utils_object = PandasUtils()
            # Get xlsx file names in download folder
            xls_files_path_list = FileUtils.get_all_files_in_folder(download_folder, FILETYPE.EXCEL_FILE_TYPE)
            # Perform operation based on Advertiser type
            if advertiser == ADVERTISERDETAILS.MEDIA_ASSEMBLY_ADVERTISER_TYPE:
                logging.info("Media assembly advertiser")
                # get dataframe from xls file
                xls_dataframe = panadas_utils_object.get_dataframe_from_excel(xls_files_path_list[0], PANDASSETTINGS.ASSEMBLY_AND_VARICK_REPORT_SHEET_NAME, PANDASSETTINGS.DATE_COLUMNS_LIST_IN_SHEET)
                # drop column
                dataframe_after_droping_columns = panadas_utils_object.drop_columns_from_dataframe(xls_dataframe, PANDASSETTINGS.COLUMNS_LIST_TO_DROP)
                # Get unique advertiser in report
                unique_advertisers = dataframe_after_droping_columns[PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER].unique()
                # Create sep report for each advertiser and upload that into FTP
                for each_advertiser in unique_advertisers:
                    advertiser_name = ADVERTISERDETAILS.adveriser_id_name_mapping_dict_for_assembly[each_advertiser]
                    output_report_name = FILENAME.ASSEMBLY_REPORT_OUTPUT_FILENAME.format(advertiser=advertiser_name)
                    output_report_path_with_name = download_folder + output_report_name
                    logging.info("output_report_name is {}".format(output_report_name))
                    each_advertiser_dataframe_after_filtering = xls_dataframe[xls_dataframe[PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER] == each_advertiser]
                    # create xls file
                    panadas_utils_object.create_xls_sheet_from_dataframe(each_advertiser_dataframe_after_filtering, output_report_path_with_name, use_excel_writer=True)
                    logging.info("Uploading file into FTP")

                    # validate the data for file to be uploaded
                    final_dataframe = each_advertiser_dataframe_after_filtering
                    s3_file_name = output_report_name
                    input_file_val_schema_path = PATH.TTD_ASSEMBLY_VARICK_SCHEMA
                    dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=s3_file_name)
                    dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
                    dv_obj.write_validations_log_report()

                    # Upload into FTP
                    ftp_object.put(output_report_path_with_name, directory_on_ftp_for_assembly_and_varick_report + output_report_name)
                    logging.info("Files uploaded into FTP")

                    # Create and Upload report status
                    # Writing success details into success report and uplod into s3
                    upload_report_status_res = create_report_object.success_report_status_to_s3(
                        advertiser,
                        main_ftp_folder_to_upload,
                        advertiser_name)
                    logging.info("result {}".format(upload_report_status_res))
                    logging.info("completed")
            elif advertiser == ADVERTISERDETAILS.ELEVATE_ADVERTISER_NAME_TYPE:
                logging.info("Elevate type advertiser")
                xls_dataframe = panadas_utils_object.get_dataframe_from_excel(xls_files_path_list[0], PANDASSETTINGS.ELEVATE_REPORT_SHEET_NAME, PANDASSETTINGS.DATE_COLUMNS_LIST_IN_SHEET)
                output_report_path_with_name = download_folder + FILENAME.ELEVATE_REPORT_OUTPUT_FILENAME
                # create xls report
                panadas_utils_object.create_xls_sheet_from_dataframe(xls_dataframe, output_report_path_with_name, use_excel_writer=True)

                # validate the data for file to be uploaded
                final_dataframe = xls_dataframe
                s3_file_name = FILENAME.ELEVATE_REPORT_OUTPUT_FILENAME
                input_file_val_schema_path = PATH.TTD_ELEVATE_SCHEMA
                dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=s3_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

                logging.info("Uploading file into FTP")
                # Upload report into FTP
                ftp_object.put(output_report_path_with_name, directory_on_ftp_for_elevate_report + FILENAME.ELEVATE_REPORT_OUTPUT_FILENAME)
                logging.info("Files uploaded into FTP")

                # Create and Upload report status
                # Writing success details into success report and uplod into s3
                upload_report_status_res = create_report_object.success_report_status_to_s3(
                    advertiser,
                    main_ftp_folder_to_upload,
                    advertiser)
                logging.info("result {}".format(upload_report_status_res))
                logging.info("completed")
            elif advertiser == ADVERTISERDETAILS.VARICK_ADVERTISER_TYPE:
                logging.info("Varick type advertiser")
                xls_dataframe = panadas_utils_object.get_dataframe_from_excel(xls_files_path_list[0], PANDASSETTINGS.ASSEMBLY_AND_VARICK_REPORT_SHEET_NAME, PANDASSETTINGS.DATE_COLUMNS_LIST_IN_SHEET)
                output_report_path_with_name = download_folder + FILENAME.VARICK_REPORT_OUTPUT_FILENAME
                # Create xls report
                panadas_utils_object.create_xls_sheet_from_dataframe(xls_dataframe, output_report_path_with_name, use_excel_writer=True)

                # validate the data for file to be uploaded
                final_dataframe = xls_dataframe
                s3_file_name = FILENAME.VARICK_REPORT_OUTPUT_FILENAME
                input_file_val_schema_path = PATH.TTD_ASSEMBLY_VARICK_SCHEMA
                dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=s3_file_name)
                dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
                dv_obj.write_validations_log_report()

                logging.info("Uploading file into FTP")

                # Upload report into FTP
                ftp_object.put(output_report_path_with_name, directory_on_ftp_for_assembly_and_varick_report + FILENAME.VARICK_REPORT_OUTPUT_FILENAME)
                logging.info("Files uploaded into FTP")

                # Create and Upload report status
                # Writing success details into success report and uplod into s3
                upload_report_status_res = create_report_object.success_report_status_to_s3(
                    advertiser,
                    main_ftp_folder_to_upload,
                    advertiser)
                logging.info("result {}".format(upload_report_status_res))
                logging.info("completed")

            elif advertiser == ADVERTISERDETAILS.SOPHIE_ADVERTISER_TYPE:
                logging.info("Sophie advertiser")

                # get dataframe from xls file
                xls_dataframe = panadas_utils_object.get_dataframe_from_excel(xls_files_path_list[0], PANDASSETTINGS.SOPHIE_REPORT_SHEET_NAME, PANDASSETTINGS.DATE_COLUMNS_LIST_IN_SHEET)

                # drop column
                # dataframe_after_droping_columns = panadas_utils_object.drop_columns_from_dataframe(xls_dataframe, PANDASSETTINGS.COLUMNS_LIST_TO_DROP)

                # Get unique advertiser in report
                unique_advertisers = xls_dataframe[PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER].unique()

                # Create seperate report for each advertiser and upload that into FTP
                for each_advertiser in unique_advertisers:

                    advertiser_name = ADVERTISERDETAILS.adveriser_id_name_mapping_dict_for_sophie[each_advertiser]

                    output_report_name = FILENAME.SOPHIE_REPORT_OUTPUT_FILENAME.format(advertiser=advertiser_name)
                    output_report_path_with_name = download_folder + output_report_name
                    logging.info("output_report_name is {}".format(output_report_name))

                    each_advertiser_dataframe_after_filtering = xls_dataframe[xls_dataframe[PANDASSETTINGS.ADVERTISER_COLUMN_TO_FILTER] == each_advertiser]

                    # create xls file
                    panadas_utils_object.create_xls_sheet_from_dataframe(each_advertiser_dataframe_after_filtering, output_report_path_with_name, use_excel_writer=True)
                    logging.info("Uploading file into FTP")

                    # validate the data for file to be uploaded
                    final_dataframe = each_advertiser_dataframe_after_filtering
                    s3_file_name = output_report_name
                    input_file_val_schema_path = PATH.TTD_SOPHIE_SCHEMA
                    dv_obj = DataValidationsUtils(platform=platform, file_name_for_s3=s3_file_name)
                    dv_obj.validate_data(input_df=final_dataframe, input_file_name=s3_file_name, input_file_val_schema_path=input_file_val_schema_path)
                    dv_obj.write_validations_log_report()

                    # Upload into FTP
                    ftp_object.put(output_report_path_with_name, directory_on_ftp_for_sophie_report + output_report_name)
                    logging.info("Files uploaded into FTP")

                    # Create and Upload report status
                    # Writing success details into success report and uplod into s3
                    upload_report_status_res = create_report_object.success_report_status_to_s3(
                        advertiser,
                        main_ftp_folder_to_upload,
                        advertiser_name)
                    logging.info("result {}".format(upload_report_status_res))
                    logging.info("completed")
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
                pass

            # Writing error details into failure report and uplod into s3
            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                advertiser,
                e,
                main_ftp_folder_to_upload)
            logging.info("result {}".format(upload_report_status_res))
            raise Exception(e)
