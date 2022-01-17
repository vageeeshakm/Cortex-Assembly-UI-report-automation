import os
import json
import logging

from config.file_config import FILETYPE
from config.etrade_config import (
    Constant,
    ColumnMapping,
    Formula
)
from utils.common.pandas_utils import PandasUtils
from utils.common.xls_utils import XlsUtils
from utils.common.aws_s3_utils import AwsS3Utils
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.create_and_upload_report_status import CreateAndUploadReportStatus

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class EtradeReport:
    """ Ettade Report Class"""

    def append_rows_to_existing_sheet(self, sheet_name, dataframe_to_append):
        """ Function to append rows to existing sheet """

        logging.info("Appending rows to sheet..")

        # Load sheet
        self.excel_utils_object.load_sheet(sheet_name)

        # Convert df to tuples
        list_of_tuples = self.pandas_utils_object.convert_dataframe_to_list_of_tuples(dataframe_to_append)

        # Append rows to sheet
        self.excel_utils_object.append_rows(list_of_tuples)

        logging.info("Rows are appended to sheet")

    def apply_formula_in_xls_sheet(self, sheet_name, column_number_formula_dict, from_row_number=2):
        """ Function to apply formula to xls sheet"""
        logging.info(f"Applying formula from row_number {from_row_number} for sheet {sheet_name}")

        # Load sheet
        self.excel_utils_object.load_sheet(sheet_name)

        for column_number, formula in column_number_formula_dict.items():
            self.excel_utils_object.apply_formula_for_column_from_row(int(column_number), from_row_number, formula)
        logging.info("Formula applied")

    def download_file_from_s3(self, s3_bucket, s3_file_path, local_file_path):
        """ Download file from s3 """
        self.aws_s3_object.s3_download(bucket_name=s3_bucket, s3_file_path=s3_file_path, local_file_path=local_file_path)
        logging.info(f"File downloaded")

    def download_and_upload_report(self, **kwargs):
        """ Download and upload report function"""

        # Report config
        params = kwargs['report_config']
        advertiser = params['advertiser']
        bucket_name = params['bucket_name']
        s3_folder = params['s3_folder']
        xlsx_file = params['s3_xlsx_file']
        dbm_file = params['s3_dbm_file']
        dcm_file = params['s3_dcm_file']
        oath_file = params['s3_oath_file']
        gemini_file = params['s3_gemini_file']

        s3_upload_file_name = params['s3_upload_file_name']

        create_report_object = CreateAndUploadReportStatus(kwargs.get('platform'))

        self.aws_s3_object = AwsS3Utils()

        # Create object for pandas utils class
        self.pandas_utils_object = PandasUtils()

        # Initialise xls utils
        self.excel_utils_object = XlsUtils()

        # Temp directory utils
        temp_dir_obj = HandleTempDirectory()

        # Download folder
        download_folder_path = temp_dir_obj.generate_dirs()[0]

        logging.info(f"S3 Bucket is -> {bucket_name}")

        # Local path to xls file
        local_xlsx_file = download_folder_path + xlsx_file
        # S3 path to upload the final xls file
        s3_path_to_upload = s3_folder + s3_upload_file_name

        try:

            logging.info("Downloading files from s3")

            s3_files_list_to_download = [xlsx_file, dbm_file, dcm_file, oath_file, gemini_file]

            # Download all required files from s3
            for each_file in s3_files_list_to_download:
                self.download_file_from_s3(bucket_name, s3_folder + each_file, download_folder_path)

            logging.info("All files are downloaded from s3")

            logging.info("Loading xls file")

            # Load xls file
            self.excel_utils_object.load_file(local_xlsx_file)

            logging.info("Xls file loaded")

            date_column = Constant.DATE_COLUMN_NAME
            day_column = Constant.DAY_COLUMN_NAME

            file_details_sheet_mapping_list = [
                {
                    'file_path': download_folder_path + oath_file,
                    'headers_to_skip': Constant.NO_ROWS_TO_SKIP,
                    'footer_to_skip': Constant.NO_ROWS_TO_SKIP,
                    'parse_dates': [day_column],
                    'xls_sheet_name': Constant.OATH_SHEET
                },
                {
                    'file_path': download_folder_path + dbm_file,
                    'headers_to_skip': Constant.NO_ROWS_TO_SKIP,
                    'footer_to_skip': Constant.DBM_FOOTER_ROWS,
                    'parse_dates': [date_column],
                    'xls_sheet_name': Constant.DBM_SHEET
                },
                {
                    'file_path': download_folder_path + dcm_file,
                    'headers_to_skip': Constant.DCM_HEADER_ROWS,
                    'footer_to_skip': Constant.DCM_FOOTER_ROWS,
                    'parse_dates': [date_column],
                    'xls_sheet_name': Constant.DCM_SHEET
                },
                {
                    'file_path': download_folder_path + gemini_file,
                    'headers_to_skip': Constant.NO_ROWS_TO_SKIP,
                    'footer_to_skip': Constant.NO_ROWS_TO_SKIP,
                    'parse_dates': [day_column, Constant.CAMPAIGN_DATE_COLUMN],
                    'xls_sheet_name': Constant.GEMINI_SHEET
                }
            ]

            xls_sheet_formula_mapping_list = [
                {
                    'sheet_name': Constant.DCM_SHEET,
                    'column_formula_dict': Formula.DCM_COLUMN_FORMULA_DICT,
                    'from_row_number': self.excel_utils_object.get_number_of_rows_in_sheet(Constant.DCM_SHEET)
                },
                {
                    'sheet_name': Constant.GEMINI_SHEET,
                    'column_formula_dict': Formula.GEMINI_COLUMN_FORMULA_DICT,
                    'from_row_number': self.excel_utils_object.get_number_of_rows_in_sheet(Constant.GEMINI_SHEET)
                }
            ]

            for each_dict in file_details_sheet_mapping_list:

                logging.info("Appending rows to sheet")

                # date columns list in file
                date_columns_list = each_dict.get('parse_dates')

                logging.info("loading csv to dataframe")
                # Read file into dataframe
                df = self.pandas_utils_object.csv_to_dataframe_with_date(
                    each_dict.get('file_path'),
                    number_of_header_rows_to_skip=each_dict.get('headers_to_skip'),
                    footer_to_skip=each_dict.get('footer_to_skip'),
                    parse_dates=date_columns_list)

                logging.info("loaded csv to dataframe")

                logging.info("Converting column to date type")
                # Convert datetime to date column
                for each_column in date_columns_list:
                    df[each_column] = df[each_column].dt.date

                logging.info("Converted column to date type")

                logging.info("Appending dataframe rows")

                self.append_rows_to_existing_sheet(each_dict.get('xls_sheet_name'), df)

            logging.info("Applying formula for sheet")

            # Apply formual for sheet
            for each in xls_sheet_formula_mapping_list:
                self.apply_formula_in_xls_sheet(each.get('sheet_name'), each.get('column_formula_dict'), each.get('from_row_number'))

            logging.info("Formula applied")

            logging.info("Saving the report.")

            # save xls file
            self.excel_utils_object.save_file(local_xlsx_file)

            logging.info("All Xls operations are completed and report saved ")

            # Upload the file into Box
            logging.info("Uploading the file to S3")

            self.aws_s3_object.s3_upload(bucket_name=bucket_name, s3_file_path=s3_path_to_upload, local_file_path=local_xlsx_file)

            logging.info(f"Uploaded the file to S3 bucket {bucket_name} and path is {s3_path_to_upload}")

            # Writing success details into success report and uplod into s3
            upload_report_status_res = create_report_object.success_report_status_to_s3(
                advertiser,
                s3_path_to_upload)
            logging.info("Uploaded result status {}".format(upload_report_status_res))

            # Clean temporory directory
            temp_dir_obj.clean_up()

        except Exception as e:
            temp_dir_obj.clean_up()
            logging.error("Error in Downloading and processing Etrade report")

            upload_report_status_res = create_report_object.failure_report_status_to_s3(
                advertiser,
                e,
                s3_path_to_upload)
            raise e
