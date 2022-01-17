import datetime
import dateutil.relativedelta
import json
import pandas as pd
import inflect
import logging
import time

from pandas.api.types import is_datetime64_any_dtype as is_datetime

from config.file_config import FILETYPE
from config.report_status_config import S3, PATH
from utils.common.handle_temporory_directory import HandleTempDirectory
from utils.common.aws_s3_utils import AwsS3Utils


class DataValidationsUtils:

    def __init__(self, platform, file_name_for_s3, input_file_date=None):

        if not input_file_date:
            input_file_date = datetime.datetime.now().date()

        self.aws = AwsS3Utils()

        # job name will be used as a folder in s3
        self.platform = platform

        # validation report file names for S3, if name has .xlsx replace with .csv
        self.s3_file_name = file_name_for_s3.replace(FILETYPE.EXCEL_FILE_TYPE, FILETYPE.CSV_FILE_TYPE)

        # date for which the job/file validation is running
        self.input_file_date = input_file_date
        self.input_file_date_str = self.input_file_date.strftime('%Y%m%d')

        # main S3 details
        self.s3_bucket = S3.BUCKET_NAME
        self.s3_report_folder = PATH.MAIN_FOLDER_ON_S3.format(current_date=self.input_file_date_str, platform=platform)

        # sub folder of S3 bucket
        self.s3_report_summary_folder = PATH.DATA_VALIDATION_REPORT_SUMMARY
        self.s3_report_details_folder = PATH.DATA_VALIDATION_REPORT_DETAILS

        # order of the attributes is as per the main validation util function
        self.attributes_required_for_each_column = [
            'column_name', 'datatype', 'infer_datatype', 'can_be_null', 'is_unique', 'should_be_sorted', 'range_of_values', 'date_format'
        ]

        # final df to be uploaded
        self.final_result_summary_df = pd.DataFrame()
        self.final_result_details_df = pd.DataFrame()

    def get_input_file_schema(self, input_file_val_schema_path):
        """Read the validation schema file to load the list of dicts from it."""

        # open the validation schema file
        columns_attributes_list_of_dicts = []
        with open(input_file_val_schema_path, mode='r+') as file_obj:
            columns_attributes_list_of_dicts = json.load(file_obj)
        return columns_attributes_list_of_dicts

    def convert_col_attr_json_list_to_single_list(self, columns_attributes_list_of_dicts):
        """Convert list of dicts from the validation schema file to a single list."""

        # first element of the list is the number of columns to be validated
        columns_attributes_list = [len(columns_attributes_list_of_dicts)]

        # check each dict for the required column attributes
        for each_col_attributes in columns_attributes_list_of_dicts:
            for each_req_attribute in self.attributes_required_for_each_column:

                # if a column dict does not have the required attribute in it, append '' else get the value from the dict
                columns_attributes_list.append(each_col_attributes.get(each_req_attribute, ''))

        return columns_attributes_list

    def write_validations_log_report(self):
        """ uploads the report to s3 """

        # validation report df which consists validation result of all files
        main_summary_df = self.final_result_summary_df
        main_details_df = self.final_result_details_df

        # if the main df has some validation result
        if not main_summary_df.empty:

            temp_dirs_obj = HandleTempDirectory()
            temp_dirs_list = temp_dirs_obj.generate_dirs(number_of_dirs_to_create=2)

            # local_dir is used for storing the original data file from the original s3 source path
            # local_dir1 is for report_summary and local_dir2 is for report_details
            local_dir1 = temp_dirs_list[0]
            local_dir2 = temp_dirs_list[1]

            # s3 location of the earlier generated report
            # ex:- s3/mdc-common-core/ui_automated_report_validations/20200901/pinterest/data_validation/report_details/pinterest_{account_id}.csv
            s3_report_summary_location = self.s3_report_folder + self.s3_report_summary_folder + self.s3_file_name
            s3_report_details_location = self.s3_report_folder + self.s3_report_details_folder + self.s3_file_name

            # local path of the report downloaded from S3/report newly generated
            local_summary_report_path = local_dir1 + self.s3_file_name
            local_details_report_path = local_dir2 + self.s3_file_name

            logging.info("Starting to upload input files validation report to - " + s3_report_summary_location + ' and ' + s3_report_details_location)

            # generate report csv on local
            main_summary_df.to_csv(path_or_buf=local_summary_report_path, index=False)
            main_details_df.to_csv(path_or_buf=local_details_report_path, index=False)

            # upload the report csv to S3
            self.aws.s3_upload(bucket_name=self.s3_bucket, local_file_path=local_summary_report_path, s3_file_path=s3_report_summary_location)
            self.aws.s3_upload(bucket_name=self.s3_bucket, local_file_path=local_details_report_path, s3_file_path=s3_report_details_location)

            # remove tmp dir where the validation file was downloaded
            temp_dirs_obj.clean_up()

            logging.info("Validation report upload completed")
        else:
            logging.info("No validation data found for generating the report")

    def validate_data(self, input_df, input_file_name, input_file_val_schema_path):
        """Validate an input file df based on the schema provided."""

        logging.info("Validation started for - " + input_file_name)

        # read input file validation schema
        columns_attributes_list_of_dicts = self.get_input_file_schema(input_file_val_schema_path)

        # columns_attributes_list gets populated here
        columns_attributes_list = self.convert_col_attr_json_list_to_single_list(columns_attributes_list_of_dicts)

        result = messages = result_summary_df = result_details_df = None

        # try:
        val_obj = DataValidations(input_df, input_file_name, self.input_file_date, columns_attributes_list)
        result, messages, result_summary_df, result_details_df = val_obj.validate_source()

        # append each file validation df to the main df
        self.final_result_summary_df = pd.concat([result_summary_df, self.final_result_summary_df], sort=False)
        self.final_result_details_df = pd.concat([result_details_df, self.final_result_details_df], sort=False)
        # except Exception as e:
        #     logging.info('Could not validate the input file - ' + str(e))

        logging.info("Validation completed for - " + input_file_name)
        return result, messages, result_summary_df

class DataValidations:

    def __init__(self, input_df, input_file_name, input_file_date, input_temp):
        # initialize the class variables. Variable names are self explanatory
        self.df_temp = input_df
        self.file_date = input_file_date
        self.file_name = input_file_name
        self.temp = input_temp
        # self.created_ts = datetime.datetime.fromtimestamp(time.time()).strftime('%d-%b-%Y %H:%M:%S')

        self.fail_msg = []  # this list stores all the failed messages to be returned to the calling script

        # row counter to write results to specific row of results dataframe.
        # This is more useful when multiple files have to be tested simultaneously
        self.results_row_counter = 0

        self.tc_count = len(self.df_temp)  # total rows in the file to be tested. To calculate no: of test cases

        # col_names list contains the column headers of df_result dataframe.
        # It is derived from total number of columns in file to be tested
        self.col_names = []
        self.col_names.append('Number_Of_Cols_Result')
        for i in range(0, self.temp[0]):
            self.col_names.append('Name_C' + str(i + 1) + '_Result')
            self.col_names.append('Datatype_C' + str(i + 1) + '_Result')
            self.col_names.append('Infer_Datatype_C' + str(i + 1) + '_Result')
            self.col_names.append('Null_C' + str(i + 1) + '_Result')
            self.col_names.append('Unique_C' + str(i + 1) + '_Result')
            self.col_names.append('Sorted_C' + str(i + 1) + '_Result')
            self.col_names.append('Range_C' + str(i + 1) + '_Result')
            self.col_names.append('Format_C' + str(i + 1) + '_Result')

        # initializing df_results dataframe to store the detailed results of validations
        self.df_results = pd.DataFrame(columns=self.col_names)

        # adding columns with _Comment and _TC-Count suffixes to store comments and test case counts
        for i in self.col_names:
            pos = self.df_results.columns.get_loc(i) + 1
            self.df_results.insert(pos, i.replace('_Result', '_Comment'), '')
            self.df_results.insert(pos + 1, i.replace('_Result', '_TC-Count'), 0)

        self.df_results.insert(0, 'Filename', '')  # enter first row
        self.df_results.at[0, 'Filename'] = self.file_name  # assign file_name value to Filename column

    def highlight_vals(self, val):
        # This function is to format the html results

        if val == 'Pass':
            return 'color: %s' % 'Green'
        elif val == 'Fail':
            return 'color: %s' % 'Red'
        elif val == 'No Schema':
            return 'color: %s' % 'Blue'
        else:
            return ''

    def get_master_results(self, text):
        # This function returns Fail if any one of the values are fail, else unique values

        text = [value for value in text if str(value) != 'nan']
        value = 'Pass'
        if len(text) == 0:
            value = 'DNT'
        elif len(set(text)) == 1 and len(text) >= 1:
            # print(set(text))
            value = str(text[0])
        elif 'Fail' in text:
            value = 'Fail'
        return value

    def format_file_size(self, size):
        # This function returns size of the file in B, KB, MB or GB depending on size
        if size < 1000:
            return str(size) + ' B'
        elif size >= 1000 and size < 1000 ** 2:
            return str(round(size / (1000), 1)) + ' KB'
        elif size >= 1000 ** 2 and size < 1000 ** 3:
            return str(round(size / (1000 ** 2), 2)) + ' MB'
        else:
            return str(round(size / (1000 ** 3), 2)) + ' GB'

    def format_exec_time(self, duration):
        # This function returns time of execution of the file in Sec, Min or Hours depending on time ran
        if duration < 60:
            return str(round(duration, 2)) + ' Sec'
        elif (duration >= 60 and duration <= 3600):
            return str(round(duration / 60, 2)) + ' Min'
        else:
            return str(round(duration / 3600, 2)) + ' Hours'

    def clean_data_type(self, text):
        # This function converts pandas datatypes and returns strings - int, float, string or date
        if text == 'int64' or text == 'Int64':
            return 'int'
        elif text == 'float64':
            return 'float'
        elif text == 'object':
            return 'string'
        elif text == 'datetime64[ns]':
            return 'date'
        else:
            return text

    def clean_comma(self, text):
        # This function removes commas from text passed to it
        text = text.replace(',', '')
        return text

    def validate_datatype(self, temp_col_names, temp_datatypes):
        # This function validates the datatype of columns
        # print(self.results_row_counter)
        temp_col_counter = 0  # This temporary variable stores column number. This variable is used to save results into the right column of dataframes self.df_results & df_summary
        for col_name, data_type in zip(temp_col_names, temp_datatypes):
            temp_col_counter += 1
            val = str(temp_col_counter)
            if data_type == '' or data_type == 'DNT':
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_TC-Count'] = 1
                self.df_results.at[
                    self.results_row_counter, 'Datatype_C' + val + '_Comment'] = col_name + ' is not tested based on input'
                self.fail_msg.append(col_name + ' is not tested based on input')
                continue
            try:
                # actual_data_type = self.clean_data_type(str(self.df_temp[col_name].dtype))
                actual_data_type = self.clean_data_type(str(pd.Series(self.df_temp[col_name], dtype='Int64').dtype))
            except KeyError:
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_TC-Count'] = 1
                self.df_results.at[
                    self.results_row_counter, 'Datatype_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                self.fail_msg.append('Column ' + col_name + ' not found in file')
                # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                continue

            if data_type == actual_data_type:
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_Result'] = 'Pass'
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_TC-Count'] = self.tc_count
                # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
            elif data_type not in ['int', 'float', 'date', 'string']:
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_TC-Count'] = 0
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_Comment'] = 'Invalid option in input'
                self.fail_msg.append('Invalid option in input')
            else:
                # print(f'datatype exp = {data_type} and actual datatype = {actual_data_type}')
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Datatype_C' + val + '_TC-Count'] = self.tc_count
                self.df_results.at[
                    self.results_row_counter, 'Datatype_C' + val + '_Comment'] = col_name + ' is ' + actual_data_type + ' expected was ' + data_type
                self.fail_msg.append(col_name + ' is ' + actual_data_type + ' expected was ' + data_type)

    def deep_validate_datatype(self, temp_col_names, temp_datatypes, temp_format, temp_infer):
        # This function infers the datatype of each columns and validates it.
        # For e.g., an int column type casted to string will return as int with this function
        # print(self.results_row_counter)

        # This temporary variable stores column number.
        # This variable is used to save results into the correct column of dataframes self.df_results & df_summary
        temp_col_counter = 0

        for col_name, is_format, to_infer in zip(temp_col_names, temp_format, temp_infer):
            temp_col_counter += 1
            val = str(temp_col_counter)
            if to_infer in ['', 'DNT']:
                self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = col_name + ' is not tested based on input'
                # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
                continue
            else:
                try:
                    # actual_data_type = self.clean_data_type(str(self.df_temp[col_name].dtype))
                    actual_data_type = self.clean_data_type(str(pd.Series(self.df_temp[col_name], dtype='Int64').dtype))
                except KeyError:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                    self.fail_msg.append('Column ' + col_name + ' not found in file')
                    # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                    continue

            if to_infer == 'int':
                try:
                    self.df_temp[col_name] = self.df_temp[col_name].apply(lambda x: self.clean_comma(str(x)))  # This step is to accomodate thousands separated with comma format
                    actual_data_type = self.clean_data_type(
                        str(pd.Series(self.df_temp[col_name].dropna().astype(int), dtype='Int64').dtype))
                except ValueError:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = 'Column ' + col_name + ' is ' + actual_data_type + ' instead of int'
                    self.fail_msg.append(col_name + ' is ' + actual_data_type + ' instead of int')
                    continue
                # print(f'datatype exp = {data_type} and actual datatype = {actual_data_type}')
                if actual_data_type == 'int':
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
                else:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = actual_data_type + ' instead of int'
                    self.fail_msg.append(actual_data_type + ' instead of int')
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Fail ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
            elif to_infer == 'float':
                try:
                    self.df_temp[col_name] = self.df_temp[col_name].apply(lambda x: self.clean_comma(str(x)))  # This step is to accomodate thousands separated with comma format
                    # print(col_name, self.df_temp[col_name][0])
                    actual_data_type = self.clean_data_type(str(self.df_temp[col_name].astype(float).dtype))
                except ValueError:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = 'Column ' + col_name + ' is ' + actual_data_type + ' instead of float'
                    self.fail_msg.append('Column ' + col_name + ' is ' + actual_data_type + ' instead of float')
                    continue
                if actual_data_type == 'float':
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
                else:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = actual_data_type + ' instead of float'
                    self.fail_msg.append(actual_data_type + ' instead of float')
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Fail  ({actual_data_type}  {data_type}) Datatype_C{temp_col_counter}_Result')
            elif to_infer == 'string':
                if actual_data_type == 'string':
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
                else:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = actual_data_type + ' instead of string'
                    self.fail_msg.append(actual_data_type + ' instead of string')
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Fail  ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
            elif to_infer == 'date':
                if actual_data_type == 'date':
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
                elif actual_data_type == 'string' or actual_data_type == 'int':
                    """
                    is_numeric = True
                    try:
                        self.df_temp[col_name].astype(float)  # just to check if column is numeric
                    except ValueError:
                        is_numeric = False
                    """
                    try:
                        # if is_numeric == False and is_datetime(pd.to_datetime(self.df_temp[col_name].dropna())):
                        # if is_datetime(pd.to_datetime(self.df_temp[col_name].dropna())):
                        if is_datetime(pd.to_datetime(self.df_temp[col_name].dropna(), format=is_format, errors='raise')):
                            actual_data_type = 'date'
                            self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Pass'
                            self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                            # print(f'Datatype of column {temp_col_counter} ({col_name}) is Pass ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')

                    except ValueError:
                        self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                        self.df_results.at[
                            self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = 'Column ' + col_name + ' is not Date datatype in specified format (' + is_format + ')'
                        self.fail_msg.append('Column ' + col_name + ' is not Date datatype in specified format (' + is_format + ')')

                else:
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = actual_data_type + ' instead of date'
                    self.fail_msg.append(actual_data_type + ' instead of date')
                    # print(f'Datatype of column {temp_col_counter} ({col_name}) is Fail ({actual_data_type} {data_type}) Datatype_C{temp_col_counter}_Result')
            else:
                self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_TC-Count'] = 0
                self.df_results.at[self.results_row_counter, 'Infer_Datatype_C' + val + '_Comment'] = 'Invalid option in input'
                self.fail_msg.append('Invalid option in input')
            # temp_col_counter+=1

    def validate_null(self, temp_col_names, temp_null):
        # print(temp_null)
        temp_col_counter = 0  # This temporary variable stores column number. This variable is used to save results into the right column of dataframes self.df_results & df_summary
        for col_name, expected_null_value in zip(temp_col_names, temp_null):
            temp_col_counter += 1
            val = str(temp_col_counter)
            if expected_null_value == 'DNT' or expected_null_value == '':
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Comment'] = col_name + ' is not tested based on input'
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_TC-Count'] = 0
                # print(f'Column {val} {col_name} is Not Tested based on input')
                continue
            else:
                try:
                    checked_null_value = self.df_temp[col_name].isnull().values.any()
                except KeyError:
                    self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Null_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Null_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                    self.fail_msg.append('Column ' + col_name + ' not found in file')
                    # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                    continue

            if expected_null_value == False and checked_null_value == True:
                # print(f'Null check of column {temp_col_counter} ({col_name}) is Fail (Null values appears in column {col_name})')
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_TC-Count'] = self.tc_count
                self.df_results.at[
                    self.results_row_counter, 'Null_C' + val + '_Comment'] = 'Null values appearing in Column ' + col_name
                self.fail_msg.append('Null values appearing in Column ' + col_name)
            elif expected_null_value == False and checked_null_value == False:
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Result'] = 'Pass'
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_TC-Count'] = self.tc_count
            elif expected_null_value == True:
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Result'] = 'Pass'
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_TC-Count'] = self.tc_count
            else:
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_TC-Count'] = 0
                self.df_results.at[self.results_row_counter, 'Null_C' + val + '_Comment'] = 'Invalid option in parameter file'
                self.fail_msg.append('Invalid option in parameter file')
                # print(f'Null check of column {temp_col_counter} ({col_name}) is Pass (Null values are as expected)')

    def write_results(self, col_suffix, col_val, res, tc_no, err_msg):
        self.df_results.at[self.results_row_counter, col_suffix + col_val + '_Result'] = res
        self.df_results.at[self.results_row_counter, col_suffix + col_val + '_TC-Count'] = tc_no
        self.df_results.at[self.results_row_counter, col_suffix + col_val + '_Comment'] = err_msg

    def validate_col_names(self, temp_col_names, current_file_col_names):
        # This function validates the column names

        temp_col_counter = 0  # This temporary variable stores column number. This variable is used to save results into the right column of dataframes self.df_results & df_summary
        # print(temp_col_names)
        # if len(temp_col_names) >= len(current_file_col_names):
        for i in range(len(temp_col_names)):
            temp_col_counter += 1
            val = str(temp_col_counter)
            if temp_col_names[i] in current_file_col_names:
                self.write_results('Name_C', val, 'Pass', 1, temp_col_names[i])
                """
                self.df_results.at[self.results_row_counter, 'Name_C' + val + '_Result'] = 'Pass'
                self.df_results.at[self.results_row_counter, 'Name_C' + val + '_TC-Count'] = 1
                self.df_results.at[self.results_row_counter, 'Name_C' + val + '_Comment'] = temp_col_names[i]
                # print(f'Column {val} is Pass {temp_col_names[i]} is same as {current_file_col_names[i]}')
                """
            else:
                self.df_results.at[self.results_row_counter, 'Name_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Name_C' + val + '_TC-Count'] = 1
                self.df_results.at[self.results_row_counter, 'Name_C' + val + '_Comment'] = temp_col_names[
                                                                                      i] + ' column is not present in ' + self.file_name
                self.fail_msg.append(temp_col_names[i] + ' column is not present in ' + self.file_name)
                # print(f'Column {val} is Fail {temp_col_names[i]} is not same as {current_file_col_names[i]}')

    def validate_is_unique(self, temp_col_names, temp_unique):
        # This function validates if all the contents are unique (it will compare after removing null values)

        # This temporary variable stores column number.
        # This variable is used to save results into the right column of dataframes self.df_results & df_summary
        temp_col_counter = 0
        for col_name, is_unique in zip(temp_col_names, temp_unique):
            temp_col_counter += 1
            val = str(temp_col_counter)
            # print(f'val = {val}, is_unique = {is_unique}')
            if is_unique == 'DNT' or is_unique == '':
                self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_Result'] = 'DNT'
                self.df_results.at[
                    self.results_row_counter, 'Unique_C' + val + '_Comment'] = col_name + ' is not tested based on input'
                self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_TC-Count'] = 0
                # print(f'Column {val} {col_name} is Not Tested based on input')
                continue
            else:
                try:
                    total_rows = len(self.df_temp[col_name])
                    total_not_null = total_rows - self.df_temp[col_name].isnull().sum()
                    total_unique = self.df_temp[col_name].nunique(dropna=True)
                except KeyError:
                    self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Unique_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                    self.fail_msg.append('Column ' + col_name + ' not found in file')
                    # print(f'Column {val} {col_name} is Fail as column not found')
                    continue

            if is_unique == False:
                self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_Result'] = 'Pass'
                self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_TC-Count'] = self.tc_count
                # print(f'Column {val} {col_name} is Pass as duplicated values are allowed')
            elif is_unique == True:
                if total_not_null == total_unique:
                    self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Column {val} {col_name} is Pass as all values are unique')
                else:
                    self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Unique_C' + val + '_Comment'] = col_name + ' has duplicated values and is not unique'
                    self.fail_msg.append(col_name + ' has duplicated values and is not unique')
                    # print(f'Column {val} {col_name} is Fail as all values are not unique')
            else:
                self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Unique_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Unique_C' + val + '_Comment'] = col_name + ' is not tested as invalid option ' + is_unique + ' is provided'
                self.fail_msg.append(col_name + ' is not tested as invalid option ' + is_unique + ' is provided')

    def validate_sorted(self, temp_col_names, temp_sorted, temp_format, temp_infer):
        # This function validates if all the contents are sorted (it will compare after removing null values)

        temp_col_counter = 0  # This temporary variable stores column number. This variable is used to save results into the right column of dataframes self.df_results & df_summary
        is_number = True
        for col_name, is_sorted, is_format, to_infer in zip(temp_col_names, temp_sorted, temp_format, temp_infer):
            temp_col_counter += 1
            val = str(temp_col_counter)
            if is_sorted == 'DNT' or is_sorted == '':
                self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'DNT'
                self.df_results.at[
                    self.results_row_counter, 'Sorted_C' + val + '_Comment'] = col_name + ' is not tested based on input'
                self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = 0
                # print(f'Column {val} {col_name} is Not Tested based on input')
                continue
            else:
                try:
                    self.df_temp[col_name].astype(float)  # just to check if column exists
                except KeyError:
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Sorted_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                    self.fail_msg.append('Column ' + col_name + ' not found in file')
                    # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                    # temp_col_counter+=1
                    continue
                except ValueError:
                    is_number = False

            if to_infer == 'date':
                try:
                    if is_number == False:
                        self.df_temp[col_name] == pd.to_datetime(self.df_temp[col_name])
                    else:
                        pd.to_datetime(self.df_temp[col_name], format=is_format, errors='raise')
                except ValueError:
                    pass
            elif to_infer == 'int':
                try:
                    self.df_temp[col_name] = self.df_temp[col_name].astype(int)
                except ValueError:
                    pass
            elif to_infer == 'float':
                try:
                    self.df_temp[col_name] = self.df_temp[col_name].astype(float)
                except ValueError:
                    pass

            if is_sorted == 'ASC':
                # if all(temp_list[i] <= temp_list[i+1] for i in range(len(temp_list)-1)):

                if self.df_temp[col_name].dropna().is_monotonic_increasing:
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                else:
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Sorted_C' + val + '_Comment'] = col_name + ' is not sorted in Ascending order'
                    self.fail_msg.append(col_name + ' is not sorted in Ascending order')
                    # print(f'Column {val} {col_name} is Fail as all values are not in Ascending Order')
            elif is_sorted == 'DESC':
                # if all(temp_list[i] >= temp_list[i+1] for i in range(len(temp_list)-1)):
                if self.df_temp[col_name].dropna().is_monotonic_decreasing:
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'Pass'
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = self.tc_count
                    # print(f'Column {val} {col_name} is Pass as all values are in Descending Order')
                else:
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Sorted_C' + val + '_Comment'] = col_name + ' is not sorted in Descending order'
                    self.fail_msg.append(col_name + ' is not sorted in Descending order')
                    # print(f'Column {val} {col_name} is Fail as all values are not in Descending Order')
            else:
                # print('Reached Here, is_sorted =', is_sorted)
                self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Sorted_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Sorted_C' + val + '_Comment'] = col_name + ' is not tested as invalid option ' + str(
                    is_sorted) + ' is provided'
                self.fail_msg.append(col_name + ' is not tested as invalid option ' + str(is_sorted) + ' is provided')

    def validate_range(self, temp_col_names, temp_range, temp_format, temp_infer):
        # This function validates if all the contents are in specified range

        temp_col_counter = 0  # This temporary variable stores column number. This variable is used to save results into the right column of dataframes self.df_results & df_summary
        is_number = True
        for col_name, range_val, is_format, to_infer in zip(temp_col_names, temp_range, temp_format, temp_infer):
            temp_col_counter += 1
            val = str(temp_col_counter)
            range_val_split = []
            if range_val == '' or range_val == 'DNT' or range_val == 'nan':
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test per input'
                continue
            else:
                try:
                    self.df_temp[col_name].astype(float)  # just to check if column exists
                except KeyError:
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                    self.fail_msg.append('Column ' + col_name + ' not found in file')
                    # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                    # temp_col_counter+=1
                    continue
                except ValueError:
                    is_number = False

            if to_infer == 'string':
                try:
                    range_val_split = str(range_val).split(',')
                    for i in range(0, len(range_val_split)):
                        range_val_split[i] = str(range_val_split[i]).strip()
                except:
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid Range options provided with exception'
                    self.fail_msg.append('Invalid Range options provided with exception')
                    continue
                string_values_match = True
                if self.df_temp[col_name].nunique(dropna=True) <= len(range_val_split):
                    for item in self.df_temp[col_name].dropna().unique().tolist():
                        if item not in range_val_split:
                            string_values_match = False
                            break
                    if string_values_match == True:
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                        # self.df_results.at[self.results_row_counter, 'Range_C'+val+'_Comment'] = 'Invalid Range options provided with exception'
                    else:
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                        self.df_results.at[
                            self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column' + col_name + 'contains values other than specified range'
                        self.fail_msg.append('Column' + col_name + 'contains values other than specified range')
                else:
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column' + col_name + 'contains values other than specified range'
                    self.fail_msg.append('Column' + col_name + 'contains values other than specified range')

            elif to_infer == 'date':
                actual_min_value = ''
                actual_max_value = ''
                if len(range_val) > 0:
                    first_char = str(range_val[0])
                else:
                    first_char = ''
                range_val = str(range_val)[1:]
                max_val = 'NA'
                min_val = 'NA'
                is_number = True

                if first_char not in ['D', 'M', 'Y']:
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                    self.df_results.at[
                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid dage Range options provided for column ' + col_name + '. Starts with ' + first_char
                    self.fail_msg.append('Invalid dage Range options provided for column ' + col_name + '. Starts with ' + first_char)
                    continue
                elif is_format == '' or is_format == 'nan' or is_format == 'DNT':
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Format needed for validting range for date. ' + col_name + 's format is not provided'
                    self.fail_msg.append('Format needed for validting range for date. ' + col_name + 's format is not provided')
                    continue
                else:

                    try:
                        range_val_split = str(range_val).split('-')
                    except:
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                        self.df_results.at[
                            self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid Range options provided with exception'
                        self.fail_msg.append('Invalid Range options provided with exception')
                        continue

                    try:
                        self.df_temp[col_name].dropna().astype(int)  # just to check if column exists
                    except KeyError:
                        self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = 1
                        self.df_results.at[
                            self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                        self.fail_msg.append('Column ' + col_name + ' not found in file')
                        # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                        # temp_col_counter+=1
                        continue
                    except ValueError:
                        is_number = False
                    except TypeError:
                        # print('hit here')
                        pass

                    if len(range_val_split) != 2:
                        print('hihihi', range_val_split)
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                        self.df_results.at[
                            self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid Range options provided ' + str(
                            range_val_split)
                        self.fail_msg.append('Invalid Range options provided ' + str(range_val_split))
                        continue
                    elif is_number == True:
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                        self.df_results.at[
                            self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column ' + col_name + ' is numeric and not date'
                        self.fail_msg.append('Column ' + col_name + ' is numeric and not date')
                        continue

                    else:
                        for i in range(0, len(range_val_split)):
                            range_val_split[i] = str(range_val_split[i]).strip()
                        try:
                            pd.to_datetime(self.df_temp[col_name], format=is_format, errors='raise')
                        except ValueError:
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column ' + col_name + ' is not in the specfied format: ' + is_format
                            self.fail_msg.append('Column ' + col_name + ' is not in the specfied format: ' + is_format)
                            continue

                        else:
                            actual_min_value = pd.to_datetime(self.df_temp[col_name], format=is_format,
                                                              errors='raise').dropna().min().date()
                            actual_max_value = pd.to_datetime(self.df_temp[col_name], format=is_format,
                                                              errors='raise').dropna().max().date()
                            # actual_min_month = actual_min_value.month
                            # actual_max_month = actual_max_value.month
                            # actual_min_year = actual_min_value.year
                            # actual_max_year = actual_max_value.year

                        try:
                            if range_val_split[0].strip() != '' and range_val_split[1].strip() != '':
                                range_val_split[0] = int(range_val_split[0])
                                range_val_split[1] = int(range_val_split[1])
                            else:
                                if range_val_split[0].strip() == '':
                                    max_val = float(range_val_split[1].strip())
                                    range_val_split[1] = int(range_val_split[1])
                                else:
                                    min_val = float(range_val_split[0].strip())
                                    range_val_split[0] = int(range_val_split[0])
                        except ValueError:
                            # print('hihihi')
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Date Range options for column ' + col_name + ' provided: ' + str(
                                range_val_split)
                            self.fail_msg.append('Incompatible Date Range options for column ' + col_name + ' provided: ' + str(range_val_split))
                            continue
                        if first_char == 'D':
                            if min_val == 'NA' and max_val == 'NA':
                                try:
                                    # print(actual_min_value, actual_max_value)
                                    # print(self.file_date-datetime.timedelta(range_val_split[0]), self.file_date-datetime.timedelta(range_val_split[1]))
                                    if actual_min_value < self.file_date - datetime.timedelta(
                                            range_val_split[1]) or actual_max_value > self.file_date - datetime.timedelta(
                                            range_val_split[0]):
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values outside prescribed range'
                                        self.fail_msg.append(col_name + ' has values outside prescribed range')
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                except TypeError:
                                    # print('reached here both min and max val', range_val_split[0], range_val_split[1])
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                                    continue
                            elif min_val != 'NA' and max_val == 'NA':
                                try:
                                    if actual_max_value > self.file_date - datetime.timedelta(range_val_split[0]):
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values less than prescribed minimum value ' + str(
                                            range_val_split[0])
                                        self.fail_msg.append(col_name + ' has values less than prescribed minimum value ' + str(
                                            range_val_split[0]))
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                except TypeError:
                                    # print('reached here max val', type(self.df_temp[col_name].dropna().min()), type(range_val_split[0]))
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                            elif min_val == 'NA' and max_val != 'NA':
                                try:
                                    if actual_min_value < self.file_date - datetime.timedelta(range_val_split[1]):
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values greater than prescribed minimum value ' + str(
                                            range_val_split[1])
                                        self.fail_msg.append(
                                            col_name + ' has values greater than prescribed minimum value ' + str(
                                                range_val_split[1]))
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                except TypeError:
                                    print('reached here min val', self.df_temp[col_name].dropna().max(), range_val_split[1])
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                                    continue
                            else:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                                self.df_results.at[
                                    self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test due to unexpected reason'
                                self.fail_msg.append('Did not test due to unexpected reason')

                        elif first_char == 'M':

                            if min_val == 'NA' and max_val == 'NA':
                                derived_min_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(months=range_val_split[1])
                                derived_max_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(months=range_val_split[0] - 1)
                                try:
                                    # print(self.file_date, pd.to_datetime(self.file_date.strftime(format = '%m-%Y'), format='%m-%Y').date(), filename)
                                    # print(derived_min_value, derived_max_value)
                                    if actual_min_value < derived_min_value or actual_max_value >= derived_max_value:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values outside prescribed range'
                                        self.fail_msg.append(col_name + ' has values outside prescribed range')
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                except TypeError:
                                    print('reached here both min and max val', range_val_split[0], range_val_split[1])
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                                    continue
                            elif min_val != 'NA' and max_val == 'NA':
                                # derived_min_value = pd.to_datetime(self.file_date.strftime(format = '%m-%Y'), format='%m-%Y').date() - \
                                #                    dateutil.relativedelta.relativedelta(months=range_val_split[1])
                                derived_max_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(months=range_val_split[0] - 1)

                                try:
                                    if actual_max_value >= derived_max_value:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values less than prescribed minimum value ' + str(
                                            range_val_split[0])
                                        self.fail_msg.append(col_name + ' has values less than prescribed minimum value ' + str(
                                            range_val_split[0]))
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                except TypeError:
                                    # print('reached here max val', type(self.df_temp[col_name].dropna().min()), type(range_val_split[0]))
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                            elif min_val == 'NA' and max_val != 'NA':
                                derived_min_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(months=range_val_split[1])
                                # derived_max_value = pd.to_datetime(self.file_date.strftime(format = '%m-%Y'), format='%m-%Y').date() - \
                                #                    dateutil.relativedelta.relativedelta(months=range_val_split[0]-1)
                                try:
                                    if actual_min_value < derived_min_value:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values greater than prescribed minimum value ' + str(
                                            range_val_split[1])
                                        self.fail_msg.append(
                                            col_name + ' has values greater than prescribed minimum value ' + str(
                                                range_val_split[1]))
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                except TypeError:
                                    #print('reached here min val', self.df_temp[col_name].dropna().max(), range_val_split[1])
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                                    continue
                            else:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'DNT'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                                self.df_results.at[
                                    self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test due to unexpected reason'
                                self.fail_msg.append('Did not test due to unexpected reason')

                        elif first_char == 'Y':

                            if min_val == 'NA' and max_val == 'NA':
                                derived_min_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(years=range_val_split[1])
                                derived_max_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(years=range_val_split[0] - 1)
                                try:
                                    # print(self.file_date, pd.to_datetime(self.file_date.strftime(format = '%m-%Y'), format='%m-%Y').date(), filename)
                                    # print(derived_min_value, derived_max_value)
                                    if actual_min_value < derived_min_value or actual_max_value >= derived_max_value:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values outside prescribed range'
                                        self.fail_msg.append(col_name + ' has values outside prescribed range')
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                except TypeError:
                                    print('reached here both min and max val', range_val_split[0], range_val_split[1])
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                                    continue
                            elif min_val != 'NA' and max_val == 'NA':
                                # derived_min_value = pd.to_datetime(self.file_date.strftime(format = '%m-%Y'), format='%m-%Y').date() - \
                                #                    dateutil.relativedelta.relativedelta(months=range_val_split[1])
                                derived_max_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(months=range_val_split[0] - 1)

                                try:
                                    if actual_max_value >= derived_max_value:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values less than prescribed minimum value ' + str(
                                            range_val_split[0])
                                        self.fail_msg.append(col_name + ' has values less than prescribed minimum value ' + str(
                                            range_val_split[0]))
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                except TypeError:
                                    print('reached here max val', type(self.df_temp[col_name].dropna().min()),
                                          type(range_val_split[0]))
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 1
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                            elif min_val == 'NA' and max_val != 'NA':
                                derived_min_value = pd.to_datetime(self.file_date.strftime(format='%m-%Y'),
                                                                   format='%m-%Y').date() - \
                                                    dateutil.relativedelta.relativedelta(months=range_val_split[1])
                                # derived_max_value = pd.to_datetime(self.file_date.strftime(format = '%m-%Y'), format='%m-%Y').date() - \
                                #                    dateutil.relativedelta.relativedelta(months=range_val_split[0]-1)
                                try:
                                    if actual_min_value < derived_min_value:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                        self.df_results.at[
                                            self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values greater than prescribed minimum value ' + str(
                                            range_val_split[1])
                                        self.fail_msg.append(
                                            col_name + ' has values greater than prescribed minimum value ' + str(
                                                range_val_split[1]))
                                        # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                                    else:
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                except TypeError:
                                    #print('reached here min val', self.df_temp[col_name].dropna().max(), range_val_split[1])
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                                    self.df_results.at[
                                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                                    self.fail_msg.append('Incompatible Range options provided with exception')
                                    continue
                            else:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                                self.df_results.at[
                                    self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test due to unexpected reason'
                                self.fail_msg.append('Did not test due to unexpected reason')

            elif to_infer == '' or to_infer == 'nan':
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test as to_infer is blank for column ' + col_name

            elif to_infer not in ['int', 'float']:
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test as value of to_infer is invalid for column ' + col_name
                self.fail_msg.append('Did not test as value of to_infer is invalid for column ' + col_name)
            else:
                max_val = 'NA'
                min_val = 'NA'
                try:
                    range_val_split = str(range_val).split('-')
                except:
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                    self.df_results.at[
                        self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid Range options provided with exception'
                    self.fail_msg.append('Invalid Range options provided with exception')

                if len(range_val_split) != 2:
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                    self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid Range options provided'
                    self.fail_msg.append('Invalid Range options provided')
                    continue
                else:
                    for i in range(0, len(range_val_split)):
                        range_val_split[i] = str(range_val_split[i]).strip()
                    if to_infer not in ['date', 'int', 'float', '', 'nan']:
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Invalid to_infer option'
                        self.fail_msg.append('Invalid to_infer option')

                    elif to_infer == 'date':
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'DNT'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                        self.df_results.at[
                            self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Range functionality for Date datatype is not ready'
                        self.fail_msg.append('Range functionality for Date datatype is not ready')
                        """
                        try:
                            if is_number == False:
                                self.df_temp[col_name] == pd.to_datetime(self.df_temp[col_name])
                                range_val_split[0] = int(range_val_split[0])
                                range_val_split[1] = int(range_val_split[1])
                        except ValueError:
                            pass
                        """
                    elif to_infer == 'int':
                        try:
                            # self.df_temp[col_name] == self.df_temp[col_name].dropna().astype(int)
                            pd.Series(self.df_temp[col_name].dropna().drop_duplicates().astype(int), dtype='Int64')
                        except ValueError:
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column ' + col_name + ' is not int datatype'
                            self.fail_msg.append('Column ' + col_name + ' is not int datatype')
                            continue
                        try:
                            if range_val_split[0].strip() != '' and range_val_split[1].strip() != '':
                                range_val_split[0] = float(range_val_split[0])
                                range_val_split[1] = float(range_val_split[1])
                            else:
                                if range_val_split[0].strip() == '':
                                    max_val = float(range_val_split[1].strip())
                                else:
                                    min_val = float(range_val_split[0].strip())
                        except ValueError:
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                            self.fail_msg.append('Incompatible Range options provided with exception')

                    elif to_infer == 'float':
                        try:
                            self.df_temp[col_name] == self.df_temp[col_name].astype(float)
                            self.df_temp[col_name].dropna().drop_duplicates().astype(float)
                        except ValueError:
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Column' + col_name + ' is not float datatype'
                            self.fail_msg.append('Column' + col_name + ' is not float datatype')
                            continue
                        try:
                            if range_val_split[0].strip() != '' and range_val_split[1].strip() != '':
                                range_val_split[0] = float(range_val_split[0])
                                range_val_split[1] = float(range_val_split[1])
                            else:
                                if range_val_split[0].strip() == '':
                                    max_val = float(range_val_split[1].strip())
                                    range_val_split[1] = float(range_val_split[1])
                                else:
                                    min_val = float(range_val_split[0].strip())
                                    range_val_split[0] = float(range_val_split[0])
                        except ValueError:
                            # print('hihihi')
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                            self.fail_msg.append('Incompatible Range options provided with exception')

                    if min_val == 'NA' and max_val == 'NA':
                        try:
                            if float(self.df_temp[col_name].dropna().min()) < range_val_split[0] or float(self.df_temp[col_name].dropna().max()) > \
                                    range_val_split[1]:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                                self.df_results.at[
                                    self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values outside prescribed range'
                                self.fail_msg.append(col_name + ' has values outside prescribed range')
                                # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                            else:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count * 2
                        except TypeError:
                            print('reached here both min and max val', range_val_split[0], range_val_split[1])
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                            self.fail_msg.append('Incompatible Range options provided with exception')
                            continue
                    elif min_val != 'NA' and max_val == 'NA':
                        try:
                            if float(self.df_temp[col_name].dropna().min()) < range_val_split[0]:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                self.df_results.at[
                                    self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values less than prescribed minimum value ' + str(
                                    range_val_split[0])
                                self.fail_msg.append(
                                    col_name + ' has values less than prescribed minimum value ' + str(range_val_split[0]))
                                # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                            else:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                        except TypeError:
                            #print('reached here max val', type(self.df_temp[col_name].dropna().min()), type(range_val_split[0]))
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                            self.fail_msg.append('Incompatible Range options provided with exception')
                    elif min_val == 'NA' and max_val != 'NA':
                        try:
                            if float(self.df_temp[col_name].dropna().max()) > range_val_split[1]:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                                self.df_results.at[
                                    self.results_row_counter, 'Range_C' + val + '_Comment'] = col_name + ' has values greater than prescribed minimum value ' + str(
                                    range_val_split[1])
                                self.fail_msg.append(col_name + ' has values greater than prescribed minimum value ' + str(
                                    range_val_split[1]))
                                # print(f'Column {val} {col_name} is Pass as all values are in Ascending Order')
                            else:
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Pass'
                                self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = self.tc_count
                        except TypeError:
                            #print('reached here min val', self.df_temp[col_name].dropna().max(), range_val_split[1])
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                            self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                            self.df_results.at[
                                self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Incompatible Range options provided with exception'
                            self.fail_msg.append('Incompatible Range options provided with exception')
                            print('XXX')
                    else:
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_Result'] = 'Fail'
                        self.df_results.at[self.results_row_counter, 'Range_C' + val + '_TC-Count'] = 0
                        self.df_results.at[
                            self.results_row_counter, 'Range_C' + val + '_Comment'] = 'Did not test due to unexpected reason'
                        self.fail_msg.append('Did not test due to unexpected reason')

    def validate_format(self, temp_col_names, temp_format, temp_infer):
        # This function validates if all the contents are in right format
        temp_col_counter = 0  # This temporary variable stores column number. This variable is used to save results into the right column of dataframes self.df_results & df_summary
        is_number = True
        final_status = 'DNT'
        final_tc_count = 0
        final_msg = ''
        for col_name, is_format, to_infer in zip(temp_col_names, temp_format, temp_infer):
            fff = time.time()
            temp_col_counter += 1
            val = str(temp_col_counter)
            if is_format == '' or is_format == 'DNT':
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' format is not tested based on input'
                continue
            else:
                try:
                    self.df_temp[col_name].astype(float)  # just to check if column exists
                except KeyError:
                    self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = 1
                    self.df_results.at[
                        self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' not found in file'
                    self.fail_msg.append('Column ' + col_name + ' not found in file')
                    # print(f'Column {temp_col_counter} ({col_name}) is Fail (Column {col_name}) not found in file')
                    # temp_col_counter+=1
                    continue
                except (ValueError, TypeError):
                    is_number = False

            if to_infer == 'date':
                try:
                    pd.to_datetime(self.df_temp[col_name], format=is_format, errors='raise')
                except ValueError:
                    # print(is_format, self.df_temp[col_name].iloc[0])
                    self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' is not in ' + is_format + ' format'
                    self.fail_msg.append('Column ' + col_name + ' is not in ' + is_format + ' format')
                    continue
                # print(f'here, is_number = {is_number} and to_infer = {to_infer} and col_name = {col_name}')
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'Pass'
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = self.tc_count
                # self.df_results.at[self.results_row_counter, 'Format_C'+val+'_Comment'] = 'Column '+col_name+' not found in file'
                """
                else:
                    self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'Fail'
                    self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = self.tc_count
                    self.df_results.at[
                        self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' is numeric and not date format'
                    self.fail_msg.append('Column ' + col_name + ' is numeric and not date format')
                """
            elif to_infer in ['string', 'int', 'float']:
                temp_store = []
                temp_tc_count_store = []
                temp_result_store = []
                temp_msg_store = []
                # if is_number == False:
                to_check = is_format.split(',')
                for i in range(0, len(to_check)):
                    to_check[i] = str(to_check[i]).lstrip()
                    temp_store = to_check[i].split(':')
                    if temp_store[0].lstrip() == 'starts_with':
                        try:
                            unique_count = self.df_temp[col_name].dropna().drop_duplicates().apply(
                                lambda x: str(x)[:len(temp_store[1])]).nunique()
                            if len(self.df_temp[col_name].dropna()[:1]) == 0:
                                first_val = True
                            else:
                                first_val = \
                                self.df_temp[col_name].dropna()[:1].apply(lambda x: str(x)[:len(temp_store[1])]).iloc[0]
                        except IndexError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        if len(temp_store) != 2:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters')
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters')
                            continue
                        if unique_count == 1 and first_val == temp_store[1]:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        else:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(self.tc_count)
                            temp_msg_store.append(
                                'Column ' + col_name + ' has values that does not start with ' + temp_store[1])
                            self.fail_msg.append('Column ' + col_name + ' has values that does not start with ' + temp_store[1])
                    elif temp_store[0].lstrip() == 'ends_with':
                        try:
                            unique_count = self.df_temp[col_name].dropna().drop_duplicates().apply(
                                lambda x: str(x)[0 - len(temp_store[1]):]).nunique()
                            if len(self.df_temp[col_name].dropna()[:1]) == 0:
                                first_val = True
                            else:
                                first_val = \
                                self.df_temp[col_name].dropna()[:1].apply(lambda x: str(x)[0 - len(temp_store[1]):]).iloc[0]
                        except IndexError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        if len(temp_store) != 2:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters')
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters')
                            continue
                        if unique_count == 1 and first_val == temp_store[1]:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        else:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(self.tc_count)
                            temp_msg_store.append(
                                'Column ' + col_name + ' has values that does not end with ' + temp_store[1])
                            self.fail_msg.append('Column ' + col_name + ' has values that does not end with ' + temp_store[1])
                    elif temp_store[0].lstrip() == 'contains':
                        try:
                            unique_count = self.df_temp[col_name].dropna().drop_duplicates().apply(
                                lambda x: temp_store[1] if temp_store[1] in str(x) else False).nunique()
                            if len(self.df_temp[col_name].dropna()[:1]) == 0:
                                first_val = True
                            else:
                                first_val = self.df_temp[col_name].dropna()[:1].apply(
                                    lambda x: temp_store[1] if temp_store[1] in str(x) else False).iloc[0]
                        except IndexError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        if len(temp_store) != 2:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters')
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters')
                            continue
                        if unique_count == 1 and first_val == temp_store[1]:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        else:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(self.tc_count)
                            temp_msg_store.append(
                                'Column ' + col_name + ' has values that does not contain ' + temp_store[1])
                            self.fail_msg.append('Column ' + col_name + ' has values that does not contain ' + temp_store[1])

                    elif temp_store[0].lstrip() == 'length_equals':
                        try:
                            temp_store[1] = int(str(temp_store[1]).lstrip())
                            unique_count = self.df_temp[col_name].dropna().drop_duplicates().apply(
                                lambda x: True if len(str(x)) == temp_store[1] else False).nunique()
                            if len(self.df_temp[col_name].dropna()[:1]) == 0:
                                first_val = True
                            else:
                                first_val = self.df_temp[col_name].dropna()[:1].apply(
                                    lambda x: True if len(str(x)) == temp_store[1] else False).iloc[0]
                        except IndexError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column ' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column ' + col_name)
                            continue
                        except ValueError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column ' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column ' + col_name)
                            continue
                        except KeyError:
                            # print(col_name, filename, temp_store)
                            continue
                        if len(temp_store) != 2:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters')
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters')
                            continue
                        # print(type(unique_count), type(first_val))
                        if unique_count == 0 and first_val == True and temp_store[1] == 0:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        elif unique_count == 1 & first_val == True:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        else:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(self.tc_count)
                            temp_msg_store.append(
                                'Column ' + col_name + ' has values whose length is not equal to ' + str(temp_store[1]))
                            self.fail_msg.append(
                                'Column ' + col_name + ' has values whose length is not equal to ' + str(temp_store[1]))

                    elif temp_store[0].lstrip() == 'length_greater_than':
                        try:
                            temp_store[1] = int(str(temp_store[1]).lstrip())
                            unique_count = self.df_temp[col_name].dropna().drop_duplicates().apply(
                                lambda x: True if len(str(x)) > temp_store[1] else False).nunique()
                            if len(self.df_temp[col_name].dropna()[:1]) == 0:
                                first_val = True
                            else:
                                first_val = self.df_temp[col_name].dropna()[:1].apply(
                                    lambda x: True if len(str(x)) > temp_store[1] else False).iloc[0]
                        except IndexError:
                            # print('hi', temp_store[1], int(str(temp_store[1]).lstrip()), col_name)
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        except ValueError:

                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        if len(temp_store) != 2:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters')
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters')
                            continue
                        if unique_count == 1 and first_val == True:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        else:
                            # print(unique_count, first_val)
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(self.tc_count)
                            temp_msg_store.append(
                                'Column ' + col_name + ' has values whose length is not greater than ' + str(temp_store[1]))
                            self.fail_msg.append(
                                'Column ' + col_name + ' has values whose length is not greater than ' + str(temp_store[1]))

                    elif temp_store[0].lstrip() == 'length_lesser_than':
                        try:
                            temp_store[1] = int(str(temp_store[1]).lstrip())
                            unique_count = self.df_temp[col_name].dropna().drop_duplicates().apply(
                                lambda x: True if len(str(x)) < temp_store[1] else False).nunique()
                            if len(self.df_temp[col_name].dropna()[:1]) == 0:
                                first_val = True
                            else:
                                first_val = self.df_temp[col_name].dropna()[:1].apply(
                                    lambda x: True if len(str(x)) < temp_store[1] else False).iloc[0]
                        except IndexError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        except ValueError:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters for the column' + col_name)
                            continue
                        if len(temp_store) != 2:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(1)
                            temp_msg_store.append(str(to_check[i]) + ' has invalid parameters')
                            self.fail_msg.append(str(to_check[i]) + ' has invalid parameters')
                            continue
                        if unique_count == 0 and first_val == True:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        elif unique_count == 1 and first_val == True:
                            temp_result_store.append('Pass')
                            temp_tc_count_store.append(self.tc_count)
                        else:
                            temp_result_store.append('Fail')
                            temp_tc_count_store.append(self.tc_count)
                            temp_msg_store.append(
                                'Column ' + col_name + ' has values whose length is not lesser than ' + str(temp_store[1]))
                            self.fail_msg.append(
                                'Column ' + col_name + ' has values whose length is not lesser than ' + str(temp_store[1]))
                    else:
                        temp_result_store.append('Fail')
                        temp_tc_count_store.append(1)
                        temp_msg_store.append('Invalid option ' + temp_store[0])
                        self.fail_msg.append('Invalid option ' + temp_store[0])

                # print(temp_result_store, '\n', temp_tc_count_store, temp_msg_store)
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = self.get_master_results(temp_result_store)
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = sum(temp_tc_count_store)
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Comment'] = str(temp_msg_store)

            elif to_infer in ['', 'nan']:
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'DNT'
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' has no to_infer value specified'
            else:
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_Result'] = 'Fail'
                self.df_results.at[self.results_row_counter, 'Format_C' + val + '_TC-Count'] = 0
                self.df_results.at[
                    self.results_row_counter, 'Format_C' + val + '_Comment'] = 'Column ' + col_name + ' has invalid to_infer value specified'
                self.fail_msg.append('Column ' + col_name + ' has invalid to_infer value specified')
            ggg = time.time()
            # if filename[0] == 'd':
            #    print('colname = ', col_name, ggg-fff)

    def validate_col_count(self, temp_col_names, expected_col_count):
        # This function validates the number of columns in the test file
        if expected_col_count == 0 or expected_col_count == '' or expected_col_count == 'DNT':
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_Result'] = 'DNT'
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_TC-Count'] = 0
        elif len(temp_col_names) >= expected_col_count:
            # print(f'Reached here {len(temp_col_names)} {expected_col_count}')
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_Result'] = 'Pass'
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_TC-Count'] = 1
            # print(f'{filename} has expected number of columns {len(temp_col_names)}')
        else:
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_Result'] = 'Fail'
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_TC-Count'] = 1
            self.df_results.at[self.results_row_counter, 'Number_Of_Cols_Comment'] = self.file_name + ' has ' + str(
                len(temp_col_names)) + ' columns but should have had ' + str(expected_col_count)
            self.fail_msg.append(
                self.file_name + ' has ' + str(len(temp_col_names)) + ' columns but should have had ' + str(expected_col_count))
            # print(f'{filename} has {len(temp_col_names)} columns but should have had {expected_col_count}')

    def validate_source(self):
        script_exec_start = time.time()
        loop_begin = time.time()
        temp_col_names, temp_datatypes, temp_null, temp_unique, no_schema, temp_infer, \
        temp_sorted, temp_range, temp_format, file_sizes, exec_times, col_counts, row_counts = ([] for i in range(13))

        col_count = len(self.df_temp.columns)
        #print('reached here')
        current_file_col_names = self.df_temp.columns.tolist()

        for iter1, iter2, iter3, iter4, iter5, iter6, iter7, iter8 in \
                zip(range(1, (self.temp[0] * 8) + 1, 8), range(2, (self.temp[0] * 8) + 1, 8), range(3, (self.temp[0] * 8) + 1, 8), range(4, (self.temp[0] * 8) + 1, 8), \
                    range(5, (self.temp[0] * 8) + 1, 8), range(6, (self.temp[0] * 8) + 1, 8), range(7, (self.temp[0] * 8) + 1, 8), range(8, (self.temp[0] * 8) + 1, 8)):
            temp_col_names.append(self.temp[iter1])
            temp_datatypes.append(self.temp[iter2])
            temp_infer.append(self.temp[iter3])
            temp_null.append(self.temp[iter4])
            temp_unique.append(self.temp[iter5])
            temp_sorted.append(self.temp[iter6])
            temp_range.append(self.temp[iter7])
            temp_format.append(self.temp[iter8])

        self.validate_col_count(current_file_col_names, self.temp[0])
        self.validate_col_names(temp_col_names, current_file_col_names)
        self.validate_datatype(temp_col_names, temp_datatypes)
        self.deep_validate_datatype(temp_col_names, temp_datatypes, temp_format, temp_infer)
        self.validate_null(temp_col_names, temp_null)
        self.validate_is_unique(temp_col_names, temp_unique)
        self.validate_sorted(temp_col_names, temp_sorted, temp_format, temp_infer)
        ddd = time.time()
        self.validate_range(temp_col_names, temp_range, temp_format, temp_infer)
        eee = time.time()
        # if filename[0] == 'd':
        #    print('range time is ', eee-ddd)

        self.validate_format(temp_col_names, temp_format, temp_infer)
        # if filename[0] == 'd':
        #    print('format time is ', time.time()-ddd)
        # validate_unqiue

        file_sizes.append('NA')
        row_counts.append(len(self.df_temp))
        col_counts.append(len(self.df_temp.columns))

        temp_col_names.clear()
        temp_datatypes.clear()
        temp_infer.clear()
        temp_null.clear()
        temp_unique.clear()
        temp_sorted.clear()
        temp_range.clear()
        temp_format.clear()

        self.results_row_counter += 1
        loop_end = time.time()
        exec_times.append(round(loop_end - loop_begin, 3))
        # print('Done')

        # self.df_results.drop(['Filename_Comment', 'skip_top_rows_Comment', 'skip_bottom_rows_Comment'], axis=1, inplace=True)
        # print(self.df_results)
        # self.df_results.fillna('', inplace=True)
        # self.df_results.to_csv('Results_Details.csv', index=False)
        script_exec_end = time.time()
        exec_time = script_exec_end - script_exec_start
        """
        if exec_time < 60:
            print('Script execution completed. Time taken is', round(exec_time, 2), 'seconds')
        elif (exec_time >= 60 & exec_time <= 3600):
            print('Script execution completed. Time taken is', round(exec_time/60, 2), 'minutes')
        else:
            print('Script execution completed. Time taken is', round(exec_time/3600, 2), 'hours')
        """

        total_validations = self.df_results[self.df_results.columns[pd.Series(self.df_results.columns).str.endswith('_TC-Count')]].apply(
            lambda x: int(x.sum() + 1), axis=1).sum() + len(no_schema)

        to_words = inflect.engine()
        df_stats = pd.DataFrame()
        df_stats['Header'] = [''] * 5
        #print('Validation Script completed execution in ' + str(self.format_exec_time((exec_time))))
        df_stats['Header'].loc[0] = 'Validation Script completed execution in ' + str(self.format_exec_time(exec_time))
        #print('Files to be validated: ' + str(len(self.df_results) + len(no_schema)))
        df_stats['Header'].loc[1] = 'Files to be validated: ' + str(len(self.df_results) + len(no_schema))
        #print('Total Files validated: ' + str(len(self.df_results)))
        df_stats['Header'].loc[2] = 'Total Files validated: ' + str(len(self.df_results))
        #print('Aggregate size of Files validated: ' + str(file_sizes))
        df_stats['Header'].loc[3] = 'Aggregate size of Files validated: ' + str(file_sizes)
        #print('Total Validations Done: ' + str(total_validations) + ' (' + to_words.number_to_words(total_validations) + ')')
        df_stats['Header'].loc[4] = 'Total Validations Done: ' + str(total_validations) + ' (' + to_words.number_to_words(
            total_validations) + ')'

        df_summary = pd.DataFrame()

        #storing the current timestamp value into df_summary
        temp_ts_store = []
        # this will give time in UTC
        temp_ts_store.append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        df_summary['TimeStamp'] = temp_ts_store

        df_summary['Filename'] = self.df_results['Filename']
        df_summary['Result'] = self.df_results.apply(lambda x: self.get_master_results(list(x)), axis=1)
        # df_summary['Master_Result'] = self.df_results.apply(lambda x: 'Fail' if 'Fail' in list(x) else 'Pass', axis=1)
        df_summary['Validations'] = self.df_results[
            self.df_results.columns[pd.Series(self.df_results.columns).str.endswith('_TC-Count')]].apply(lambda x: int(x.sum() + 1),
                                                                                               axis=1)
        df_summary['File_Size'] = file_sizes
        df_summary['Exec_Time'] = exec_times
        df_summary['Rows'] = row_counts
        df_summary['Columns'] = col_counts

        df_summary['Col_Count'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Number_Of_Cols')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)

        df_summary['Col_Name'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Name_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)

        df_summary['Datatype'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Datatype_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)

        df_summary['Infer_Dtype'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Infer_Datatype_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)

        df_summary['Null'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Null_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)
        df_summary['Unique'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Unique_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)
        df_summary['Sort'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Sorted_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)
        df_summary['Range'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Range_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)
        df_summary['Format'] = self.df_results \
            [self.df_results.columns[pd.Series(self.df_results.columns).str.startswith('Format_C')] & self.df_results.columns[
                pd.Series(self.df_results.columns).str.endswith('_Result')]] \
            .apply(lambda x: self.get_master_results(list(x)), axis=1)

        for i in range(len(no_schema)):
            to_append = [no_schema[i]] + ['No Schema', 1, 0, 0] + ['DNT'] * 11
            a_series = pd.Series(to_append, index=df_summary.columns)
            df_summary = df_summary.append(a_series, ignore_index=True)

        df_summary.sort_values(by=['Result', 'Filename'], inplace=True)
        df_summary.reset_index(drop=True, inplace=True)
        """
        df_summary.at[-1,['Filename', 'Result', 'Validations', 'File_Size', 'Exec_Time', 'Rows', 'Columns', 'Col_Count', 'Col_Name', 'Datatype', 'Infer_Dtype', 'Null', 'Unique', 'Sort', \
                          'Range', 'Format']] = ['Aggregate/Summary of '+str(len(df_summary))+' Files', self.get_master_results(df_summary['Result'].tolist()), df_summary['Validations'].sum(), \
                         df_summary['File_Size'].sum(), exec_time, '-', '-', self.get_master_results(df_summary['Col_Count'].tolist()), self.get_master_results(df_summary['Col_Name'].tolist()), \
                         self.get_master_results(df_summary['Datatype'].tolist()), self.get_master_results(df_summary['Infer_Dtype'].tolist()), self.get_master_results(df_summary['Null'].tolist()), \
                         self.get_master_results(df_summary['Unique'].tolist()), \
                         self.get_master_results(df_summary['Sort'].tolist()), self.get_master_results(df_summary['Range'].tolist()), self.get_master_results(df_summary['Format'].tolist())]

        """
        df_summary[['Validations', 'File_Size']] = df_summary[['Validations', 'File_Size']]
        # df_summary['File_Size'] = df_summary['File_Size'].apply(lambda x: self.format_file_size(x))
        df_summary['Exec_Time'] = df_summary['Exec_Time'].apply(lambda x: self.format_exec_time(x))
        df_summary.sort_index(inplace=True)
        df_summary.index += 1

        # df_summary['File_Size'] = df_summary[df_summary['Result'] == 'No Schema']['File_Size'] = 'DNT'

        df_summary.loc[df_summary['Result'] == 'No Schema', ['File_Size', 'Exec_Time']] = 'DNT'

        df_summary.drop('File_Size', axis=1, inplace=True) # Removing the File_Size column

        # df_stats.to_html('Result_Summary.html', header=False, index=False, border=0)
        # f = open('Result_Summary.html', 'a')
        # f.write(df_summary.style.set_properties(
        #     **{'font-size': '11.5pt', 'background-color': '#edeeef', 'border-color': 'black', 'border-style': 'solid', \
        #        'border-width': '1px', 'padding': '6px', 'border-collapse': 'collapse', 'text-align': 'center'}) \
        #         .set_table_styles(
        #     [{'selector': 'th', 'props': [('font-size', '12pt'), ('background-color', '#edeeef'), ('border-color', 'black'), \
        #                                   ('border-style', 'solid'), ('border-collapse', 'collapse'), ('padding', '6px')]}]) \
        #         .applymap(self.highlight_vals).render())
        # f.close()

        # df_summary.to_csv('Result_Summary.csv')
        #print(df_summary)
        #print(df_summary['Result'].iloc[0])
        #print(self.fail_msg)
        return df_summary['Result'].iloc[0], self.fail_msg, df_summary, self.df_results
