import logging
import pandas as pd


class PandasUtils:
    """ PANDAS UTILS CLASS """
    def read_csv_using_python_engine(self, csv_files_path, number_of_footer_lines_to_skip=0, skip_blank_line=False):
        """ Function will read csv file using python engine and returns the dataframe
            INPUT:
                csv_files_path - csv files path
                number_of_footer_lines_to_skip - number of footer lines to be skipped while reading
                skip_blank_line - skip lines with no data while reading csv file
            OUTPUT:
                return pandas dataframe
        """
        dataframe = pd.read_csv(csv_files_path, skip_blank_lines=skip_blank_line, skipfooter=number_of_footer_lines_to_skip, engine='python')
        return dataframe

    def get_dataframe_from_csv(self, csv_files_path, file_encoding='utf-8', number_of_starting_rows_to_skip=0, number_of_starting_rows_to_consider=0, delimiter_to_use=',', csv_header='infer'):
        """ Read dataframe from csv file
        INPUT:
            csv_files_path - csv files path
            number_of_starting_rows_to_skip - starting number of rows to skip in csv file
            number_of_starting_rows_to_consider - starting number of rows to consider in csv file
            file_encoding - encoding type
            delimiter_to_use - delimiter to read csv file
            csv_header - header type
        OUTPUT:
            return pandas dataframe
        """
        if number_of_starting_rows_to_consider:
            dataframe = pd.read_csv(csv_files_path, encoding=file_encoding, nrows=number_of_starting_rows_to_consider, delimiter=delimiter_to_use, header=csv_header)
        else:
            dataframe = pd.read_csv(csv_files_path, encoding=file_encoding, skiprows=number_of_starting_rows_to_skip, delimiter=delimiter_to_use, header=csv_header)
        return dataframe

    def combine_files_to_dataframe(self, csv_files_path_list, starting_number_of_rows_to_skip=0):
        """ Utils to combine all the csv files inside folder
        INPUT:
            csv_files_path_list - list of csv files path
            starting_number_of_rows_to_skip - starting number of rows to skip in csv file
        OUTPUT:
            Merge csv and return dataframe
        """
        csv_data_frame_list = []
        for each_csv_file in csv_files_path_list:
            csv_dataframe = pd.read_csv(each_csv_file, skiprows=range(0, starting_number_of_rows_to_skip), index_col=None)
            csv_data_frame_list.append(csv_dataframe)
        concatinated_csv_dataframe = pd.concat(csv_data_frame_list, axis=0, ignore_index=True)
        return concatinated_csv_dataframe

    def insert_column_in_csv(self, dataframe, index_to_insert, column_name, value_to_insert=None):
        """Insert a column into dataframe
            INPUT:
                dataframe - input dataframe
                index_to_insert - Index number to insert new column
                column_name - column_name to be inserted in csv
                value_to_insert - value in the column
            OUTPUT:
                return dataframe
        """
        dataframe.insert(loc=index_to_insert, column=column_name, value=value_to_insert)
        return dataframe

    def get_dataframe_from_excel(self, full_path_to_excel_file, sheet_name_to_read, date_columns_list):
        """ Read dataframe from excel file
            INPUT:
                full_path_to_excel_file: Full path with name
                sheet_name_to_read - sheet name in xls file to read
                date_columns_list - If any columns of type date to retain same format while reading
            OUTPUT:
                return dataframe
        """
        column_type_convertion_dict = {}
        for each_date_column in date_columns_list:
            column_type_convertion_dict[each_date_column] = pd.to_datetime
        dataframe = pd.read_excel(full_path_to_excel_file, sheet_name=sheet_name_to_read, converters=column_type_convertion_dict)
        return dataframe

    def create_xls_sheet_from_dataframe(self, dataframe, output_file_path_and_name, xl_date_format='MM/DD/YYYY', use_excel_writer=False):
        """ Creates csv files and write into specified path as output
            INPUT:
                dataframe - pandas dataframe
                output_file_path_and_name - output file name with path
                use_excel_writer - pandas ExcelWriter library to write xls file
                xl_date_format - date format used in ExcelWriter while writing into file
            OUTPUT:
                generate xls file from dataframe
        """
        if use_excel_writer:
            logging.info("Writing into xl using ExcelWriter")
            with pd.ExcelWriter(output_file_path_and_name,
                                date_format=xl_date_format,
                                datetime_format=xl_date_format) as writer:
                dataframe.to_excel(writer, index=False)
        else:
            logging.info("Writing into xl using default pandas settings")
            dataframe.to_excel(output_file_path_and_name, index=False)

    def create_csv_file_from_dataframe(self, dataframe, output_file_path_and_name, file_encoding='utf-8', delimiter_to_use=',', require_header=True, writing_mode='w'):
        """ Creates csv files and write into specified path as output
            INPUT:
                dataframe - pandas dataframe
                output_file_path_and_name - output file name with path
                require_header - specify if header required while writing into csv
            OUTPUT:
                generate csv file
        """
        dataframe.to_csv(output_file_path_and_name, encoding=file_encoding, sep=delimiter_to_use, index=False, header=require_header, mode=writing_mode)

    def select_and_rearrange_columns_from_dataframe(self, dataframe, columns_list_to_select_in_order):
        """ Select required columns in order from the dataframe
            INPUT:
                dataframe - pandas dataframe
                columns_list_to_select_in_order - list of columns in order
            OUTPUT:
                dataframe with only specified columns set in order
        """
        dataframe_after_reorder = dataframe[columns_list_to_select_in_order]
        return dataframe_after_reorder

    def drop_columns_from_dataframe(self, dataframe, columns_list_to_drop):
        """ Drop columns from dataframe
            INPUT:
                dataframe - pandas dataframe
                columns_list_to_drop - list of columns to drop from dataframe
            OUTPUT:
                dataframe after dropping the columns
        """
        # errors='ignore' -- drop columns only if column exist
        # inplace=True so that the original data can be modified without creating a copy
        # axis number 0 for rows and 1 for columns
        dataframe.drop(columns_list_to_drop, inplace=True, axis=1, errors='ignore')
        return dataframe

    def filter_value_from_dataframe(self, dataframe, column_name, value_to_filter):
        """ Filter column based on the value
            INPUT:
                dataframe - pandas dataframe
                column_name - column name in dataframe to filter
                value_to_filter - value to be filtered in dataframe
            OUTPUT:
                dataframe with only specified columns set in order
        """
        dataframe_after_filter = dataframe.loc[dataframe[column_name] == value_to_filter]
        return dataframe_after_filter

    def rename_columns_in_dataframe(self, dataframe, columns_name_dictionory):
        """ Function to rename columns in dataframe
            INPUT: columns_name_dictionory - dictionory with current column name as key and new column name as value
                    example: columns={"Existing column name": "New column name"}
            OUTPUT:
                dataframe after renaming columns
        """
        dataframe_after_renaming_columns = dataframe.rename(columns=columns_name_dictionory)
        return dataframe_after_renaming_columns

    def convert_date_format_in_dataframe(self, dataframe, column_name, date_format):
        """ Function to convert date format in dataframe
            INPUT: dataframe - pandas dataframe
                   column_name - column name for which date format shoul change
                   date_format - date format
            OUTPUT: pandas dataframe after changing the date format
        """
        # convert column values into datetime
        dataframe[column_name] = pd.to_datetime(dataframe[column_name])
        # convert date format
        dataframe[column_name] = dataframe[column_name].dt.strftime('%m/%d/%Y')
        return dataframe

    def insert_row_in_dataframe(self, dataframe, index, value, column_name=None):
        """ Insert row at specified Index in dataframe
            INPUT: dataframe - pandas dataframe
                   index - index value to insert row
                   value - value to insert as row in dataframe
                   column_name - column name to insert value
                                 If None then insert value for all columns
            OUTPUT: dataframe after inserting row
        """
        # If column name specified insert for only that column at specified location
        if column_name:
            dataframe.loc[index, column_name] = value
        else:
            # row insertion at specified index for all columns
            dataframe.loc[index] = value
        return dataframe

    def get_total_number_of_rows_and_columns_in_dataframe(self, dataframe):
        """ Get total number of rows in dataframe and return
            Input: dataframe - pandas dataframe
            Output: tuple with total number of rows and columns in dataframe
        """
        total_rows, total_columns = dataframe.shape
        return total_rows, total_columns

    def round_decimal_value_for_columns(self, dataframe, columns_list, number_of_decimal_digits=2):
        """ Rounds the decimal values of columns in a dataframe
            INPUT: dataframe - pandas dataframe
                   columns_list - list of columns to round off
                   number_of_decimal_digits - maximum decimal digits in converted value
            OUTPUT: dataframe after inserting row
        """
        for each in columns_list:
            dataframe[each] = dataframe[each].round(decimals=number_of_decimal_digits)

        return dataframe

    def select_rows_in_dataframe_using_index(self, dataframe, starting_index_value=None, ending_index_value=None):
        """ select rows in dataframe using index values
            INPUT:
                starting_index_value: index value that specifies start to read from that row
                ending_index_value: index value that specifies last row to be selected for dataframe
            OUTPUT: dataframe by selecting values using start and end index
        """
        dataframe = dataframe.iloc[starting_index_value:ending_index_value]
        return dataframe

    def get_columns_in_dataframe(self, dataframe):
        """ Find and return column names list in dataframe
            INPUT: dataframe - pandas dataframe
            OUTPUT: list of column names
        """
        return list(dataframe.columns.values)

    def specify_columns_for_dataframe(self, dataframe, column_name_list):
        """ Set column name to dataframe
            INPUT:
                dataframe - pandas dataframe
                column_name_list - column name list to be specified
            OUTPUT:
                full dataframe with columns
        """
        dataframe.columns = column_name_list
        return dataframe

    def append_to_datframe(self, dataframe, values_to_be_appended, ignore_index_values=True, sort_columns=False):
        """ Function to Append values to the dataframe
            INPUT:
                dataframe - pandas dataframe (main dataframe to which values will be appended)
                values_to_be_appended - dictionary or dataframe to be appended
            OUTPUT:
                dataframe after appending the values
        """
        dataframe_after_appending_values = dataframe.append(values_to_be_appended, ignore_index=ignore_index_values, sort=sort_columns)
        return dataframe_after_appending_values

    def find_sum_for_columns(self, dataframe, columns_list):
        """ Function to find sum for all the specified columns in list
            INPUT:
                dataframe - pandas dataframe
                columns_list - list of column names for which sum to be calculated
            OUTPUT:
                dict with column name and sum
        """
        column_name_with_sum_dict = {}
        for each_column_name in columns_list:
            sum_value_for_column = dataframe[each_column_name].sum()
            column_name_with_sum_dict[each_column_name] = sum_value_for_column
        return column_name_with_sum_dict

    def get_unique_values_list_from_dataframe(self, dataframe, column_name):
        """ Function to get Unique values in column from dataframe
            INPUT:
                dataframe - pandas dataframe
                column_name - column name to find unique values
            OUTPUT:
                return list of unique values in dataframe column
        """
        unique_values_list = dataframe[column_name].unique()
        return unique_values_list

    def convert_float_into_int_datatype(self, dataframe, columns_list_to_convert):
        """ Function to convert float into integer data type
            dataframe - pandas dataframe
            columns_list_to_convert - columns list to convert datatype(from float to integer)
        """
        for each_column in columns_list_to_convert:
            dataframe[each_column] = dataframe[each_column].astype(float, errors='ignore').astype('Int64', errors='ignore')
        return dataframe

    def convert_dictionry_into_dataframe(self, dictionary_data):
        """ Function to convert dictionary into pandas dataframe
            Input:
                dictionary_data - dictionary to convert into dataframe
            OUTPUT:
                return pandas dataframe
        """
        dataframe = pd.DataFrame(dictionary_data)
        return dataframe

    def get_dataframe_from_bytes(self, file_bytes, index_col_value=False):
        """ Create anf return dataframe from bytes
            INPUT:
                file_bytes - file bytes to be converted into dataframe
            OUTPUT:
               pandas dataframe
        """
        dataframe = pd.read_csv(file_bytes, index_col=index_col_value)
        return dataframe

    def csv_to_dataframe_with_date(self, file_name_with_path, number_of_header_rows_to_skip=0, footer_to_skip=0, parse_dates=False):
        """ To read csv into dataframe
            file_name_with_path - file path
            heders_to_skip - Number of header rows to skip
            footer_to_skip - Number of footer rows to skip
            parse_dates - Default value is False
                Available values:
                    1. list of numbers - list of column numberse to parse as date
                    2. list of names - list of column names to parse as date
                    3. dict - dict
                        e.g. {‘foo’ : [1, 3]} -> parse columns 1, 3 as date and call result ‘foo’
        """
        dataframe = pd.read_csv(file_name_with_path, skiprows=number_of_header_rows_to_skip, skipfooter=footer_to_skip, parse_dates=parse_dates)
        return dataframe

    def convert_dataframe_to_list_of_tuples(self, dataframe):
        """ Function to convert dataframe to list of tuples """
        return [tuple(x) for x in dataframe.values]
