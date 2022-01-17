import openpyxl


class XlsUtils:
    """ XLS Utils class"""
    def load_file(self, file_path):
        """" Function to read xls file using openpyxl module
             file_path - full file path
        """
        self.xls_file = openpyxl.load_workbook(file_path)

    def load_sheet(self, sheet_name):
        """ Function to read sheet from xls file"""
        self.xls_sheet = self.xls_file[sheet_name]

    def append_rows(self, tuple_list):
        """ Append rows to existing sheet in xls """
        for each_row in tuple_list:
            self.xls_sheet.append(each_row)

    def apply_formula_for_column_from_row(self, column_number, from_row_number, formual):
        """ Function to apply formual to perticuler column
            Input:
                column_number - column number in xls sheet
                from_row_number - starting row number from where we have to apply formula
                formual - formula to be applied
        """
        for row_number, cell_obj in enumerate(list(self.xls_sheet.columns)[column_number]):
            if row_number >= from_row_number:
                cell_obj.value = formual.format(row_number=row_number + 1)

    def save_file(self, file_path):
        """ Function to save xls file"""
        self.xls_file.save(file_path)

    def get_number_of_rows_in_sheet(self, sheet_name):
        """ Get total number of rows in sheet """
        self.load_sheet(sheet_name)
        return self.xls_sheet.max_row

    def get_xls_sheet(self, sheet_name):
        """ Function to get xls sheet"""
        return self.xls_file[sheet_name]

    def get_xls_file(self, file_path):
        """ Function to get xls file"""
        return self.xls_file
