import os


def replace_function(input_string, replace_charecter, value_to_replace):
    """ Function to replace the specified charecter in string with input value to replace
        Output:
            string after replacing the charecter with the specified input charecter
    """
    return input_string.replace(replace_charecter, value_to_replace)


def split_function(input_string, split_charecter):
    """ Function will split the input string based on split_charecter
        INPUT:
            input_string : string value
            split_charecter: substring to split
        OUTPUT:
            list after spitting the given string
    """
    return input_string.split(split_charecter)


def extract_extension_from_filename(full_file_name_with_extension):
    """ Function will split full_file_name_with_extension to get file name and extension
        INPUT:
            full_file_name_with_extension - full name of file along with extension
        OUTPUT:
            filename and extension of file
    """
    return os.path.splitext(full_file_name_with_extension)
