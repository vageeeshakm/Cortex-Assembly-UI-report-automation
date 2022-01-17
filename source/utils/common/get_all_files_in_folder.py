import glob


class FileUtils:
    """ FILE UTILS class"""
    def get_all_files_in_folder(files_folder_path, file_format):
        """ Find and return all the files path of specified format in the input folder
            INPUT:
                files_folder_path: path to the folder that contains files
                file_format: file format to serach and get names
            OUTPUT:
                return list of files path of specified format in folder
        """
        all_files = glob.glob("{path}*{format}".format(path=files_folder_path, format=file_format))
        return all_files
