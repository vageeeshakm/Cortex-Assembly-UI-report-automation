import tempfile
import os
import shutil

from config.file_config import PATH

class HandleTempDirectory:
    """Class to handle temprory directory"""
    def __init__(self):

        # the dirs names list that are created
        self.dirs_created = []
        # the files names list that are created
        self.files_created = []

    def generate_dirs(self, directory=PATH.TEMP_DIRECTORY_PATH, number_of_dirs_to_create=1):
        """To generate the temp dirs.
            INPUT:
            directory - directory to create temporory files
            number_of_dirs_to_create - number of directories to be created
        """
        for each in range(number_of_dirs_to_create):
            local_temp_file = tempfile.NamedTemporaryFile(dir=directory, delete=False)
            local_temp_file.close()
            os.remove(local_temp_file.name)
            os.makedirs(local_temp_file.name)
            self.dirs_created.append(local_temp_file.name + os.sep)
        return self.dirs_created

    def remove_dirs(self):
        """To delete the generated dirs."""
        list_to_update = self.dirs_created.copy()
        for each in list_to_update:
            shutil.rmtree(each, ignore_errors=True)
            self.dirs_created.remove(each)

    def generate_files(self, number_of_files=1):
        """To generate the temp files."""
        self.number_of_files = number_of_files
        for each in range(self.number_of_files):
            local_temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='UTF-8', delete=False)
            self.files_created.append(local_temp_file)
        return self.files_created

    def remove_files(self):
        """To delete the generated dirs."""
        list_to_update = self.files_created.copy()
        for each in list_to_update:
            each.close()
            self.files_created.remove(each)
            os.remove(each.name)

    def clean_up(self):
        """To delete temp dirs and files."""
        self.remove_dirs()
        self.remove_files()
