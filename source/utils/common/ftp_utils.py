import pysftp
import logging

class FtpUtils:

    def __init__(self, host, user, password, port=22):
        """
        INPUT:
            host - ftp host
            user- ftp password
            password - ftp user
            port - ftp port
        """
        self.port = port
        self.host = host
        self.user = user
        self.password = password

        self.cnopts = pysftp.CnOpts()
        self.cnopts.compression = True
        self.cnopts.hostkeys = None

    def get(self, remote_filename, local_filename, show_progress=True):
        """ Download the file stored on ftp server
            INPUT:
                remote_filename - full path to remote file
                local_filename - full path to local file, where file should stored
                show_progress - shows download progress
            OUTPUT:
                download the file into local
        """
        with pysftp.Connection(self.host, username=self.user, password=self.password, cnopts=self.cnopts) as sftp:
            fo = open(local_filename, 'wb')
            if show_progress:
                sftp.getfo(remotepath=remote_filename, flo=fo, callback=self.get_status)
            else:
                sftp.getfo(remotepath=remote_filename, flo=fo)

            fo.close()

    def put(self, local_filename, remote_filename):
        """ Upload the file into ftp server
        INPUT:
            local_filename - full path to loacl file that to be uploaded
            remote_filename - full remote path to upload the file
        OUTPUT:
            uploads the file into ftp server
        """
        with pysftp.Connection(self.host, username=self.user, password=self.password, cnopts=self.cnopts) as sftp:
            sftp.put(local_filename, remote_filename)

    def check_directory_exists(self, remote_path):
        """ check if directory exists on ftp server
            INPUT:
                remote path: path to the directory
            OUTPUT: Boolean
                True -> if directory exist
                False -> if directory not exist
        """
        with pysftp.Connection(self.host, username=self.user, password=self.password, cnopts=self.cnopts) as sftp:
            try:
                sftp.chdir(remotepath=remote_path)
                return True
            except IOError:
                return False

    def create_directory(self, remote_path):
        """ Create directory on ftp server
            INPUT:
                remote_path - path to create directory
            OUTPUT:
                creates directory on ftp
        """
        with pysftp.Connection(self.host, username=self.user, password=self.password, cnopts=self.cnopts) as sftp:
            sftp.mkdir(remotepath=remote_path, mode=777)
        sftp.close()

    def list(self, remote_path):
        """ List files and directory on ftp
            INPUT:
                remote_path - path to create directory
            OUTPUT:
                list of files and directory : [list]
        """
        with pysftp.Connection(self.host, username=self.user, password=self.password, cnopts=self.cnopts) as sftp:
            return sftp.listdir(remotepath=remote_path)

    def create_nested_directory(self, full_remote_path):
        """ creates nested directory on ftp
            INPUT:
                full_remote_path: full path seperated by '/'
            OUTPUT:
                creates all the directory only if not exist

        """
        try:
            directory_list = full_remote_path.split('/')
            directory_to_create = '/'
            for each_directory in directory_list:
                if each_directory != '':
                    directory_to_create = directory_to_create + each_directory + '/'
                    directory_exist_status = self.check_directory_exists(directory_to_create)
                    if not directory_exist_status:
                        logging.info("Creating Directory on FTP")
                        self.create_directory(directory_to_create)
                    else:
                        logging.info("Directory already exists on FTP")
        except Exception as e:
            logging.error("Error {} while creating the Directory".format(e))
            raise Exception(e)
