import logging
import json
from boxsdk import JWTAuth, Client

from config.aws_config import AWS
from config.box_config import BOX
from utils.common.handle_temporory_directory import HandleTempDirectory

# Set logger to critical so that not to print any logs related box credentials
logging.getLogger('boxsdk').setLevel(logging.CRITICAL)


class BOXUtility:

    def __init__(self):

        # aws_secret_manager_object = AwsSecretManager()
        # aws_secret_json = json.loads(aws_secret_manager_object.get_secret(BOX.SECRET_NAME))

        # self.client_id = aws_secret_json.get('BOX_APP_Client_ID')
        # self.client_secret = aws_secret_json.get('BOX_APP_Client_Secret')
        # self.public_key_id = aws_secret_json.get('BOX_APP_Public_Key_ID')
        # self.private_key = aws_secret_json.get('BOX_APP_Private_Key')
        # self.pass_phrase = aws_secret_json.get('BOX_APP_Pass_Phrase')
        # self.enterprise_id = aws_secret_json.get('BOX_APP_Enterprise_ID')

        self.client_id = '4tiv27bhnvxsj6w2b4e5i2p6howni4ne'
        self.client_secret = '10haViHAijcU8W8ONP828u3jDfoSo7NR'
        self.pass_phrase = 'f444a225c0e58da876eb141b3788f3cd'
        self.enterprise_id = '374368'
        self.public_key_id = 'rypu9zfg'
        self.private_key = '-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFDjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIk6gT124RotACAggA\nMBQGCCqGSIb3DQMHBAhgU5sBxbp69ASCBMgYMQrIYCL5zdKwjFDLGA4MC5iD+Kn6\nlqg2WmApSTkw2vmdKH7rddBkBnzm3ocrU4SzyhglN5kfZgm+vClyCm+aUPlJYysy\nhXInid5SkTikQVdr+p+Wkzo4TE9cWlbqjQr52mGCIfEBKyeHzWMKcb7xGhY3oqjX\nbzY9ubpxvGytcMO2EENNn8LzfTcfzAO2zb0N4abu/Sie+RhTJgo10I0Q2xBfTwg1\nTrF6aoaWr2J+SPAIDBKZcKzIo++A1mP7+pdyucxDR/YhIHNA4dv4eZDVTLQPxs5F\nFcpFGy0q8fo4Z/l2sAJUP+1CTvF3BkLCzUY1+4s6RTda2iNd1ePJr6hqyHzMwUrF\nUIz4rIln7VxcWs7cdKBxGx0RzixOaIKf9MaQXCQyh6HB3Vfxga3qm4QrbDvK7Jc7\n0ZKGxpygwMVotJcv/nOtnlsBgP5W7I0PZO6hk/L0HNNQ8hkFDLlW/2Sklsqb4UfT\ndisVaiz4SwcELKlBYoZ+2iJ2iuF47F1l+sKjjtxtJnhQYvq6qjAD2sBd4xZA9/vI\nhnVX/BvY3EWlmq1kRJHt+A1T94uaiGZslQkQgIgIKIq+dHiBPyp8nPr5MLLuky0G\n+X3RaRZjP3VfN1p8pFlvRfpPPQ2fLlAZP+aMPjk+B7xeNpAtMuAbInsTyfF/1tW3\neqn9rY5CBLtFulNd66LQDQHUyNiOaZI3V+jlimUWs1i9g3050RwhCuLnFMpWD0KQ\nqm9f2/FPl4lYhwu5fNkJEi6yQ0a3lbHIvCXG059qINNRZtN9sXWZcM5DqsnbBtHw\n8OUDg6A4q5eWF02fMOlA5BPnNKoy2nitF/SMGYrOrkVCaQe85z5jg8RWxWYg+sdE\nFq9iR20p3y6j4oH5mVKaMOS7b6Os3/sGnh9ScQsSFUYPyJotnytnIj80FYAd6vVl\nb/giTPx3tIho16VDzlgxi4+ttYl++vsuIofEK6ZlboRwL33PcayvrbxTDBSqvqWp\nbRUaUSrupJJVy7bpi17050h2d1bJ4TPtY3p6YIBsZQsF4tJQVaJD5XroQnkNCUO/\nLRx9JEwPsWQ2QOnWZn4uV16tHMDChKaX1pX8sDSS1HciY4K0nmVgaXq8DwBLNHsN\nf5A/SnroHlmxHslwOj5N1NKJg8kbJ1OkusNX+WmOEt4E+aw5OMYK8/ikZhPFnXxs\nBmlusK2RDRemuvISlBBMQzp6pqQ1UDLnM/BBOkhsZI1WAxRdzDYdzIfEA5pMVZzD\nDst8QMauDslcIW8scaP4zx6rxhPh0oZ46BxiGDl+hgY61uIch6pSamBMKgQz1j9a\nc+pvzFL0SJaT+52Kc2rvR3maRPsbgUvNPI9kvTYCy/Hm1NxzNIRPyBTpSAUF41Kw\nBhdyRqV9gH5S7nu3aO0UHl63xrUeeFAC5S6xeZKXJWmBbzKIlDN37933q0tzCHva\nIYo6MRgdivqEKYm6D6U1ysQObAJBTColgYVNL8t7BSAoCUaNn9NuOcPdUTZSd9Es\n//607Fd377L5+QWF4UyOY0tXUoxFpX78uJRLyBc1o7jv/5RInIXsM4AMAfuJGcVL\nrXDRn4MLXrtWWX1KdPNp/8PpJ1STm/chSsIcUHRvHS50EbIlq+E2n6Z/35xd6liN\nRFw=\n-----END ENCRYPTED PRIVATE KEY-----\n'

        # create temporary file
        temp_folder_object = HandleTempDirectory()
        local_temp_files = temp_folder_object.generate_files()
        local_token_file = local_temp_files[0].name

        # add credentails to the temp file
        self.add_jwt_tokens_to_file(local_token_file)

        try:
            # create box client object
            sdk = JWTAuth.from_settings_file(local_token_file)
            self.box_client = Client(sdk)

        except Exception as e:
            logging.error(f"Error occurred while box authorization - {str(e)}")

        temp_folder_object.clean_up()

    def add_jwt_tokens_to_file(self, file):
        """ Add the BOX tokens to  the file"""

        token_json = {
            "boxAppSettings": {
                "clientID": self.client_id,
                "clientSecret": self.client_secret,
                "appAuth": {
                    "publicKeyID": self.public_key_id,
                    "privateKey": self.private_key.replace('\\n', '\n'),
                    "passphrase": self.pass_phrase
                },
            },
            "enterpriseID": self.enterprise_id
        }

        with open(file, 'w') as token_file:
            json.dump(token_json, token_file)

    def get_contents_inside_folder(self, folder_path_list):
        """ Get the contents inside a folder"""

        # get the contents of the root folders
        root_folder = self.box_client.root_folder().get()
        items = root_folder.get_items()

        # iterate through all parent folders to reach the final folder
        for each_folder in folder_path_list:
            folder_found = False
            for each_item in items:
                if each_item.name == each_folder:
                    # get the contents of subfolder
                    items = self.box_client.folder(folder_id=each_item.id).get_items()
                    folder_found = True
                    break

            if not folder_found:
                logging.error(f"Folder not found - {str(each_folder)}")
                return None

        return items

    def check_if_item_exists(self, item_full_path):
        """Checks if the file or folders exists in the Box

            item_full_path - Full path of File or Folder
        """
        folder_breakdown = item_full_path.split('/')
        parent_folders = folder_breakdown[0:-1]
        item_name = folder_breakdown[-1]

        # get contents inside parent folder
        contents = self.get_contents_inside_folder(parent_folders)

        if contents:
            for each in contents:
                if each.name == item_name:
                    item_id = each.id
                    return item_id

        return None

    def download_file(self, full_file_path, local_file_name):
        """ Download a file from Box

            full_file_path - Full path of file to be downloaded separated using '/'
            local_file_name - file name in the local system where file needs to be downloaded
        """

        file_id = self.check_if_item_exists(full_file_path)

        if file_id:
            with open(local_file_name, 'wb') as open_file:
                self.box_client.file(file_id).download_to(open_file)
                logging.info(f"File is downloaded to {str(local_file_name)}")
                return True
        else:
            logging.error(f"File not found in {full_file_path}")
            raise Exception("File not found")

    def upload_file(self, folder_path, local_file_name):
        """ Upload a file to Box

            folder_path - Full path of parent folder where file needs to be uploaded separated using '/'
            local_file_name - file name in the local system which needs to be uploaded
        """

        folder_id = self.check_if_item_exists(folder_path)

        if folder_id:
            new_file = self.box_client.folder(folder_id).upload(local_file_name)
            logging.info("File is uploaded successfully")
            return True
        else:
            logging.error(f"Folder not found in {folder_path}")
            return False
