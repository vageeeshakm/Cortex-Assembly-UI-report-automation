import os
import io
import boto3
import json
import logging

from airflow.models import Variable

from config.aws_config import AWS
from config.file_config import FILETYPE
from utils.common.aws_secret_manager_utils import AwsSecretManager
from utils.common.pandas_utils import PandasUtils
from utils.common.handle_temporory_directory import HandleTempDirectory


class AwsS3Utils():
    def __init__(self):
        """ AwsS3Utils initialisation.
            gets the required credentials from aws secrets manager and creates a s3 client
            Raise:
                connection error : Raises error if error in connection
        """
        aws_secret_manager_object = AwsSecretManager()

        aws_secret_name = Variable.get("aws_secret_name", None)

        # If aws secret name is present then use that else use the default one from config
        if aws_secret_name:
            logging.info('Using Aws secret name provided in Variable')
            aws_secret_json = json.loads(aws_secret_manager_object.get_secret(aws_secret_name))
        else:
            logging.info('Using Default Aws secret name from config')
            aws_secret_json = json.loads(aws_secret_manager_object.get_secret(AWS.AWS_ACCESS_KEY_SECRET_NAME))

        aws_access_key = aws_secret_json.get('aws_access_key')
        aws_key_secret = aws_secret_json.get('aws_key_secret')
        aws_region_name = aws_secret_json.get('aws_region_name')

        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_key_secret,
                region_name=aws_region_name
            )
        except Exception as connection_error:
            logging.error('Error occured while initialising client {}'. format(connection_error))
            raise connection_error

    def s3_upload(self, local_file_path, bucket_name, s3_file_path):
        """uploads a file to s3"""
        logging.info("Uploading file to s3 path - " + bucket_name + '/' + s3_file_path)
        self.s3_client.upload_file(local_file_path, bucket_name, s3_file_path)

    def dataframe_to_s3(self, dataframe, bucket_name, s3_file_path, file_type=FILETYPE.CSV_FILE_TYPE):
        """converts a dataframe to csv and upload to s3 """
        temp_folder_object = HandleTempDirectory()
        local_temp_file = temp_folder_object.generate_files()[0].name
        if file_type == FILETYPE.CSV_FILE_TYPE:
            pandas_utils_object = PandasUtils()
            pandas_utils_object.create_csv_file_from_dataframe(dataframe, local_temp_file)
            self.s3_upload(local_temp_file, bucket_name, s3_file_path)
        temp_folder_object.clean_up()

    def get_bytes_from_s3_file(self, bucket_name, s3_file_path):
        """ Read file and return bytes"""
        try:
            s3_obj = self.s3_client.get_object(Bucket=bucket_name, Key=s3_file_path)
            file_bytes = io.BytesIO(s3_obj['Body'].read())
            return file_bytes
        except self.s3_client.exceptions.NoSuchKey:
            logging.error("No such key in this bucket")
            return None
        except Exception as e:
            logging.error('Error occured while getting file contents {}'. format(e))
            raise e

    def s3_download(self, local_file_path, bucket_name, s3_file_path):
        """downloads a file from s3"""
        logging.info("Downloading file from s3 path - " + bucket_name + '/' + s3_file_path)
        logging.info("downloading report {}".format(os.path.basename(s3_file_path)))
        self.s3_client.download_file(bucket_name, s3_file_path, local_file_path + os.path.basename(s3_file_path))

    def s3_list_files(self, bucket_name, s3_path):
        """returns the list of files in s3 path"""
        files = []
        logging.info("Getting all files from s3 path - " + bucket_name + '/' + s3_path)
        paginator = self.s3_client.get_paginator('list_objects')
        page_response = paginator.paginate(Bucket=bucket_name, Prefix=s3_path)

        # page_response Holds 1000 objects at a time and will continue to repeat in chunks of 1000.
        for pageobject in page_response:
            for file_obj in pageobject.get("Contents"):
                files.append(file_obj)

        return files

    def download_all_files(self, bucket_name, s3_path, download_folder):
        """downloads all files from a s3 location"""
        files = self.s3_list_files(bucket_name, s3_path)
        if files:
            for each_s3_file in files:
                logging.info("Downloading from " + str(each_s3_file.get('Key')))
                self.s3_download(download_folder, bucket_name, each_s3_file.get('Key'))

        logging.info("All files downloaded")

    def delete_all_files(self, bucket_name, s3_path):
        """deletes all files from a S3 folder """

        files = self.s3_list_files(bucket_name, s3_path)
        objects = []

        if files:
            for each in files:
                objects.append({'Key': each['Key']})

            objects_to_delete = {'Objects': objects}

            logging.info("Deleting all files from - " + bucket_name + '/' + s3_path)
            self.s3_client.delete_objects(Bucket=bucket_name, Delete=objects_to_delete)
