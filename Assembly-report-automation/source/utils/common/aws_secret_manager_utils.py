""" AWS secret manager to store retrieve and delete secret """
import boto3
import base64
import logging
from config.aws_config import AWS
from botocore.exceptions import ClientError


class AwsSecretManager():
    def __init__(self):
        """ AwsSecretManager initialisation.
            initialise the client by using two parameter (resource, region)

            Raise:
                connection error : Raises error if error in connection or AWS_REGION not found in settings file
        """
        try:
            self.client = boto3.client('secretsmanager', region_name=AWS.AWS_REGION)
        except Exception as connection_error:
            logging.error('Error occuered while initialising client {}'. format(connection_error))
            raise connection_error

    def create_secret(self, name, secret_string):
        """
            This Function is used to create a new secret in aws secret manager
            Arguments: name , secret_string
        name :
            Data Type is string.
            It is name of secret. It is unique
        secret_string :
            Data Type is dictionory.
            Value to be stored in the secret.
        example:
            create_secret('secret_name',{'value'})
        """
        try:
            result = self.client.create_secret(
                Name=name,
                SecretString=secret_string)
            return result
        except Exception as aws_secret_manager_error:
            logging.error('Error occuered while creating the token {}'. format(aws_secret_manager_error))
            raise aws_secret_manager_error

    def delete_secret(self, **kwargs):
        """
            Function will delete the secret from the aws secret manager using  secret name
            Arguments: name {dict} : It is the name of the secret to be deleted
            Returns: True if deleted or False
            ForceDeleteWithoutRecovery is True, which means you can't recover this secret after delete
            Raise:
                delete_secret_error if secret name is not found .
        """
        try:
            self.client.delete_secret(
                SecretId=kwargs.get('name'),
                ForceDeleteWithoutRecovery=True)
            return True
        except Exception as delete_secret_error:
            logging.error('Error occuered while deleting the token {}'. format(delete_secret_error))
            raise delete_secret_error

    def get_secret(self, secret_name):
        """
            Function will retrieve the secret from the aws secret manager using  secret name
            Arguments: name {dict} : It is the name of the secret
            Returns: secret value associated with secret name
            Raise:
                client_error if secret name is not found.
        """
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as client_error:
            logging.error('Error occuered while fetching the token {}'. format(client_error))
            raise client_error
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret

    def update_secret(self, **kwargs):
        try:
            self.client.update_secret(SecretId=kwargs.get('secret_name'), SecretString=kwargs.get('update_values_json'))
        except ClientError as update_err:
            logging.error('Error occured while updating secret value: {err}'.format(err=update_err))
