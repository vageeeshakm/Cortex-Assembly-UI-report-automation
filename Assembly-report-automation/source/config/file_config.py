from airflow.models import Variable


class FILETYPE:
    """FILETYPE CONSTANT CLASS"""
    CSV_FILE_TYPE = '.csv'
    EXCEL_FILE_TYPE = '.xlsx'
    ZIP_FILE_TYPE = '.zip'


class PATH:
    """ PATH CONSTANT CLASS """
    # Get data from airflow Variable if present else take this default value
    SOURCE_PATH = Variable.get("schema_path", "/application/source/")
    TEMP_DIRECTORY_PATH = '/downloads'
    SCHEMA_PATH = SOURCE_PATH + "file_schema/"
    REPORT_DOWNLOAD_PARAMS_PATH = SOURCE_PATH + "report_download_params_config/"
