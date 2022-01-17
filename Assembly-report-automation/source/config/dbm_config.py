from config.file_config import PATH as file_path


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "dbm/dbm.json"
    FTP_PARENT_DIRECTORY = '/DSP UI Reports - All Clients for Cortex - DBM - 118625/'
    FTP_PARENT_DIRECTORY_TO_UPLOAD_DBM_ASSEMBLY_REPORT = 'Last_14_days_DBM_Report_{current_date}/'
    FTP_PARENT_DIRECTORY_TO_UPLOAD_DBM_SOPHIE_REPORT = 'Sophie Advertiser/Last_14_days_Sophie_Report_{current_date}/'
    DBM_SCHEMA = file_path.SCHEMA_PATH + "dbm_schema.json"


class DATE:
    """ DURATION CONSTANT CLASS """
    DATE_FORMAT = "%Y%m%d"


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    NUMBER_OF_FOOTER_LINES_TO_SKIP = 2
    NUMBER_OF_FOOTER_LINES_TO_SKIP_FOR_VALIDATION = 15
    COLUMN_NAMES_LIST_TO_INSERT = ['Partner', 'Partner ID']
    ASSEMBLY_PARTNER_NAME_ID_MAPPING_DICT = {
        "Assembly Media": "213",
        "E-Trade MDC Media Partners": "1135738"
    }
    SOPHIE_PARTNER_NAME_ID_MAPPING_DICT = {
        "Varick Media Management": "31",
        "Assembly Media": "213"
    }
    REPORT_COLUMN_NAME_TO_ADD_FILTER_DETAILS = 'Advertiser ID'
    REPORT_FILTERS_LIST = ['Advertiser ID', 'Partner ID']
    REPORT_DATE_COLUMN_NAME = 'Date'
    REPORT_DATE_FORMAT = '%m/%d/%Y'
    COLUMNS_TO_CONVERT_INTO_INT = ['Line Item ID', 'Campaign ID', 'Creative ID', 'Impressions', 'Clicks']
    NUMBER_OF_FOOTER_LINES_TO_SKIP_FOR_DATA_VALIATION = 15


class ADVERTISERDETAILS:
    """ ADVERTISERDETAILS CONSTANT CLASS"""
    ASSEMBLY_ADVERTISER_TYPE = "DBM_Assembly"
    SOPHIE_ADVERTISER_TYPE = "DBM_Sophie"
