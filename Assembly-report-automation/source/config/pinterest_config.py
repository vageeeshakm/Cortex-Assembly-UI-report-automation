from config.file_config import PATH as file_path


# class URL:
#     """URL CONSTANT CLASS"""
#     LOGIN = "https://analytics.pinterest.com/login/"
#     LOG_OUT = 'https://ads.pinterest.com/logout/'
#     ADS_REPORT = "https://ads.pinterest.com/advertiser/{account_id}/report-center/builder/"


class BUTTON:
    """Button element CONSTANT CLASS"""
    # CLEAR_XPATH = "//div[@class='Jea XiG gjz zI7 iyn Hsu']//button[contains(@class,'StA')]"
    # DOWNLOAD_XPATH = "//div[text()='Run report']"
    # SIGNUP_CLASS_NAME = 'red.SignupButton.active'
    # PIN_PROMOTION_ID = 'PIN_PROMOTION'
    # REPORT_DOWNLOAD_BUTTON_XPATH = '//button[@aria-label="Download"]'
    DOWNLOAD_BUTTON_XPATH = "//div[text()='Download scheduled report']//parent::button"


# class INPUT:
#     """Input elements CONSTANT CLASS"""
#     INPUT_COLUMN_XPATH = "//input[contains(@id,'{column_name}')]"
#     EMAIL_ID = "email"
#     PASSWORD_ID = 'password'
#     START_DATE_ID = 'startDate'
#     END_DATE_ID = 'endDate'


# class LABEL:
#     """LABEL Element CONSTANT CLASS"""
#     REPORT_ID = 'reportName'
#     COLUMNS_PATH = 'arguments[0].click();'
#     REPORT_BUILDER_PATH_ID = 'reportBuilderColumnPickerSearch'
#     DOWNLOAD_STATUS_XPATH = "//div[@class='gestalt-table']/table/thead//div[contains(text(),'Report details')]/ancestor::thead//following-sibling::tbody//div[text()='{report_name}'][1]/ancestor::td/following-sibling::td//div[contains(text(),'Completed')]"
#     REMOVE_COLUMNS_XPATH = '//button[@aria-label="icon for removing column from column viewer"]'
#     ACCORDION_SECTION_XPATH = "//div[@class='Jea KO4 jzS zI7 iyn Hsu']//div[@role='button']"


class PATH:
    """ PATH CONSTANT CLASS """
    # JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "pinterest/pinterest.json"
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "pinterest/pinterest_email.json"
    FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES = "/Social UI Reports - All Clients for Cortex - Pinterest/"
    PINTEREST_SCHEMA = file_path.SCHEMA_PATH + "pinterest_schema.json"


# class AWS_SECRET_MANGER:
#     """ AWS_SECRET_MANGER CONSTANT CLASS """
#     PINTEREST_SECRET_NAME = '/rep_auto_pinterest/assembly/pinterest'


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    OUTPUT_FILE_NAME = "{}_{}_Pinterest_UI_{}.csv"
    COLUMNS_LIST_TO_DROP = ['Ad entity status', 'Organic pin ID']
    # Rename columns in dictionory
    COLUMNS_TO_RENAME_DICT = {
        'Earned Pin clicks': 'Earned link clicks',
        'Paid Pin clicks': 'Paid link clicks',
        'Pin clicks': 'Link clicks'
    }
    COLUMNS_TO_SELECT_IN_ORDER = ['Ad group name', 'Ad group', 'Ad ID', 'Ad name',
                                  'Campaign ID', 'Campaign name', 'Date', 'Earned impressions', 'Earned link clicks',
                                  'Earned saves', 'Impressions', 'Link clicks', 'Paid impressions', 'Paid link clicks',
                                  'Paid saves', 'Paid video played at 100%', 'Paid video played at 25%',
                                  'Paid video played at 75%', 'Paid video played at 95%', 'Spend in account currency']
    DATE_FORMAT_IN_REPORT = '%m/%d/%Y'


class DATE:
    """ DURATION CONSTANT CLASS """
    YMD_DATE_FORMAT = "%Y%m%d"


class TEXT:
    """ Text constant class"""
    STRING_TO_SPLIT = "To view this content open the following URL in your browser: "
    EMAIL_SUBJECT = "Your scheduled report"
    DOWNLAOD_TEXT = "Download "
    REPORT_TEXT = " report"
