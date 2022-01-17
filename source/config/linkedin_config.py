from config.file_config import PATH as file_path


class URL:
    """URL CONSTANT CLASS"""
    LOGINPAGE_LOGIN = "https://www.linkedin.com/campaignmanager/accounts"
    ACCOUNT_PAGE_CAMPAIGN_MANAGER = 'https://www.linkedin.com/campaignmanager/accounts/{account_id}/campaign-groups'


class BUTTON:
    """Button element CONSTANT CLASS"""
    CLEAR_XPATH = ""
    ACCOUNT_PAGE_ADS_XPATH = '//*[text()="Ads"]'
    LOGOUT_PAGE_LOGOUT_XPATH = "//span[contains(text(), 'Sign Out')]"


class INPUT:
    """Input elements CONSTANT CLASS"""
    LOGIN_PAGE_USERNAME_ID = "username"
    LOGIN_PAGE_PASSWORD_ID = "password"


class LABEL:
    """LABEL Element CONSTANT CLASS"""
    LOGIN_PAGE_IFRAME_CLASS = "iframe--authentication"
    ACCOUNT_PAGE_GENERATE_REPORT_XPATH = "//span[text()='Export']"
    ACCOUNT_PAGE_ADS_BREAKDOWN_XPATH = "//label[@for='timeframe-DAILY']"
    ACCOUNT_PAGE_EXPORT_REPORT_XPATH = '//button[text()="Export"]'
    ACCOUNT_PAGE_REPORT_TYPE_XPATH = "//select[@id='report-type']"
    ACCOUNT_PAGE_NO_RECORDS_XPATH = "//td[text()='No records to show']"
    ACCOUNT_PAGE_DATE_SELECTOR_ID = "date-range-control"
    ACCOUNT_PAGE_START_DATE_XPATH = '//input[@name="startName"]'
    ACCOUNT_PAGE_END_DATE_XPATH = '//input[@name="endName"]'
    ACCOUNT_PAGE_UPDATE_DATE_XPATH = '//button[@aria-label="Update"]'
    LOGOUT_PAGE_LOGOUT_HEADER_XPATH = "//*[@class='global-header-dropdown__dropdown-trigger']//parent::button"


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "linkedin/linkedin_daily.json"
    WEEKLY_REPORT_JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "linkedin/linkedin_weekly.json"
    FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES = '/Social UI Reports - All Clients for Cortex - LinkedIn - 118257/'
    LINKEDIN_WEEKLY_SCHEMA = file_path.SCHEMA_PATH + "linkedin_weekly_schema.json"


class AWS_SECRET_MANAGER:
    """ AWS_SECRET_MANGER CONSTANT CLASS """
    SECRET_NAME = '/rep_auto_linkedin/assembly/linkedin'


class FILENAME:
    """ PANDASSETTINGS CONSTANT CLASS """
    AD_PERFORMANCE_FILE_SUFFIX = '_creative_performance_report.csv'
    CONVERSION_AD_PERFORMANCE_FILE_SUFFIX = '_creative_conversion_performance_report.csv'
    STANDARD_REPORT_DOWNLOADED_FILE_NAME = 'account_{account_id}' + AD_PERFORMANCE_FILE_SUFFIX
    CONVERSION_REPORT_DOWNLOADED_FILE_NAME = 'account_{account_id}' + CONVERSION_AD_PERFORMANCE_FILE_SUFFIX
    STANDARD_REPORT_OUTPUT_FILE_NAME = 'Standard_{account_id}_{today_date}.csv'
    CONVERSION_REPORT_OUTPUT_FILE_NAME = 'Conversion_{account_id}_{today_date}.csv'
    WEEKLY_REPORT_OUTPUT_FILE_NAME = 'LinkedIn Last 14 Days Weekly Report_{current_date}'


class PLATFORMTYPE:
    """ PLATFORMTYPE CONSTANT CLASS """
    LINKEDINDAILY = "linkedin_daily"
    LINKEDINWEEKLY = "linkedin_weekly"


class DATE:
    """ DURATION CONSTANT CLASS """
    YMD_DATE_FORMAT = "%Y%m%d"
    WEEKLY_REPORT_MDY_DATE_FORMAT = "%m/%d/%Y"
    WEEKLY_REPORT_OUTPUT_FILE_DATE_FORMAT = "%Y.%m.%d"


class REPORTTYPE:
    """ REPORTTYPE CONSTANT CLASS """
    ADPERFORMANCE = 'Ad Performance'
    CONVERSIONPERFORMANCE = 'Conversion ad performance'


class EMAIL:
    """ EMAIL CONSTANT CLASS """
    LINKEDIN_WEEKLY_REPORT_MESSAGE_BODY = 'Hi,|Please find attached Last 14 days LinkedIn weekly report| Thank you,| Report Automation'
    LINKEDIN_WEEKLY_REPORT_MESSAGE_SUBJECT = 'Last 14 days LinkedIn weekly report'


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    DAILY_REPORT_NUMBER_OF_ROWS_TO_SKIP = 3
    WEEKLY_REPORT_NUMBER_OF_ROWS_TO_SKIP = 4
    DELIMITER_TO_READ_CSV = '\t'
    CSV_FILE_ENCODING_TYPE = 'utf-16'
    WEEKLY_REPORT_INSERT_COLUMN_INDEX = 1
    WEEKLY_REPORT_COLUMN_NAME_TO_INSERT = 'Account Id'
    # Key in the dict specifies existing column name and value will be the column name after renaming the existing column
    WEEKLY_REPORT_RENAME_COLUMNS = {"Ad ID": "Creative ID"}
    WEEKLY_REPORTS_COLUMNS_TO_SELECT = [
        'Start Date (in UTC)',
        'Account Name',
        'Account Id',
        'Creative Name',
        'Creative ID',
        'Campaign Name',
        'Campaign ID',
        'Impressions',
        'Clicks',
        'Total Spent',
        'Total Social Actions',
        'Total Engagements',
        'Conversions',
        'Video Views',
        'Video Completions'
    ]
