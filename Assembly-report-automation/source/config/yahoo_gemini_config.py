from config.file_config import PATH as file_path


class URL:
    """URL CONSTANT CLASS"""
    LOGINPAGE_LOGIN = 'https://gemini.yahoo.com/advertiser/reporting'


class BUTTON:
    """Button element CONSTANT CLASS"""
    REPORTING_PAGE_NEW_REPORT_XPATH = "//*[@class='backgrounder']//button[contains(text(), 'New report')]"
    DOWNLOAD_REPORT_PAGE_LOGOUT_BUTTON_XPATH = "//*[@id='application-header-right-nav']//a[contains(text(), 'Log out')]"


class INPUT:
    """Input elements CONSTANT CLASS"""
    LOGINPAGE_USERNAME_ID = "login-username"
    LOGIN_PAGE_PASSWORD_ID = "login-passwd"
    REPORTING_PAGE_SEARCH_ACCOUNT_XPATH = "//input[@id='searchtext']"


class LABEL:
    """LABEL Element CONSTANT CLASS"""
    REPORTING_PAGE_AD_GROUP_PERFORMANCE_XPATH = "//*[@class='backgrounder']//span[contains(text(), '{}')]"
    REPORTING_PAGE_SELECT_ALL_REPORTS_XPATH = "//label[@for='check_all']"
    REPORTING_PAGE_SELECT_DATE_XPATH = "//*[@class='date-range-selector']//child::div"
    REPORTING_PAGE_LAST_14_DAYS_DATE_RANGE_XPATH = "//*[@class='date-range']//li[contains(text(), '{}')]"
    REPORTING_PAGE_COLUMN_ID = "campaign-columns-button"
    REPORTING_PAGE_LAST_14_DAYS_COLUMN_XPATH = "//*[@id='campaign-columns-button']/following-sibling::ul//span[contains(text(), '{}')]"
    REPORTING_PAGE_CREATE_REPORT_XPATH = "//*[@id='new-report-actions']//button[contains(text(), 'Create report')]"
    DOWNLOAD_REPORT_PAGE_RECENT_REPORT_XPATH = "//*[@class='directive-table']//tbody//td[@column-id='token']"
    DOWNLOAD_REPORT_PAGE_READY_STATUS_XPATH = DOWNLOAD_REPORT_PAGE_RECENT_REPORT_XPATH + "//span[text()='Ready']"
    DOWNLOAD_REPORT_PAGE_PROCESSING_STATUS_XPATH = DOWNLOAD_REPORT_PAGE_RECENT_REPORT_XPATH + "//span[text()='Processing']"
    DOWNLOAD_REPORT_PAGE_DOWNLOAD_REPORT_XPATH = DOWNLOAD_REPORT_PAGE_RECENT_REPORT_XPATH + "//child::a"
    DOWNLOAD_REPORT_PAGE_LOGOUT_HEADER_XPATH = "//*[@id='application-header-right-nav']//child::li"


class PATH:
    """ PATH CONSTANT CLASS """
    YAHOO_VERIZON_JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "yahoo_gemini/yahoo_gemini_verizon.json"
    YAHOO_MAPPING_JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "yahoo_gemini/yahoo_gemini_mapping.json"
    YAHOO_ETRADE_WEEKLY_JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "yahoo_gemini/yahoo_gemini_etrade_weekly.json"
    FTP_PARENT_DIRECTORY_TO_YAHOO_VERIZON_FILES = "/DSP UI Reports - All Clients for Cortex - Yahoo Gemini_118691/Last_14_days_YahooGemini_Report_{}/"
    FTP_PARENT_DIRECTORY_TO_YAHOO_MAPPING_FILES = "/Yahoo Gemini Mapping Report_120572/"
    YAH00_GEMINI_ETRADE_WEEKLY_SCHEMA = file_path.SCHEMA_PATH + "yahoo_gemini_etrade_weekly_schema.json"
    YAHOO_GEMINI_MAPPING_SCHEMA = file_path.SCHEMA_PATH + "yahoo_gemini_mapping_schema.json"
    YAHOO_GEMINI_VERIZON_SCHEMA = file_path.SCHEMA_PATH + "yahoo_gemini_verizon_schema.json"


class AWS_SECRET_MANAGER:
    """ AWS_SECRET_MANGER CONSTANT CLASS """
    YAHOO_SECRET_NAME = '/rep_auto_yahoo_gemini/assembly/yahoo_gemini_report'


class TIME:
    """ TIME CONSTANT CLASS """
    MAXIMUM_WAITING_SECONDS_TO_PROCESS_REPORT = 2500
    INTERVAL_TIME_IN_SECONDS = 5


class DATE:
    """ DURATION CONSTANT CLASS """
    YAHOO_GEMINI_MAPPING_DOWNLOAD_DATE_RANGE = 'D:7'
    DATE_FORMAT = "%Y%m%d"


class PLATFORMTYPE:
    """ PLATFORMTYPE CONSTANT CLASS """
    YAHOO_GEMINI_VERIZON = "yahoo_gemini_verizon"
    YAHOO_GEMINI_MAPPING = "yahoo_gemini_mapping"
    YAHOO_GEMINI_WEEKLY_ETRADE = "yahoo_gemini_weekly_etrade"


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    YAHOO_GEMINI_VERIZON_OUTPUT_FILE_NAME = 'Last_14_days_{}.xlsx'
    YAHOO_GEMINI_MAPPING_OUTPUT_FILE_NAME = '{account_name}_{from_date}-{to_date}.xlsx'
    YAHOO_GEMINI_WEEKLY_ETRADE_OUTPUT_FILE_NAME = "Yahoo Gemini Last 14 Days_{}.csv"
    YAHOO_GEMINI_WEEKLY_ETRADE_COLUMN_SET = ['Day', 'Campaign ID', 'Campaign Name', 'Ad ID', 'Ad Name', 'Impressions', 'Clicks', 'Spend']
    DAILY_AND_MAPPING_REPORT_DATE_FORMAT = '%m/%d/%Y'
    DAILY_AND_MAPPING_REPORT_DATE_COLUMN_NAME = 'Day'


class EMAIL:
    """ EMAIL CONSTANT CLASS """
    YAHOO_GEMINI_WEEKLY_ETRADE_MESSAGE_SUBJECT = 'Last 14 days Yahoo Gemini weekly report'
    YAHOO_GEMINI_WEEKLY_ETRADE_MESSAGE_BODY = 'Hi,|Please find attached Yahoo Gemini weekly report| Thank you,| Report Automation'
