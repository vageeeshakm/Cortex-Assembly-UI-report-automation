from config.file_config import PATH as file_path


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "zillow/zillow.json"
    ZILLOW_PULTE_SCHEMA = file_path.SCHEMA_PATH + "zillow_schema.json"



class AWS_SECRET_MANAGER:
    """ AWS_SECRET_MANGER CONSTANT CLASS """
    SECRET_NAME = '/rep_auto_zillow_pulte/assembly/zillow_pulte'


class URL:
    """URL CONSTANT CLASS"""
    LOGINPAGE_LOGIN = "https://builders.zillowgroup.com/login"
    BUILDER_BOOST_PAGE = "https://builders.zillowgroup.com/reports/builder-boost/overview?accountId={account_id}=="


class INPUT:
    """Input elements CONSTANT CLASS"""
    LOGIN_PAGE_USERNAME_XPATH = "//input[contains(@name,'email')]"
    LOGIN_PAGE_PASSWORD_XPATH = "//input[contains(@name,'password')]"


class BUTTON:
    """Button elements in UI"""

    BUILDER_BOOST_PAGE_GROUP_BY_DROPDOWN_XPATH = '//div[@Class="dropdown reports-entity-group-by-dropdown__dropdown"]//child::div[@Class="dropdown__label"]'
    BUILDER_BOOST_PAGE_DURATION_DROPDOWN_CLASS_NAME = 'date-range'
    BUILDER_BOOST_PAGE_DURATION_BUTTON = '//*[(text()="{download_duration}")]'
    BUILDER_BOOST_PAGE_GROUP_BY_FILTER_XPATH = '//*[(text()="{group_by_filter}")]'
    BUILDER_BOOST_PAGE_AGGREGATION_LEVEL_XPATH = '//*[(text()="{aggregation_level}")]//ancestor::button'
    BUILDER_BOOST_PAGE_EXPORT_TABLE_XPATH = '//*[(text()="Export Table View")]//ancestor::button'
    SIDEBAR_LOGOUT_BUTTON_XPATH = "//*[contains(text(), 'Log Out')]//parent::a"
    SIDEBAR_HEADER_XPATH = "//span[contains(@class, 'sidebar__header')]"


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    SHEET_NAME = 'sheet1'
    DATE_COLUMNS_LIST_IN_SHEET = ['Day', 'Line Item Start Date', 'Line Item End Date']
    CHANNEL_FILTER_COLUMN = 'Channel'
    CHANNEL_FILTER_VALUE = 'Offsite'
    COLUMNS_TO_ROUND_OFF = ['CTR', 'eCPC', 'eCPM', 'Est. Spend']
    DECIMAL_DIGITS = 2


class DATE:
    """ DATE CLASS """
    DATE_FORMAT = "%Y%m%d"
