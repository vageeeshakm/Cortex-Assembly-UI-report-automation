from config.file_config import PATH as file_path


class URL:
    """URL CONSTANT CLASS"""
    LOGINPAGE_LOGIN = "https://analytics.google.com/analytics"


class BUTTON:
    """Button element CONSTANT CLASS"""
    ACCOUNT_PAGE_SEARCH_XPATH = '//*[@class="suite-up-search-post-gmp ng-pristine ng-valid"]/child::input'


class INPUT:
    """Input elements CONSTANT CLASS"""
    CUSTOM_REPORT_PAGE_TOTAL_PAGES_XPATH = '//*[@class="C_PAGINATION_ROWS_LONG"]/child::label'
    REPORT_PAGE_START_DATE_XPATH = "//input[contains(@class, 'ID-datecontrol-primary-start')]"
    REPORT_PAGE_END_DATE_XPATH = "//input[contains(@class, 'ID-datecontrol-primary-end')]"


class LABEL:
    """LABEL Element CONSTANT CLASS"""
    STAY_AIGNED_IN_PAGE_BACK_BUTTON_ID = "idBtn_Back"
    HOME_PAGE_CUSTOMIZATION_SECTION_XPATH = '//*[@id="customizationSection"]/child::div'
    HOME_PAGE_CUSTOM_REPORT_TYPE_XPATH = "//span[contains(text(), '{report_type}')]/parent::span/parent::div"
    HOME_PAGE_ACCOUNT_XPATH = '//*[@id="suite-top-nav"]//suite-universal-picker//button'
    ACCOUNT_PAGE_REPORT_NAME_XPATH = '//*[(text()="{account_id}")]//ancestor::a'
    CUSTOM_REPORT_PAGE_IFRAME_ID = "galaxyIframe"
    CUSTOM_REPORT_PAGE_SELECT_REPORT_SHOW_ROW_XPATH = '//*[@class="ID-tablePagination _GAWS"]//select'
    CUSTOM_REPORT_PAGE_EXPORT_XPATH = '//*[@id="ID-reportHeader-reportToolbar"]//*[contains(text(), "Export")]//parent::span'
    CUSTOM_REPORT_PAGE_DOWNLOAD_CSV_XPATH = '//*[@id="ID-reportHeader-reportToolbar-exportControl"]//*[contains(text(), "CSV")]//parent::li'
    CUSTOM_REPORT_PAGE_PAGE_INPUT_XPATH = '//span[@class="C_PAGINATION_ROWS_LONG"]//input'
    CUSTOM_REPORT_PAGE_SETTINGS_XPATH = '//*[@id="suite-top-nav"]//suite-gaia-switcher//button'
    CUSTOM_REPORT_PAGE_LOGOUT_XPATH = '//*[@class="suite-gaia-panel-content"]//*[contains(text(), "Sign out")]//ancestor::a'
    TEMPLATE_ELEMENT_MAPPING = {
        "automation_pulte_t_1": '//*[@id="{report_table_id}"]//div[contains(text(), "automation_pulte_t_1")]',
        "automation_contact_us_innovage_t_1": '//*[@id="{report_table_id}"]//div[contains(text(), "automation_contact_u")]',
        "automation_lead_source_innovage_t_1": '//*[@id="{report_table_id}"]//a[contains(text(), "automation_lead_source_innovage_t_1")]',
        "automation_lead_source_innovage_t_2": '//*[@id="{report_table_id}"]//a[contains(text(), "automation_lead_source_innovage_t_2")]',
        "automation_lead_source_innovage_t_3": '//*[@id="{report_table_id}"]//a[contains(text(), "automation_lead_source_innovage_t_3")]',
        "automation_web_session_innovage_t_1": '//*[@id="{report_table_id}"]//div[contains(text(), "automation_web_sessi")]'
    }
    CUSTOM_DATE_PICKER_XPATH = '//*[@id="ID-reportHeader-customSection"]//div[contains(@data-guidedhelpid, "date-picker-container")]//table'
    DATE_RANGE_DROPDOWN_XPATH = '//*[@id="ID-reportHeader-customSection"]//div[contains(text(), "Date Range: ")]//select'
    CUSTOM_DATE_PICKER_APPLY_XPATH = '//*[@id="ID-reportHeader-customSection"]//input[contains(@value, "Apply") and contains(@type, "button")]'
    DATE_RANGE_TEXT_MAPPING = {
        'M:1': 'Last month',
        'D:7': 'Last 7 days'
    }
    REPORT_TYPE_TEXT_MAPPING = {
        'saved_report': 'Saved Reports',
        'custom_report': 'Custom Reports'
    }
    REPORT_TABLE_ID_MAPPING = {
        'saved_report': 'ID-savedReportsTable',
        'custom_report': 'ID-customReportsTable'
    }


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "google_analytics/google_analytics.json"
    WEEKLY_JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "google_analytics/google_analytics_weekly.json"
    FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES_PULTE = '/Google Analytics UI Report for Pulte_128054/'
    FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES_INNOVAGE = '/Google Analytics UI Report for Innovage_132372/'
    GOOGLE_ANALYTICS_PULTE_SCHEMA = file_path.SCHEMA_PATH + 'google_analytics_pulte_schema.json'
    GOOGLE_ANALYTICS_INNOVAGE_SCHEMA_MAPPING = {
        'Innovage Contact Us': file_path.SCHEMA_PATH + 'google_analytics_innovage_contact_us_schema.json',
        'Innovage Lead Source': file_path.SCHEMA_PATH + 'google_analytics_innovage_lead_source_schema.json',
        'Innovage Web Session': file_path.SCHEMA_PATH + 'google_analytics_innovage_web_session_schema.json'
    }


class PANDASSETTINGS:
    NUMBER_OF_ROWS_TO_SKIP = 6
    INSERT_COLUMN_INDEX = 1
    COLUMN_NAME_TO_INSERT = 'Profile ID'
    PULTE_OUTPUT_FILE_NAME = 'Analytics All Web Site Data_{account_name}_report {from_date}-{to_date}.csv'
    INNOVAGE_OUTPUT_FILE_NAME = 'Analytics InnovAge Production View {account_name} report {from_date}-{to_date}.csv'
    HOST_COLUMN_NAME_TO_FILTER = 'Hostname'
    NUMBER_OF_EMPTY_COLUMNS_TO_INSERT = 7


class DATE:
    """ DURATION CONSTANT CLASS """
    DATE_FORMAT = "%Y%m%d"
    WEEKLY_REPORT_INPUT_DATE_FORMAT = '%m/%d/%Y'


class AWS_SECRET_MANAGER:
    GOOGLE_ANALYTICS_SECRET_NAME = '/rep_auto_google_analytics/assembly/google_analytics'
