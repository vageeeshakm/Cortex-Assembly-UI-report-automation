from config.file_config import PATH as file_path


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "trueview/trueview.json"
    FTP_PARENT_DIRECTORY = '/DSP UI Reports - All Clients for Cortex - TrueView- 118686/'
    FTP_PARENT_DIRECTORY_TO_UPLOAD_TRUEVIEW_REPORT = 'Last_14_days_Trueview_Report_{current_date}/'
    FTP_PARENT_DIRECTORY_TO_UPLOAD_CONVERSION_REPORT = 'Last_14_days_Trueview_Conversion_Reports_{current_date}/'
    TRUEVIEW_CONVERSION_SCHEMA = file_path.SCHEMA_PATH  + 'trueview_conversion_schema.json'
    TRUEVIEW_SCHEMA = file_path.SCHEMA_PATH + 'trueview_schema.json'


class DATE:
    """ DURATION CONSTANT CLASS """
    DATE_FORMAT = "%Y%m%d"


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    NUMBER_OF_FOOTER_LINES_TO_SKIP = 14
    NUMBER_OF_STARTING_ROWS_TO_SKIP = 1
    ADVERTISER_COLUMN_TO_FILTER = 'Advertiser'
    TRUEVIEW_COLUMNS_NAME_LIST_TO_FIND_SUM = ['Impressions',
                                              'Clicks',
                                              'Media Cost (Advertiser Currency)',
                                              'Media Fee 1 (Adv Currency)',
                                              'Media Fee 2 (Adv Currency)',
                                              'Media Fee 3 (Adv Currency)',
                                              'Media Fee 4 (Adv Currency)',
                                              'Media Fee 5 (Adv Currency)',
                                              'Revenue (Adv Currency)',
                                              'First-Quartile Views (Video)',
                                              'Midpoint Views (Video)',
                                              'Third-Quartile Views (Video)',
                                              'Complete Views (Video)',
                                              'TrueView: Views',
                                              'Earned Views']
    CONVERSION_COLUMNS_NAME_LIST_TO_FIND_SUM = ['Conversions', 'View through Conversion', 'Store Visit Conversions', 'Total Conversion Value (Adv Currency)']

    TRUEVIEW_COLUMNS_TO_RENAME_DICT = {'YouTube: Views': 'TrueView: Views',
                                       'YouTube Ad ID': 'TrueView Ad ID'}
    CONVERSION_COLUMNS_TO_RENAME_DICT = {'YouTube Ad ID': 'TrueView Ad ID'}

    TRUEVIEW_COLUMNS_TO_CONVERT_INTO_INT = ['Advertiser ID', 'Insertion Order ID', 'Line Item ID', 'TrueView Ad ID', 'Impressions', 'Clicks',
                                            'First-Quartile Views (Video)', 'Midpoint Views (Video)', 'Third-Quartile Views (Video)',
                                            'Complete Views (Video)', 'TrueView: Views', 'Earned Views']

    CONVERSION_COLUMNS_TO_CONVERT_INTO_INT = ['Advertiser ID', 'Insertion Order ID', 'Line Item ID', 'TrueView Ad ID', 'Conversions',
                                              'View through Conversion', 'Store Visit Conversions', 'Total Conversion Value (Adv Currency)']


class FILENAME:
    """ FILENAME CONSTANT CLASS"""
    TRUEVIEW_REPORT_OUTPUT_FILENAME = 'Last_14_days_MA_{advertiser}_Trueview_Reports_{current_date}.csv'
    CONVERSION_REPORT_OUTPUT_FILENAME = 'Last_14_days_MA_{advertiser}_Trueview_conversion_Reports_{current_date}.csv'


class REPORTTYPE:
    """ REPORTTYPE CONSTANT CLASS """
    TRUEVIEW_REPORT_TYPE = 'Trueview'
    CONVERSION_REPORT_TYPE = 'Trueview_conversion'
