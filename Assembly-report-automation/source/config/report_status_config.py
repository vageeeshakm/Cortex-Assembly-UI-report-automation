class EMAIL:
    """ EMAIL CONSTANT CLASS """
    EMAIL_SUBJECT = "UI Automation Task Report"
    EMAIL_BODY = "Please find the attached reports"


class S3:
    """ S3 CONSTANT CLASS"""
    BUCKET_NAME = 'mdc-common-core'


class PATH:
    """ PATH CONSTANT CLASS"""
    MAIN_FOLDER_ON_S3 = 'ui-automated-report-validations/{current_date}/{platform}/'
    SUCCESS_VALIDATION_REPORTS_FOLDER = 'upload_validation/success_reports/'
    FAILURE_VALIDATION_REPORTS_FOLDER = 'upload_validation/failed_reports/'
    DATA_VALIDATION_REPORT_DETAILS = 'data_validation/report_details/'
    DATA_VALIDATION_REPORT_SUMMARY = 'data_validation/report_summary/'
    CONSOLIDATED_VALIDATION_REPORTS_FOLDER = 'upload_validation/'
    CONSOLIDATED_DATA_VALIDATION_REPORTS_FOLDER = 'data_validation/'
    REPORT_NAME = "{advertiser}.csv"
    CONSOLIDATED_SUCCESS_REPORT_NAME = 'file_upload_success_report.csv'
    CONSOLIDATED_FAILURE_REPORT_NAME = 'file_upload_failure_report.csv'
    CONSOLIDATED_REPORT_DETAILS_NAME = 'data_validation_details.csv'
    CONSOLIDATED_REPORT_SUMMARY_NAME = 'data_validation_summary.csv'


class PLATFORM:
    """ PLATFORM CONSTANT CLASS"""
    PINTEREST = 'pinterest'
    ZILLOW_PULTE = 'zillow_pulte'
    GOOGLE_ANALYTICS = 'google_analytics'
    GOOGLE_ANALYTICS_WEEKLY = 'google_analytics_weekly'
    DBM = 'dbm'
    LINKEDINDAILY = 'linkedin_daily'
    LINKEDINWEEKLY = 'linked_weekly'
    TRUEVIEW = 'trueview'
    TTD = 'ttd'
    ETRADE = 'etrade'


class REPORTLOCATION:
    """ REPORT LOCATION CONSTANT CLASS"""
    EMAIL = 'EMAIL'
    FTP = 'FTP'
    S3 = 'S3'


class SUCCESSREPORTCOLUMNS:
    """ SUCCESS REPORT COLUMNS CONSTANT CLASS"""
    ADVERTISER = 'Advertiser Name'
    ACCOUNT = 'Account Name'
    UPLOAD_PATH = 'Uploaded Path'
    REPORT_GENERATED_TIME = 'Report generated timestamp'
    LOCATION = 'Location'


class FAILUREREPORTCOLUMNS:
    """ FAILURE REPORT COLUMNS CONSTANT CLASS"""
    ADVERTISER = 'Advertiser Name'
    ACCOUNT = 'Account Name'
    UPLOAD_PATH = 'Path to Upload'
    ERROR = 'Error Message'
    REPORT_GENERATED_TIME = 'Report generated timestamp'
    LOCATION = 'Location'


class S3DATE:
    """ DATE CONSTANT CLASS """
    DATE_FORMAT = "%Y%m%d"
