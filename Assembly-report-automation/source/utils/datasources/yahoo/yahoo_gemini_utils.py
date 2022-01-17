import os
import logging
from time import sleep
from config.selenium_config import SELECTORTYPE, KEYINPUT
from config.yahoo_gemini_config import (
    URL,
    BUTTON,
    INPUT,
    LABEL,
    TIME
)
from utils.selenium.selenium_utils import SeleniumUtilsConstants

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class YahooGeminiUtils:
    """All yahoo Gemini related functionalities."""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def login(self, username, password):
        """
        this function will login into the Yahoo Gemini analytics site.
        username - username to be used for login
        password - password to be used for login
        """
        self.selenium_object.get_page(URL.LOGINPAGE_LOGIN)
        sleep(10)
        logging.info('Entering email address')
        email_result, resp_data = self.selenium_object.check_element_loaded_status(INPUT.LOGINPAGE_USERNAME_ID, select_type=SELECTORTYPE.ID)
        if email_result:
            email_element = resp_data[SeleniumUtilsConstants.ELEMENT]
            send_email_res, data = self.selenium_object.send_value_to_input(email_element, username, keyboard_input=KEYINPUT.ENTER, interactive_mode=True)
            if not send_email_res:
                email_exception = data[SeleniumUtilsConstants.EXCEPTION]
                raise email_exception
        else:
            exception_obj = resp_data[SeleniumUtilsConstants.EXCEPTION]
            raise exception_obj
        logging.info("Email entered")
        sleep(4)
        # FInd password field and enter the password
        password_result, resp_data = self.selenium_object.check_element_loaded_status(INPUT.LOGIN_PAGE_PASSWORD_ID, select_type=SELECTORTYPE.ID)
        if password_result:
            password_element = resp_data[SeleniumUtilsConstants.ELEMENT]
            send_password_res, data = self.selenium_object.send_value_to_input(password_element, password, keyboard_input=KEYINPUT.ENTER, interactive_mode=True)
            if not send_password_res:
                email_exception = data[SeleniumUtilsConstants.EXCEPTION]
                raise email_exception
        else:
            logging.error("Either element not found or redirected to a page and asking to enter OTP")
            exception_obj = resp_data[SeleniumUtilsConstants.EXCEPTION]
            raise exception_obj
        logging.info("password entered")
        sleep(5)
        logging.info("logging done")

    def create_new_report(self, report_type):
        """
        this function will create new report by selecting the report type
        INPUT: report_type - report type to be created
        """
        create_report_res, create_report_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.REPORTING_PAGE_NEW_REPORT_XPATH, interactive_mode=True)
        if not create_report_res:
            logging.error("Either element not found or Asking to choose I am not a Robot option")
            raise create_report_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Clicked on new report")

        ad_group_res, ad_group_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_AD_GROUP_PERFORMANCE_XPATH.format(report_type), interactive_mode=True)
        if not ad_group_res:
            raise ad_group_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Ad group selected")

        sleep(8)

    def select_account(self, account_name):
        """
        Function to search and select the account
        INPUT: account_name - account name to be selected
        """
        search_account_res, search_account_resp = self.selenium_object.check_element_loaded_status(INPUT.REPORTING_PAGE_SEARCH_ACCOUNT_XPATH)
        if not search_account_res:
            raise search_account_resp[SeleniumUtilsConstants.EXCEPTION]
        send_account_name_res, send_account_name_resp = self.selenium_object.send_value_to_input(search_account_resp[SeleniumUtilsConstants.ELEMENT], account_name, interactive_mode=True)
        if not send_account_name_res:
            raise send_account_name_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Account searched")

        sleep(6)
        select_account_res, select_account_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_SELECT_ALL_REPORTS_XPATH, interactive_mode=True)
        if not select_account_res:
            raise select_account_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Account selected")

    def select_date_range(self, date_range):
        """ Function to select the date range for which report to be downloaded
            INPUT: date_range - date range to be selected to download the report
        """
        date_res, date_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_SELECT_DATE_XPATH, interactive_mode=True)
        if not date_res:
            raise date_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on date-range")

        select_date_res, select_date_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_LAST_14_DAYS_DATE_RANGE_XPATH.format(date_range), interactive_mode=True)
        if not select_date_res:
            raise select_date_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("date-range selected")

    def select_report_columns(self, column_set):
        """ Select columns(Combination of metrics and dimention) in the repport
            INPUT: column_set - column set to be choosed to download the report
        """
        columns_res, columns_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_COLUMN_ID, select_type=SELECTORTYPE.ID, interactive_mode=True)
        if not columns_res:
            raise columns_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("column field clicked")

        select_columns_res, select_columns_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_LAST_14_DAYS_COLUMN_XPATH.format(column_set), interactive_mode=True)
        if not select_columns_res:
            raise select_columns_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("column selected")

    def generate_report(self):
        """ Function to generate the report"""
        sleep(5)
        create_report_res, create_report_resp = self.selenium_object.check_element_clickable_and_click(LABEL.REPORTING_PAGE_CREATE_REPORT_XPATH, interactive_mode=True)
        if not create_report_res:
            raise create_report_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on create report")

    def check_report_processing_status(self):
        """ Function to check if report is processed and available for download"""
        report_still_processing = True
        # maximum processing time is 30 minutes.
        maximum_waiting_time_to_process = TIME.MAXIMUM_WAITING_SECONDS_TO_PROCESS_REPORT
        current_waiting_time = 0
        while report_still_processing and current_waiting_time <= maximum_waiting_time_to_process:
            report_ready_status_res, report_ready_status_resp = self.selenium_object.find_element_by_xpath(LABEL.DOWNLOAD_REPORT_PAGE_READY_STATUS_XPATH)
            if report_ready_status_res:
                report_still_processing = False
                return True
            else:
                logging.info("Still waiting to get report ready for download")
                sleep(TIME.INTERVAL_TIME_IN_SECONDS)
                current_waiting_time = current_waiting_time + TIME.INTERVAL_TIME_IN_SECONDS
                processing_status_res, processing_status_resp = self.selenium_object.find_element_by_xpath(LABEL.DOWNLOAD_REPORT_PAGE_PROCESSING_STATUS_XPATH)
                if not processing_status_res:
                    report_ready_status_res, report_ready_status_resp = self.selenium_object.find_element_by_xpath(LABEL.DOWNLOAD_REPORT_PAGE_READY_STATUS_XPATH)
                    if report_ready_status_res:
                        logging.info("report processed and it can be downloaded")
                        report_still_processing = False
                        return True
                    if not report_ready_status_res:
                        logging.info("Report processing failed !")
                        report_still_processing = False
                        return False
        logging.info("waiting time to process the report exceeded")
        return False

    def download_report(self):
        """ Function that will click on download report option"""
        logging.info("downloading the report..")
        download_report_res, download_report_resp = self.selenium_object.find_element_by_xpath(LABEL.DOWNLOAD_REPORT_PAGE_DOWNLOAD_REPORT_XPATH)
        if not download_report_res:
            raise download_report_resp[SeleniumUtilsConstants.EXCEPTION]
        click_res, click_resp = self.selenium_object.click_element(download_report_resp[SeleniumUtilsConstants.ELEMENT], interactive_mode=True)
        if not click_res:
            raise click_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on download report")

    def check_download_status(self):
        """ This function will check wether download is success and files are present in download folder"""
        sleep(5)
        logging.info("Checking downloading status")
        download_res, download_resp = self.selenium_object.check_file_downloaded_status()
        if not download_res:
            raise download_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Download completed")

    def logout(self):
        """
        logout function
        """
        logging.info('logging out')
        # open log out dropdown by clicking on header
        log_out_header_res, log_out_header_resp = self.selenium_object.check_element_clickable_and_click(LABEL.DOWNLOAD_REPORT_PAGE_LOGOUT_HEADER_XPATH, interactive_mode=True)
        if not log_out_header_res:
            raise log_out_header_resp[SeleniumUtilsConstants.EXCEPTION]
        # click on log out button
        log_out_res, log_out_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.DOWNLOAD_REPORT_PAGE_LOGOUT_BUTTON_XPATH, interactive_mode=True)
        if not log_out_res:
            raise log_out_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info('log out done')
        sleep(5)
