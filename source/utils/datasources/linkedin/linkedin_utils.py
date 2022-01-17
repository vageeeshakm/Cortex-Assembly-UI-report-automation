import os
import logging
from time import sleep
from config.selenium_config import SELECTORTYPE, KEYINPUT
from config.linkedin_config import (
    URL,
    BUTTON,
    INPUT,
    LABEL
)
from utils.selenium.selenium_utils import SeleniumUtilsConstants
from utils.common.time_utils import date_range

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class LinkedInUtils:
    """All LinkedIn related functionalities."""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def login(self, username, password):
        """
        this function will login into the LinkedIn site.
        username - username to be used for login
        password - password to be used for login
        """
        logging.info("loading page")
        self.selenium_object.get_page(URL.LOGINPAGE_LOGIN)
        logging.info("page loaded")
        res, resp = self.selenium_object.switch_into_iframe(select_type=SELECTORTYPE.CLASS, identifier=LABEL.LOGIN_PAGE_IFRAME_CLASS)
        if not res:
            raise resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info("Switched into iframe")

        logging.info('Entering email address')
        email_result, resp_data = self.selenium_object.check_element_loaded_status(INPUT.LOGIN_PAGE_USERNAME_ID, select_type=SELECTORTYPE.ID)
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
        # FInd password field and enter the password
        password_result, resp_data = self.selenium_object.check_element_loaded_status(INPUT.LOGIN_PAGE_PASSWORD_ID, select_type=SELECTORTYPE.ID)
        if password_result:
            password_element = resp_data[SeleniumUtilsConstants.ELEMENT]
            send_password_res, data = self.selenium_object.send_value_to_input(password_element, password, keyboard_input=KEYINPUT.ENTER, interactive_mode=True)
            if not send_password_res:
                email_exception = data[SeleniumUtilsConstants.EXCEPTION]
                raise email_exception
        else:
            exception_obj = resp_data[SeleniumUtilsConstants.EXCEPTION]
            raise exception_obj
        logging.info("password entered")
        logging.info("logging done")

    def load_account_page(self, account_id):
        """
        Function to load account page for specified account id
        INPUT: account_id - account id to be loaded
        """
        self.selenium_object.get_page(URL.ACCOUNT_PAGE_CAMPAIGN_MANAGER.format(account_id=account_id))
        # After loding the page wait for 5 seconds
        logging.info("Page loaded for specified Account")

    def select_account_ads(self):
        """ Functio to select ads for account"""
        click_ads_res, click_ads_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.ACCOUNT_PAGE_ADS_XPATH, interactive_mode=True)
        if not click_ads_res:
            raise click_ads_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Account Ads selected")

    def check_account_ads(self):
        """ Function to verify if ads are present for an account"""
        logging.info("Checking Ads for this account")
        # Check If no Ads for that account
        check_ads_res, check_ads_resp = self.selenium_object.check_element_loaded_status(LABEL.ACCOUNT_PAGE_NO_RECORDS_XPATH)
        if check_ads_res:
            logging.info("No ads present for this account")
            return False
        return True

    def select_date_range_to_generate_report(self, date_range_to_select, date_format):
        """" Function to select date range to generate the report
             INPUT:
                date_range_to_select - date range. Ex: D:14 to select last 14 days in report
                date_format - date format
            OUTPUT:
                select start and end date in report to be generated.
        """
        res, from_date, to_date = date_range(date_range_to_select, date_format)
        logging.info("from date {}".format(from_date))
        logging.info("to date {}".format(to_date))

        date_header_res, date_header_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCOUNT_PAGE_DATE_SELECTOR_ID, select_type=SELECTORTYPE.ID, interactive_mode=True)
        if not date_header_res:
            raise date_header_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on select date range")
        # select start date
        start_date_label_res, start_date_label_res = self.selenium_object.check_element_loaded_status(LABEL.ACCOUNT_PAGE_START_DATE_XPATH)
        if not start_date_label_res:
            raise start_date_label_res[SeleniumUtilsConstants.EXCEPTION]
        # send start date value
        input_start_date_res, input_start_date_resp = self.selenium_object.send_value_to_input(
            start_date_label_res[SeleniumUtilsConstants.ELEMENT],
            from_date,
            execute_scriptto_clear_value=True,
            click_and_clear_value=False)
        if not input_start_date_res:
            raise input_start_date_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Start date entered")
        # Select end date
        end_date_label_res, end_date_label_res = self.selenium_object.check_element_loaded_status(LABEL.ACCOUNT_PAGE_END_DATE_XPATH)
        if not end_date_label_res:
            raise end_date_label_res[SeleniumUtilsConstants.EXCEPTION]
        # send end date value
        input_end_date_res, input_end_date_resp = self.selenium_object.send_value_to_input(
            end_date_label_res[SeleniumUtilsConstants.ELEMENT],
            to_date,
            execute_scriptto_clear_value=True,
            click_and_clear_value=False)
        if not input_end_date_res:
            raise input_end_date_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("End date entered")
        # Sleep is required to wait sometime before clicking on update date button.
        # If time gap is not given then update button may clicked before end date gets refreshed after enring value
        sleep(5)
        # click on update date
        update_date_res, update_date_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCOUNT_PAGE_UPDATE_DATE_XPATH, interactive_mode=True)
        if not date_header_res:
            raise date_header_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("start and end dates are updated")

    def generate_report(self, report_type):
        """ Function to generate the report
            INPUT: report_type - report type to generate report
                   example: Ad performance type
        """
        # click on generate report button
        click_generate_report_res, click_generate_report_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCOUNT_PAGE_GENERATE_REPORT_XPATH, interactive_mode=True)
        if not click_generate_report_res:
            raise click_generate_report_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on generate report")

        report_type_res, report_type_resp = self.selenium_object.check_element_loaded_status(LABEL.ACCOUNT_PAGE_REPORT_TYPE_XPATH)
        if not report_type_res:
            raise report_type_resp[SeleniumUtilsConstants.EXCEPTION]

        # Select report type from dropdown option
        select_report_type_res, select_report_type_resp = self.selenium_object.select_from_drop_down(report_type_resp[SeleniumUtilsConstants.ELEMENT], report_type)
        if not select_report_type_res:
            raise select_report_type_resp[SeleniumUtilsConstants.EXCEPTION]

        click_breakdown_res, click_breakdown_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCOUNT_PAGE_ADS_BREAKDOWN_XPATH, interactive_mode=True)
        if not click_breakdown_res:
            raise click_breakdown_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Ads breakdown selected")

    def download_report(self):
        """ Function that will click on download report option"""
        logging.info("downloading the report..")

        click_download_report_res, click_download_report_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCOUNT_PAGE_EXPORT_REPORT_XPATH, interactive_mode=True)
        if not click_download_report_res:
            raise click_download_report_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info("clicked on download report")

    def check_download_status(self, files_to_be_downloaded):
        """ This function will check wether download is success and files are present in download folder
            INPUT: files_to_be_downloaded - number of files to be downloaded
        """
        logging.info("Checking downloading status")
        download_res, download_resp = self.selenium_object.check_file_downloaded_status(files_to_be_downloaded)
        if not download_res:
            raise download_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Download completed")

    def logout(self):
        """
        logout function
        """
        logging.info('logging out')

        # click on loagout header option
        log_out_header_click_res, log_out_header_click_resp = self.selenium_object.check_element_clickable_and_click(LABEL.LOGOUT_PAGE_LOGOUT_HEADER_XPATH, interactive_mode=True)
        if not log_out_header_click_res:
            raise log_out_header_click_resp[SeleniumUtilsConstants.EXCEPTION]

        # click on logout button
        log_out_button_click_res, log_out_button_click_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.LOGOUT_PAGE_LOGOUT_XPATH, interactive_mode=True)
        if not log_out_button_click_res:
            raise log_out_button_click_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info('log out done')
