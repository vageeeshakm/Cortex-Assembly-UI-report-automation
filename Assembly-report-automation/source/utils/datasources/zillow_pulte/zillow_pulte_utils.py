import os
import logging

from config.zillow_pulte_config import (
    URL,
    BUTTON,
    INPUT
)
from config.selenium_config import KEYINPUT
from utils.selenium.selenium_utils import SeleniumUtilsConstants, SELECTORTYPE

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

class ZillowPulteUtils:

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def login(self, username, password):
        """ This function will login into the Zillow site.
        INPUT:
            username - username to be used for login
            password - password to be used for login
        """

        logging.info("loading login page")
        self.selenium_object.get_page(URL.LOGINPAGE_LOGIN)
        logging.info("page loaded")

        # Find email field and enter the email address
        logging.info('Entering email address')
        email_result, resp_data = self.selenium_object.check_element_loaded_status(INPUT.LOGIN_PAGE_USERNAME_XPATH)
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

        # Find password field and enter the password
        logging.info('Entering password')
        password_result, resp_data = self.selenium_object.check_element_loaded_status(INPUT.LOGIN_PAGE_PASSWORD_XPATH)
        if password_result:
            password_element = resp_data[SeleniumUtilsConstants.ELEMENT]
            send_password_res, data = self.selenium_object.send_value_to_input(password_element, password, keyboard_input=KEYINPUT.ENTER, interactive_mode=True)
            if not send_password_res:
                email_exception = data[SeleniumUtilsConstants.EXCEPTION]
                raise email_exception
        else:
            exception_obj = resp_data[SeleniumUtilsConstants.EXCEPTION]
            raise exception_obj
        logging.info("Password entered")

    def select_filters_and_download(self, params):
        """This function selects the filters in the UI and downloads the report"""

        logging.info("loading builder boost page")
        builder_boost_page_url = URL.BUILDER_BOOST_PAGE.format(account_id=params['account_id'])
        self.selenium_object.get_page(builder_boost_page_url, include_waiting_time=True)

        # click the dropdown button for duration filter
        dropdown_res, dropdown_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.BUILDER_BOOST_PAGE_DURATION_DROPDOWN_CLASS_NAME, select_type=SELECTORTYPE.CLASS, interactive_mode=True)
        if not dropdown_res:
            raise dropdown_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on the duration dropdown button")

        # select the duration from the dropdown
        duration_filter_button = BUTTON.BUILDER_BOOST_PAGE_DURATION_BUTTON.format(download_duration=params['download_duration'])
        line_item_res, line_item_resp = self.selenium_object.check_element_clickable_and_click(duration_filter_button, interactive_mode=True)
        if not line_item_res:
            raise line_item_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("selected the duration filter option")

        # click the dropdown button for group by filter
        dropdown_res, dropdown_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.BUILDER_BOOST_PAGE_GROUP_BY_DROPDOWN_XPATH, interactive_mode=True)
        if not dropdown_res:
            raise dropdown_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on the group by dropdown button")

        # select the group by param from the dropdown
        group_by_filter_button = BUTTON.BUILDER_BOOST_PAGE_GROUP_BY_FILTER_XPATH.format(group_by_filter=params['group_by_filter'])
        line_item_res, line_item_resp = self.selenium_object.check_element_clickable_and_click(group_by_filter_button, interactive_mode=True)
        if not line_item_res:
            raise line_item_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("selected the group by option")

        # select the aggregation level filter
        aggregation_level_button = BUTTON.BUILDER_BOOST_PAGE_AGGREGATION_LEVEL_XPATH.format(aggregation_level=params['aggregation_level'])
        day_filter_res, day_filter_resp = self.selenium_object.check_element_clickable_and_click(aggregation_level_button, interactive_mode=True)
        if not day_filter_res:
            raise day_filter_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on aggregation level filter")

        # click the export table button to download report
        click_download_report_res, click_download_report_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.BUILDER_BOOST_PAGE_EXPORT_TABLE_XPATH, interactive_mode=True)
        if not click_download_report_res:
            raise click_download_report_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("clicked on export table view")

    def check_download_status(self):
        """ This function will check whether download is success and files are present in download folder"""
        logging.info("Checking downloading status")
        download_res, download_resp = self.selenium_object.check_file_downloaded_status()
        if not download_res:
            raise download_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Download completed")

    def logout(self):
        """logout function """
        logging.info('logging out')

        logging.info("Expanding the side bar menu to click Logout!")
        # Expand Side bar before click on Log out button
        sidebar_click_result, click_resp_data = self.selenium_object.check_element_clickable_and_click(BUTTON.SIDEBAR_HEADER_XPATH, interactive_mode=True)
        if not sidebar_click_result:
            raise click_resp_data[SeleniumUtilsConstants.EXCEPTION]

        logging.info("Clicking on Log Out!")
        # click on logout button
        log_out_button_click_res, log_out_button_click_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.SIDEBAR_LOGOUT_BUTTON_XPATH, interactive_mode=True)
        if not log_out_button_click_res:
            raise log_out_button_click_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info('log out done')
