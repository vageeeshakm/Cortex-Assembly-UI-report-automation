import os
import logging
from time import sleep
from config.google_config import (
    BUTTON,
    LABEL
)
from config.selenium_config import SELECTORTYPE
from utils.selenium.selenium_utils import SeleniumUtilsConstants


PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class GoogleUtils:
    """All Google related functions"""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def login_page_account_authentication(self, email):
        """This function will find and select google account
           email: email to find and select
        """
        logging.info("Logging into google")
        email_res, email_resp = self.selenium_object.check_element_loaded_status(LABEL.LOGIN_PAGE_EMAIL_XPATH)
        next_button_id = BUTTON.LOGIN_PAGE_NEXT_BUTTON_ID
        if not email_res:
            # If not able to find the previous email element on web page then check for this since google loads different login page sometimes.
            logging.info("Checking Login page")
            # Check for the email in this page.
            email_res, email_resp = self.selenium_object.check_element_loaded_status(LABEL.LOGIN_PAGE_EMAIL_ID, select_type=SELECTORTYPE.ID)
            next_button_id = BUTTON.NEXT_BUTTON_ID
        if email_res:
            email_element = email_resp[SeleniumUtilsConstants.ELEMENT]
            sleep(5)
            res, resp = self.selenium_object.send_value_to_input(email_element, email)
            if not res:
                raise resp[SeleniumUtilsConstants.EXCEPTION]
            # click on next button
            next_button_res, next_button_resp = self.selenium_object.check_element_clickable_and_click(next_button_id, select_type=SELECTORTYPE.ID)
            if not next_button_res:
                raise next_button_resp[SeleniumUtilsConstants.EXCEPTION]
            logging.info("Email found and clicked next")
        else:
            logging.error("Error in finding the email")
            raise email_resp[SeleniumUtilsConstants.EXCEPTION]
        sleep(3)
