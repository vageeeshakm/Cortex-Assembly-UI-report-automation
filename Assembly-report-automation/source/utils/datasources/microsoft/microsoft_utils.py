import os
import logging
from time import sleep
from config.selenium_config import SELECTORTYPE
from config.microsoft_config import (
    BUTTON,
    LABEL
)
from utils.selenium.selenium_utils import SeleniumUtilsConstants

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class MicrosoftUtils:
    """All Microsoft related functions"""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def login_page_username_authentication(self, email):
        """This function will find and select microsoft account
           email: email to find and select
        """
        logging.info("Finding microsoft account")
        sleep(3)
        email_res, email_resp = self.selenium_object.check_element_loaded_status(LABEL.LOGIN_PAGE_EMAIL_XPATH)
        if email_res:
            # Enter email into the input box
            email_element = email_resp[SeleniumUtilsConstants.ELEMENT]
            sleep(4)
            res, resp = self.selenium_object.send_value_to_input(email_element, email, click_and_clear_value=False, execute_scriptto_clear_value=False, interactive_mode=True)
            if not res:
                raise resp[SeleniumUtilsConstants.EXCEPTION]
            # Click on next button
            next_button_res, next_button_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.LOGIN_PAGE_SUBMIT_BUTTON_XPATH)
            if not next_button_res:
                raise next_button_resp[SeleniumUtilsConstants.EXCEPTION]
            logging.info("Email found and clicked next")
        else:
            logging.info("Entering email Failed")
            raise email_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Microsoft account found")

    def login_page_password_authentication(self, password):
        """This function will authenticate microsoft account
           password: password for the microsoft account
        """
        sleep(5)
        logging.info("Authenticating microsoft account")
        # Find and enter the password into password input field
        pass_res, pass_resp = self.selenium_object.check_element_loaded_status(LABEL.LOGIN_PAGE_PASSWORD_XPATH)
        if pass_res:
            pass_element = pass_resp[SeleniumUtilsConstants.ELEMENT]
            sleep(5)
            res, resp = self.selenium_object.send_value_to_input(pass_element, password, click_and_clear_value=False, execute_scriptto_clear_value=False, interactive_mode=True)
            if not res:
                raise resp[SeleniumUtilsConstants.EXCEPTION]
            submit_button_res, submit_button_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.ENTER_PASSWORD_PAGE_SIGN_IN_BUTTON_XPATH)
            if not submit_button_res:
                raise submit_button_resp[SeleniumUtilsConstants.EXCEPTION]
            logging.info("password Entered and clicked on submit")
        else:
            logging.info("Entering password step failed")
            raise pass_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Microsoft Account Authenticated")
