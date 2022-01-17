import os
import logging
from time import sleep
# from config.selenium_config import SELECTORTYPE
# from config.pinterest_config import (
#     URL,
#     BUTTON,
#     INPUT,
#     LABEL
# )
# from utils.common.time_utils import date_range

from config.pinterest_config import BUTTON
from utils.selenium.selenium_utils import SeleniumUtilsConstants

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class PinterestUtils:
    """All Pinterest related functionalities."""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def download_report(self, report_download_link):
        """
        this function will download the report using link
        """
        self.selenium_object.get_page(report_download_link)

        current_page_url = self.selenium_object.get_current_page_url()
        modified_url = self.modify_download_url(current_page_url)
        logging.info(f"Final URL to download report - {str(modified_url)}")

        self.selenium_object.get_page(modified_url)

        logging.info("page loaded")

        download_res, download_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.DOWNLOAD_BUTTON_XPATH)
        if download_res:
            logging.info("report download button cicked")
        else:
            logging.info("download button class name changed or security-us.mimecast.com page not loaded")

    def check_download_status(self, number_of_files_to_present):
        """ This function will check wether download is success and files are present in download folder"""
        logging.info("Checking downloading status")
        logging.info(f"Number of files to be present is {number_of_files_to_present}")

        download_res, download_resp = self.selenium_object.check_file_downloaded_status(number_of_files_to_present)
        if not download_res:
            raise download_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Download completed")

    def modify_download_url(self, url):
        """Modifies the download url for Pinterest download page"""

        # remove the 'amp;' string which comes after '&' character
        new_url = url.replace('&amp;', '&')
        return new_url

    # def login(self, username, password):
    #     """
    #     this function will login into the Pinterest analytics site.
    #     username - username to be used for login
    #     password - password to be used for login
    #     """
    #     self.selenium_object.get_page(URL.LOGIN)
    #     sleep(10)
    #     logging.info('email started')
    #     # Find and pass value to username field
    #     email_result, resp_data = self.selenium_object.find_element_by_id(INPUT.EMAIL_ID)
    #     if email_result:
    #         email_element = resp_data[SeleniumUtilsConstants.ELEMENT]
    #         send_email_res, data = self.selenium_object.send_value_to_input(email_element, username, interactive_mode=True)
    #         if not send_email_res:
    #             email_exception = data[SeleniumUtilsConstants.EXCEPTION]
    #             raise email_exception
    #     else:
    #         exception_obj = resp_data[SeleniumUtilsConstants.EXCEPTION]
    #         raise exception_obj

    #     sleep(4)
    #     logging.info('email done')

    #     # Find and pass value to password field
    #     pass_result, pass_data = self.selenium_object.find_element_by_id(INPUT.PASSWORD_ID)
    #     if pass_result:
    #         pass_element = pass_data[SeleniumUtilsConstants.ELEMENT]
    #         set_password_result, data = self.selenium_object.send_value_to_input(pass_element, password, interactive_mode=True)
    #         if not set_password_result:
    #             set_pass_exception = data[SeleniumUtilsConstants.EXCEPTION]
    #             raise set_pass_exception
    #     else:
    #         exception_obj = pass_data[SeleniumUtilsConstants.EXCEPTION]
    #         raise exception_obj
    #     sleep(6)
    #     logging.info("login started")
    #     signin_button_res, resp_data = self.selenium_object.check_element_clickable_and_click(BUTTON.SIGNUP_CLASS_NAME, select_type=SELECTORTYPE.CLASS, interactive_mode=True)
    #     if not signin_button_res:
    #         raise resp_data[SeleniumUtilsConstants.EXCEPTION]
    #     logging.info("login done")
    #     sleep(10)

    # def load_create_report_page(self, account_id):
    #     """
    #     this function will load the create report page for specific account.
    #     account_id - account_id for which the report needs to be generated
    #     """
    #     res, create_tab_resp = self.selenium_object.create_tab()
    #     if res is False:
    #         raise create_tab_resp[SeleniumUtilsConstants.EXCEPTION]

    #     switch_result, switch_res = self.selenium_object.switch_tab()
    #     if switch_result:
    #         logging.info("switched to new tab")
    #         self.selenium_object.get_page(URL.ADS_REPORT.format(account_id=account_id))
    #     else:
    #         logging.info("Error in switching to new tab")
    #         raise switch_res[SeleniumUtilsConstants.EXCEPTION]

    #     logging.info("switched to create report page")

    #     sleep(2)

    # def fill_report_input_details(self, report_name, download_duration, date_format, current_date):
    #     """
    #     this function will help to configure the report options like columns required etc.
    #     report_name - report name to be used for downloading the report
    #     download_duration - indicator for past how many days the reports needs to be downloaded
    #     date_format - the format of the report start and end dates to be entered in the UI
    #     current_date - specifes the current date to download the report;
    #                     By default current date is today's date
    #     """
    #     res, from_date, to_date = date_range(download_duration, date_format, current_date)

    #     if res is False:
    #         return

    #     logging.info('report name input started')
    #     sleep(5)
    #     report_name_res, report_response = self.selenium_object.check_element_loaded_status(LABEL.REPORT_ID, SELECTORTYPE.ID)
    #     if report_name_res:
    #         sleep(5)
    #         report_ele = report_response[SeleniumUtilsConstants.ELEMENT]
    #         res, input_response = self.selenium_object.send_value_to_input(report_ele, report_name)
    #         if res is False:
    #             raise input_response[SeleniumUtilsConstants.EXCEPTION]
    #     else:
    #         raise report_response[SeleniumUtilsConstants.EXCEPTION]

    #     logging.info('report name input done')

    #     start_date_res, st_response = self.selenium_object.find_element_by_id(INPUT.START_DATE_ID)
    #     if start_date_res:
    #         st_element = st_response[SeleniumUtilsConstants.ELEMENT]
    #         start_date_res, resp = self.selenium_object.send_value_to_input(st_element, from_date)
    #         if not start_date_res:
    #             raise resp[SeleniumUtilsConstants.EXCEPTION]
    #     else:
    #         raise st_response[SeleniumUtilsConstants.EXCEPTION]

    #     logging.info('start date {} input done'.format(from_date))

    #     end_date_res, end_element_response = self.selenium_object.find_element_by_id(INPUT.END_DATE_ID)
    #     if end_date_res:
    #         end_element = end_element_response[SeleniumUtilsConstants.ELEMENT]
    #         end_date_res, resp = self.selenium_object.send_value_to_input(end_element, to_date)
    #         if not end_date_res:
    #             raise resp[SeleniumUtilsConstants.EXCEPTION]
    #     else:
    #         raise end_element_response[SeleniumUtilsConstants.EXCEPTION]

    #     logging.info('end date {} input done'.format(to_date))
    #     sleep(6)
    #     find_promotion, prm_ele_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.PIN_PROMOTION_ID, SELECTORTYPE.ID)
    #     if not find_promotion:
    #         raise prm_ele_resp[SeleniumUtilsConstants.EXCEPTION]
    #     logging.info('Ad breakdown selected')

    #     remove_column_res, remove_column_resp = self.selenium_object.find_elements_by_xpath(LABEL.REMOVE_COLUMNS_XPATH)
    #     if remove_column_res:
    #         elements = remove_column_resp[SeleniumUtilsConstants.ELEMENT]
    #         for each in elements:
    #             res, response = self.selenium_object.execute_script(LABEL.COLUMNS_PATH, each)
    #             if res is False:
    #                 raise response[SeleniumUtilsConstants.EXCEPTION]
    #     else:
    #         raise remove_column_resp[SeleniumUtilsConstants.EXCEPTION]

    #     logging.info('remove cols done')

    #     sleep(2)

    #     pinterest_cols = ['Ad group name', 'Ad group ID', 'Campaign ID', 'Campaign name',
    #                       'Earned impressions', 'Earned link clicks', 'Earned saves', 'Impressions', 'Link clicks', 'Paid impressions',
    #                       'Paid link clicks', 'Paid saves', 'Paid video played at ,100,%', 'Paid video played at ,25,%', 'Paid video played at ,75,%', 'Paid video played at ,95,%',
    #                       'Spend']

    #     # the column name search box
    #     search_elt_res, search_resp = self.selenium_object.check_element_loaded_status(LABEL.REPORT_BUILDER_PATH_ID, SELECTORTYPE.ID)
    #     if search_elt_res is False:
    #         raise search_resp[SeleniumUtilsConstants.EXCEPTION]
    #     search_elt = search_resp[SeleniumUtilsConstants.ELEMENT]

    #     logging.info('adding columns')

    #     for each in pinterest_cols:

    #         logging.info("Adding column {column_name}".format(column_name=each))

    #         clear_res, clear_resp = self.selenium_object.clear_content(search_elt)
    #         if clear_res is False:
    #             raise clear_resp[SeleniumUtilsConstants.EXCEPTION]
    #         send_key_res, send_resp = self.selenium_object.send_value_to_input(search_elt, each, click_and_clear_value=False, execute_scriptto_clear_value=False)
    #         if send_key_res is False:
    #             raise send_resp[SeleniumUtilsConstants.EXCEPTION]
    #         sleep(0.5)
    #         # open section
    #         column_res, column_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCORDION_SECTION_XPATH, interactive_mode=True)
    #         if not column_res:
    #             raise column_resp[SeleniumUtilsConstants.EXCEPTION]

    #         # checkbox for the column
    #         elt_res, etl_resp = self.selenium_object.check_element_clickable_and_click(INPUT.INPUT_COLUMN_XPATH.format(column_name=each))
    #         if not elt_res:
    #             raise etl_resp[SeleniumUtilsConstants.EXCEPTION]

    #         # close section
    #         close_res, close_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCORDION_SECTION_XPATH, interactive_mode=True)
    #         if not close_res:
    #             raise close_resp[SeleniumUtilsConstants.EXCEPTION]

    #         # click clear button
    #         clear_res, clear_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.CLEAR_XPATH)
    #         if not clear_res:
    #             raise clear_resp[SeleniumUtilsConstants.EXCEPTION]

    #         logging.info("column Added")

    #     logging.info('adding cols done')

    # def generate_report(self):
    #     """
    #     this function will click on the report generate button from the create report page
    #     which will automatically navigate to the report download page.
    #     """
    #     # click on report download
    #     download_res, download_response = self.selenium_object.check_element_clickable_and_click(BUTTON.DOWNLOAD_XPATH, interactive_mode=True)
    #     if not download_res:
    #         raise download_response[SeleniumUtilsConstants.EXCEPTION]

    #     sleep(10)
    #     logging.info("report download cicked")

    # def click_on_(self):
    #     """ Function to click on download report button in Report History Page"""
    #     download_button_res, download_button_resp = self.selenium_object.find_elements_by_xpath(BUTTON.REPORT_DOWNLOAD_BUTTON_XPATH)
    #     if not download_button_res:
    #         raise download_button_resp[SeleniumUtilsConstants.EXCEPTION]
    #     download_button_elements = download_button_resp[SeleniumUtilsConstants.ELEMENT]
    #     # Get the first element which is always the recent one
    #     element_to_click = download_button_elements[0]
    #     # Click on first download element since always first element will be the latest generated report
    #     click_res, click_response = self.selenium_object.click_element(element_to_click, interactive_mode=True)
    #     if not click_res:
    #         raise click_response[SeleniumUtilsConstants.EXCEPTION]

    # def check_download_status_and_download_report(self):
    #     """ This function will check wether download is success and file is present in download folder
    #         If file is not auto downloaed then call the function to clcik on downoad report button
    #     """
    #     logging.info("Checking downloading status")
    #     download_res, download_resp = self.selenium_object.check_file_downloaded_status()
    #     if not download_res:
    #         logging.info("File not auto downloaded and clicking on download report button")
    #         # If no files downloaded automatically then click the download button on UI
    #         self.click_on_download_report()
    #         # Check if file downloaded and present in the folder
    #         download_res, download_resp = self.selenium_object.check_file_downloaded_status()
    #         if not download_res:
    #             raise download_resp[SeleniumUtilsConstants.EXCEPTION]
    #     logging.info("Download completed")

    # def logout(self):
    #     """
    #     this function will login into the Pinterest analytics site.
    #     username - username to be used for login
    #     password - password to be used for login
    #     """
    #     logging.info('logging out')
    #     self.selenium_object.get_page(URL.LOG_OUT)
    #     logging.info('log out done')
