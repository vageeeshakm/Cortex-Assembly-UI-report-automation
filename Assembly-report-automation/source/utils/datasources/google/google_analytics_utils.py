import os
import logging
from time import sleep
from config.selenium_config import KEYINPUT, SELECTORTYPE
from config.google_analytics_config import (
    URL,
    INPUT,
    BUTTON,
    LABEL,
    DATE
)
from utils.common.time_utils import date_range
from utils.common.general_utils import extract_extension_from_filename
from utils.datasources.google.google_utils import GoogleUtils
from utils.datasources.microsoft.microsoft_utils import MicrosoftUtils
from utils.selenium.selenium_utils import SeleniumUtilsConstants

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class GoogleAnalyticsUtils:
    """All Google Analytics related functionalities."""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object
        self.number_of_downloading_files = 0

    def login(self, username, password):
        """ This function will login into google analytics
            username - username to be used for login
            password - password to be used for login
        """
        logging.info("loading page")
        self.selenium_object.get_page(URL.LOGINPAGE_LOGIN)
        sleep(10)
        logging.info("login stared")
        google_object = GoogleUtils(selenium_object=self.selenium_object)
        google_object.login_page_account_authentication(username)
        microsoft_object = MicrosoftUtils(selenium_object=self.selenium_object)
        microsoft_object.login_page_username_authentication(username)
        microsoft_object.login_page_password_authentication(password)
        stay_signed_res, stay_signed_resp = self.selenium_object.check_element_clickable_and_click(LABEL.STAY_AIGNED_IN_PAGE_BACK_BUTTON_ID, select_type=SELECTORTYPE.ID, interactive_mode=True)
        if not stay_signed_res:
            raise stay_signed_resp[SeleniumUtilsConstants.EXCEPTION]
        sleep(2)
        logging.info("Logging done")

    def report_customization_section(self):
        """This function will click and select customization section in the report"""
        logging.info("report customization")
        customization_res, customization_resp = self.selenium_object.check_element_clickable_and_click(LABEL.HOME_PAGE_CUSTOMIZATION_SECTION_XPATH, interactive_mode=True)
        if not customization_res:
            raise customization_resp[SeleniumUtilsConstants.EXCEPTION]
        sleep(2)
        logging.info("clicked on cutomization section")

    def select_report_type(self, report_type):
        """This function will select the report type which is inside customize report section"""
        report_type_text = LABEL.REPORT_TYPE_TEXT_MAPPING[report_type]
        report_res, report_resp = self.selenium_object.check_element_clickable_and_click(LABEL.HOME_PAGE_CUSTOM_REPORT_TYPE_XPATH.format(report_type=report_type_text), interactive_mode=True)
        if not report_res:
            raise report_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("report customization done.")

    def select_analytics_account(self, account_id, profile_id):
        """ Functioon will select analytics account
            account_id : account id for which report should be downloaded
            profile_id : profile to be searched in input box
        """
        # click on account section of google analytics page
        account_res, account_resp = self.selenium_object.check_element_clickable_and_click(LABEL.HOME_PAGE_ACCOUNT_XPATH)
        if not account_res:
            raise account_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Searching account name")
        # find search element
        search_res, search_resp = self.selenium_object.check_element_loaded_status(BUTTON.ACCOUNT_PAGE_SEARCH_XPATH)
        if not search_res:
            raise search_resp[SeleniumUtilsConstants.EXCEPTION]
        sleep(3)
        # send report id to input element
        send_key_res, send_key_resp = self.selenium_object.send_value_to_input(search_resp[SeleniumUtilsConstants.ELEMENT], profile_id, execute_scriptto_clear_value=False, interactive_mode=True)
        if not send_key_res:
            raise send_key_resp[SeleniumUtilsConstants.EXCEPTION]
        # click the report after it is found
        sleep(5)
        report_res, report_resp = self.selenium_object.check_element_clickable_and_click(LABEL.ACCOUNT_PAGE_REPORT_NAME_XPATH.format(account_id=account_id), interactive_mode=True)
        if not report_res:
            raise report_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info("Account selected")

    def load_custom_report(self, report_template_name, report_type):
        """ This function will loads the report by clicking on report that to be downloaded"""
        sleep(5)
        res, resp = self.selenium_object.switch_into_iframe(select_type=SELECTORTYPE.ID, identifier=LABEL.CUSTOM_REPORT_PAGE_IFRAME_ID)
        if not res:
            raise resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("switched to iframe")
        sleep(4)
        report_table_id = LABEL.REPORT_TABLE_ID_MAPPING[report_type]
        custom_repoort_res, custom_repoort_resp = self.selenium_object.check_element_clickable_and_click(LABEL.TEMPLATE_ELEMENT_MAPPING[report_template_name].format(report_table_id=report_table_id))
        if not custom_repoort_res:
            raise custom_repoort_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("custom report loaded")

    def report_dispaly_rows_setting(self, number_of_rows_to_display):
        """ Function will select maximum number of rows that can be selectable to download the report
            select option: maxmim number of rows to select
        """
        # find select option on the report
        select_res, select_resp = self.selenium_object.check_element_loaded_status(LABEL.CUSTOM_REPORT_PAGE_SELECT_REPORT_SHOW_ROW_XPATH)
        if not select_res:
            raise select_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Rows List Dropdown element found")
        sleep(5)
        # select number of rows to be displayed on the page
        select_rs, select_resp = self.selenium_object.select_from_drop_down(element=select_resp[SeleniumUtilsConstants.ELEMENT], select_option=str(number_of_rows_to_display))
        if not select_rs:
            raise select_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Page setting done")

    def paginate_and_download_report(self, number_of_rows_to_display, download_folder_path):
        """Function will paginate and download the report untill all the pages are finished.
           select option: maxmim number of rows to select for download
        """

        # find number of rows on the page
        pages_res, pages_resp = self.selenium_object.check_element_loaded_status(INPUT.CUSTOM_REPORT_PAGE_TOTAL_PAGES_XPATH)
        if not pages_res:
            raise pages_res[SeleniumUtilsConstants.EXCEPTION]
        # Find the text that containes total number of rows
        total_rows_details = pages_resp[SeleniumUtilsConstants.ELEMENT].text
        total_rows_list = total_rows_details.split("of")
        total_rows = total_rows_list[1]
        logging.info("total number of rows are {}".format(total_rows))
        # find total number of iteration based on number_of_rows_to_display and total number of rows on report
        number_of_iteration = (int(total_rows) // number_of_rows_to_display) + 1
        current_iteration = 0
        logging.info("Number of iterations {}".format(number_of_iteration))

        iterate = True
        while iterate:
            # download report function to download the report
            self.download_report()
            # After download decrease iteration
            number_of_iteration = number_of_iteration - 1
            current_iteration += 1
            logging.info("Current iteration - {}".format(current_iteration))

            if number_of_iteration == 0:
                iterate = False
                break
            # Find page input element to input the rows number
            page_input_res, page_input_resp = self.selenium_object.check_element_loaded_status(LABEL.CUSTOM_REPORT_PAGE_PAGE_INPUT_XPATH)
            if not page_input_res:
                raise page_input_resp[SeleniumUtilsConstants.EXCEPTION]
            logging.info("Page input found ")
            # Find the current selected row
            current_row = int(page_input_resp[SeleniumUtilsConstants.ELEMENT].get_attribute('value'))
            logging.info("Current page is {}".format(current_row))
            # Find the next row that to be selected on page
            go_to_row = current_row + number_of_rows_to_display
            # clear and send value to page input element: Defines from which row the report to be downloaded
            page_input_element = page_input_resp[SeleniumUtilsConstants.ELEMENT]
            input_res, input_resp = self.selenium_object.send_value_to_input(page_input_element, go_to_row, execute_scriptto_clear_value=False, keyboard_input=KEYINPUT.ENTER)
            logging.info("row selected is {}".format(go_to_row))
            if not input_res:
                raise input_resp[SeleniumUtilsConstants.EXCEPTION]

            # renames the downloaded file to avoid duplicate naming
            suffix_to_add = '_' + str(current_iteration)

            self.rename_latest_downloaded_file(download_folder_path, suffix_to_add)

        logging.info("All pages are downloaded")

    def download_report(self):
        """Function will download the report"""
        logging.info("Downloading..")
        sleep(3)
        export_res, export_resp = self.selenium_object.check_element_clickable_and_click(LABEL.CUSTOM_REPORT_PAGE_EXPORT_XPATH)
        if not export_res:
            raise export_resp[SeleniumUtilsConstants.EXCEPTION]

        download_format_res, download_format_resp = self.selenium_object.check_element_clickable_and_click(LABEL.CUSTOM_REPORT_PAGE_DOWNLOAD_CSV_XPATH)
        if not download_format_res:
            raise download_format_resp[SeleniumUtilsConstants.EXCEPTION]
        self.number_of_downloading_files = self.number_of_downloading_files + 1

    def switch_out_iframe(self):
        """ This function will switch out iframe into default page"""
        res, resp = self.selenium_object.switch_out_of_iframe()
        if not res:
            raise resp[SeleniumUtilsConstants.EXCEPTION]

    def open_google_analytics_account_setting(self):
        """ This function will click and select google analytics settings"""
        settings_res, settings_resp = self.selenium_object.check_element_clickable_and_click(LABEL.CUSTOM_REPORT_PAGE_SETTINGS_XPATH, interactive_mode=True)
        if not settings_res:
            raise settings_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Clicked on google account settings")

    def logout(self):
        """ This function will logout from google analytics"""
        logging.info("logout process started")
        logout_res, logout_resp = self.selenium_object.check_element_clickable_and_click(LABEL.CUSTOM_REPORT_PAGE_LOGOUT_XPATH)
        if not logout_res:
            raise logout_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info("log out done!")

    def check_download_status(self):
        """ This function will check wether download is success and files are present in download folder"""
        logging.info("Checking downloading status")
        download_res, download_resp = self.selenium_object.check_file_downloaded_status(number_of_files_to_be_present=self.number_of_downloading_files)
        if not download_res:
            raise download_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Download completed")

    def select_custom_date_range(self, download_date_range):
        """ Selects a custom date range to Download the report"""
        # Find the start and end date to download the report
        date_res, formated_from_date, formated_to_date = date_range(download_date_range, DATE.WEEKLY_REPORT_INPUT_DATE_FORMAT, consider_current_date_as_end_date=True)
        if not date_res:
            logging.error("Error while finding the date_range")
            raise Exception("Error while finding the date_range")

        # click on the date picker table dropdown
        date_picker_res, date_picker_resp = self.selenium_object.check_element_clickable_and_click(LABEL.CUSTOM_DATE_PICKER_XPATH)
        if not date_picker_res:
            raise date_picker_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Clicked on Date Picker Drop Down")

        # Find start_date element
        start_date_res, start_date_resp = self.selenium_object.check_element_loaded_status(INPUT.REPORT_PAGE_START_DATE_XPATH)
        if not start_date_res:
            raise start_date_resp[SeleniumUtilsConstants.EXCEPTION]
        # Send value to start date
        send_value_res, send_value_resp = self.selenium_object.send_value_to_input(start_date_resp[SeleniumUtilsConstants.ELEMENT], formated_from_date, execute_scriptto_clear_value=False)
        if not send_value_res:
            raise send_value_resp[SeleniumUtilsConstants.EXCEPTION]

        logging.info("start date sent")
        # FInd end date element
        end_date_res, end_date_resp = self.selenium_object.check_element_loaded_status(INPUT.REPORT_PAGE_END_DATE_XPATH)
        if not end_date_res:
            raise end_date_resp[SeleniumUtilsConstants.EXCEPTION]
        # Send value to end date element
        send_value_res, send_value_resp = self.selenium_object.send_value_to_input(end_date_resp[SeleniumUtilsConstants.ELEMENT], formated_to_date, execute_scriptto_clear_value=False)
        if not send_value_res:
            raise send_value_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("end date sent")
        # click on the apply button
        select_res, select_resp = self.selenium_object.check_element_clickable_and_click(LABEL.CUSTOM_DATE_PICKER_APPLY_XPATH)
        if not select_res:
            raise select_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Clicked on Date Range Apply Button")
        sleep(10)

    def rename_latest_downloaded_file(self, download_folder_path, suffix_to_add):
        """Renames the most recent downloaded file to avoid duplicate names"""

        file_name_with_extension = max([os.path.join(download_folder_path, f) for f in os.listdir(download_folder_path)], key=os.path.getctime)

        # Split file name and file extension
        file_name, file_extension = extract_extension_from_filename(file_name_with_extension)

        logging.info(f"filename is {file_name} and extension of file is {file_extension}")

        # New file name to save as
        new_file_name = file_name + suffix_to_add + file_extension

        # Rename existing file with the new file name
        os.rename(os.path.join(download_folder_path, file_name_with_extension), os.path.join(download_folder_path, new_file_name))
