import os
import logging

from config.ttd_config import BUTTON
from utils.selenium.selenium_utils import SeleniumUtilsConstants

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


class TtdUtils:
    """All TTD related functionalities."""

    def __init__(self, selenium_object):
        # selenium_object object used for loading pages and performing actions and events
        self.selenium_object = selenium_object

    def download_report(self, report_download_link):
        """
        this function will download the report using link
        """
        self.selenium_object.get_page(report_download_link)
        logging.info("page loaded")

        download_res, download_resp = self.selenium_object.check_element_clickable_and_click(BUTTON.DOWNLOAD_REPORT_XPATH)
        if download_res:
            logging.info("report download button cicked")
        else:
            logging.info("download button class name changed or security-us.mimecast.com page not loaded")

    def check_download_status(self):
        """ This function will check wether download is success and files are present in download folder"""
        logging.info("Checking downloading status")
        download_res, download_resp = self.selenium_object.check_file_downloaded_status()
        if not download_res:
            raise download_resp[SeleniumUtilsConstants.EXCEPTION]
        logging.info("Download completed")
