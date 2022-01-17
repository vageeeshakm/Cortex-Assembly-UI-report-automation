import logging
import glob
import os
from time import sleep
from selenium import webdriver
from pathlib import Path
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    ElementNotInteractableException,
    TimeoutException,
    ElementClickInterceptedException,
    ElementNotSelectableException,
    InsecureCertificateException,
    InvalidSelectorException,
    NoSuchAttributeException,
    WebDriverException
)
from selenium.webdriver.support.ui import Select

from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.webdriver import FirefoxProfile

from selenium.webdriver.common.keys import Keys

from config.selenium_config import (BROWSERTYPE, PATH, TIMINGS, SELECTORTYPE, KEYINPUT)
from utils.selenium.selenium_randomize_utils import SeleniumRandomUtils
from utils.selenium.selenium_events_responses import SeleniumUtilsConstants, SeleniumEventsResponses


class SeleniumUtils(SeleniumRandomUtils):
    """ SeleniumUtils class"""

    def __init__(self, browser=BROWSERTYPE.CHROME, download_path=PATH.download_path):
        """ initialising driver with default options"""
        try:
            self.download_path = download_path
            self.browser = browser
            # exception object for unknown browser type
            browser_exception_message = "Unknown browser type"
            if self.browser == BROWSERTYPE.FIREFOX:
                logging.info("Adding firefox browser preferences")
                self.options = FirefoxOptions()
                # Profile arguments for firefox browser
                self.profile = FirefoxProfile()
                self.profile.set_preference("browser.download.panel.shown", False)
                self.profile.set_preference("browser.helperApps.neverAsk.openFile", "text/csv,application/vnd.ms-excel,attachment/csv")
                self.profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv,application/vnd.ms-excel,attachment/csv")
                self.profile.set_preference("browser.download.folderList", 2)
                self.profile.set_preference("browser.download.dir", self.download_path)
            elif self.browser == BROWSERTYPE.CHROME:
                self.options = ChromeOptions()
                # Browser preferences
                self.prefs = {'download.default_directory': self.download_path}
            else:
                # Raise exception
                raise Exception(browser_exception_message)

            # options for browser
            self.options.add_argument("--start-maximized")
            self.options.add_argument('window-size=2560,1440')
            self.options.add_argument('--no-sandbox')
            self.options.add_argument("--headless")
            self.options.add_argument('--disable-gpu')
            self.options.add_argument('--disable-dev-shm-usage')

            if self.browser == BROWSERTYPE.FIREFOX:
                logging.info("firefox browser started")
                self.driver = webdriver.Firefox(options=self.options, firefox_profile=self.profile, executable_path=PATH.firefox_driver_path)
            elif self.browser == BROWSERTYPE.CHROME:
                logging.info("chrome browser started")
                self.options.add_experimental_option('prefs', self.prefs)
                self.driver = webdriver.Chrome(chrome_options=self.options, executable_path=PATH.chrome_driver_path)
            else:
                raise Exception(browser_exception_message)
            logging.info("driver initialized")
            SeleniumRandomUtils.__init__(self, self.driver)

        except (WebDriverException) as e:
            logging.error("Exception - {} - occured while initialising the driver".format(e))
            raise e
        except Exception as e:
            logging.error("Exception {}".format(e))
            raise e

    def get_page(self, url, include_waiting_time=False):
        """ Finction that loads the page with given url
            INPUT: url that be loaded on browser
                   include_waiting_time: if True, the interval time is added
            OUTPUT: load the page with specified url
         """
        try:
            if include_waiting_time:
                interval_time = TIMINGS.INTERVAL_TIME
                sleep(interval_time)
                self.driver.get(url)
                sleep(interval_time)
            else:
                self.driver.get(url)
        except(TimeoutException, InsecureCertificateException) as e:
            logging.error("Exception occured while getting the page")
            raise e
        except Exception as e:
            logging.error("Exception occured while loading page", e)
            raise e

    def check_element_loaded_status(self, identifier, select_type=SELECTORTYPE.XPATH, log_error=False):
        """ Function that wait till element appear on page and return the corresponding object using identifier.
            INPUT: identifier: identifier to find the element on page
                   select_type: selector type
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: find and return the element object for specified xpath
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and {dict} with Exception
                          return format is in error_function
        """
        timer = True
        maximum_waiting_time = TIMINGS.MAXIMUM_TIME
        waiting_time = 0
        interval_time = TIMINGS.INTERVAL_TIME
        error_message = ''
        while timer:
            try:
                if select_type == SELECTORTYPE.XPATH:
                    xpath_res, xpath_resp = self.find_element_by_xpath(identifier)
                    if xpath_res:
                        timer = False
                        return xpath_res, xpath_resp
                    else:
                        error_message = xpath_resp
                elif select_type == SELECTORTYPE.CLASS:
                    class_res, class_resp = self.find_element_by_class_name(identifier)
                    if class_res:
                        timer = False
                        return class_res, class_resp
                    else:
                        error_message = class_resp
                elif select_type == SELECTORTYPE.ID:
                    id_res, id_resp = self.find_element_by_id(identifier)
                    if id_res:
                        timer = False
                        return id_res, id_resp
                    else:
                        error_message = id_resp
                else:
                    timer = False
                    identifier_exception_obj = Exception('Unknown select type specified in check_element_loaded_status function')
                    return SeleniumEventsResponses.error_function(identifier_exception_obj)
                logging.error("Waiting to load")
                sleep(interval_time)
                waiting_time = waiting_time + interval_time
                if waiting_time > maximum_waiting_time:
                    logging.error("Element not found.")
                    timer = False
                    return SeleniumEventsResponses.error_function(error_message[SeleniumUtilsConstants.EXCEPTION])
            except Exception as e:
                timer = False
                if log_error:
                    logging.error("Exception- {} ".format(e))
                return SeleniumEventsResponses.error_function(e)

    def check_element_clickable_and_click(self, identifier, select_type=SELECTORTYPE.XPATH, interactive_mode=False, log_error=False):
        """ Function that waits till element is loaded and clickable on web page, clicks on specified element on the page.
            INPUT: identifier: identifier to find the element on page
                   select_type: selector type
                   interactive_mode: Boolean specifies to activate interactive mode ; like moving the cursor over element
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: returns Boolean value True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        # Find the elemt on the web page
        find_res, find_resp = self.check_element_loaded_status(identifier, select_type, log_error)
        # If element not found return
        if not find_res:
            return find_res, find_resp
        timer = True
        maximum_waiting_time = TIMINGS.MAXIMUM_TIME
        waiting_time = 0
        interval_time = TIMINGS.INTERVAL_TIME
        while timer:
            try:
                click_res, click_resp = self.click_element(find_resp[SeleniumUtilsConstants.ELEMENT], interactive_mode=interactive_mode)
                if click_res:
                    return click_res, click_resp
                sleep(interval_time)
                waiting_time = waiting_time + interval_time
                if waiting_time > maximum_waiting_time:
                    logging.error("Element not found")
                    timer = False
                    return SeleniumEventsResponses.error_function(click_resp[SeleniumUtilsConstants.EXCEPTION])
            except Exception as e:
                timer = False
                if log_error:
                    logging.error("Exception- {} ".format(e))
                return SeleniumEventsResponses.error_function(e)

    def find_element_by_xpath(self, xpath, log_error=False):
        """ Function that finds the elemnt and return the corresponding object using given xpath
            INPUT: xpath: xpath to find the element on page
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: find and return the element object for specified xpath
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and {dict} with Exception
                          return format is in error_function
        """
        try:
            element = self.driver.find_element_by_xpath(xpath)
            data = {
                SeleniumUtilsConstants.ELEMENT: element
            }
            return SeleniumEventsResponses.success_function(data=data)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            if log_error:
                logging.error("Exception - {} - occured while finding element by xpath".format(e))
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("GeneralException", e)
            return SeleniumEventsResponses.error_function(e)

    def find_elements_by_xpath(self, xpath, log_error=False):
        """ Function that finds the list of elemnts and return the corresponding objects using given xpath
            INPUT: xpath: xpath to find the element on page
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: find and return the list of elements object for specified xpath
                            return type: list
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        try:
            elements = self.driver.find_elements_by_xpath(xpath)
            data = {
                SeleniumUtilsConstants.ELEMENT: elements
            }
            return SeleniumEventsResponses.success_function(data=data)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            if log_error:
                logging.error("Exception - {} - occured while finding element by xpath".format(e))
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("GeneralException", e)
            return SeleniumEventsResponses.error_function(e)

    def find_element_by_id(self, id_name, log_error=False):
        """ Function that finds the elemnt and return the corresponding object using given specified id_name
            INPUT: id_name: id_name to find the element on page
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: find and return the element object for specified id_name
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        try:
            element = self.driver.find_element_by_id(id_name)
            data = {
                SeleniumUtilsConstants.ELEMENT: element
            }
            return SeleniumEventsResponses.success_function(data=data)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            if log_error:
                logging.error("Exception - {} - occured while finding element by id".format(e))
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("Exception", e)
            return SeleniumEventsResponses.error_function(e)

    def find_element_by_class_name(self, class_name, log_error=False):
        """ Function that finds the elemnt and return the corresponding object using specified class_name
            INPUT: class_name: class_name to find the element on page
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: find and return the element object for specified class_name
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                              return format is in error_function

        """
        try:
            element = self.driver.find_element_by_class_name(class_name)
            data = {
                SeleniumUtilsConstants.ELEMENT: element
            }
            return SeleniumEventsResponses.success_function(data=data)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            if log_error:
                logging.error("Exception - {} - occured while finding element by class".format(e))
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("Exception", e)
            return SeleniumEventsResponses.error_function(e)

    def click_element(self, element, log_error=False, interactive_mode=False):
        """ Function that clicks on specified element on the page.
            INPUT: element: element that to be clicked
                   log_error: Boolean value that specifies to include/exclude logging message
                   interactive_mode: Boolean specifies to activate interactive mode ; like moving the cursor over element
            OUTPUT:
                IF SUCCESS: returns Boolean value True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        if interactive_mode:
            self.element_pointer_movement(element)
        try:
            element.click()
            return SeleniumEventsResponses.success_function()
        except (ElementClickInterceptedException, ElementNotInteractableException) as e:
            if log_error:
                logging.error("Exception - {} - occured while clicking the element".format(e))
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("Exception", e)
            return SeleniumEventsResponses.error_function(e)

    def select_from_drop_down(self, element, select_option, log_error=False):
        """ Function that select specified option from drop_down.
            INPUT: element: drop_down element
                   select_option: option to be selected in drop_down
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: returns True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        try:
            select_object = Select(element)
            select_object.select_by_visible_text(select_option)
            return SeleniumEventsResponses.success_function()
        except (ElementNotSelectableException, InvalidSelectorException, ElementNotInteractableException, NoSuchAttributeException) as e:
            if log_error:
                logging.error("Exception - {} - occured while selecting the element from drop_down".format(e))
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("Exception {}".format(e))
            return SeleniumEventsResponses.error_function(e)

    def send_value_to_input(self, element, input_string, click_and_clear_value=True, execute_scriptto_clear_value=True, keyboard_input=KEYINPUT.TAB, log_error=False, interactive_mode=False):
        """ Function that sneds value to the specidied element.
            INPUT: element: element(object) to which value should sent
                   input_string: string value that to be sent for input element
                   log_error: Boolean value that specifies to include/exclude logging message
                   interactive_mode: Boolean value to perform random mouse and click interactions in the process.
                                     this function works normally if False is passed as well.
            OUTPUT:
                IF SUCCESS: returns True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        if interactive_mode:
            self.generate_random_curve()
            self.element_pointer_movement(element)
        try:
            if keyboard_input == KEYINPUT.TAB:
                key_to_press = Keys.TAB
            if keyboard_input == KEYINPUT.ENTER:
                key_to_press = Keys.ENTER

            # click and clear values in the input box
            if click_and_clear_value:
                result, resp = self.click_element(element)
                if not result:
                    return result, resp
                result, resp = self.clear_content(element)
                if not result:
                    return result, resp
            # Execute script to clear all default values in the input box
            if execute_scriptto_clear_value:
                result, resp = self.click_element(element)
                if not result:
                    return result, resp
                result, resp = self.execute_script("arguments[0].value = ''", element)
                if not result:
                    return result, resp
                result, resp = self.click_element(element)
                if not result:
                    return result, resp
            if interactive_mode:
                self.randomize_input_values(element, input_string, key_to_press)
            else:
                element.send_keys(input_string)
                element.send_keys(key_to_press)
            return SeleniumEventsResponses.success_function()
        except (ElementNotInteractableException) as e:
            if log_error:
                logging.error("Exception - {} - occured while sending keys to input element", e)
            return SeleniumEventsResponses.error_function(e)
        except Exception as e:
            if log_error:
                logging.error("General Exception occured while sending keys to input element", e)
            return SeleniumEventsResponses.error_function(e)

    def execute_script(self, script, argument=None, log_error=False):
        """Function that executes JavaScript in the current window/frame
            INPUT: script: script to be executed on current window/frame
                   argument: argument to the script, if any
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: returns True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        try:
            if argument:
                self.driver.execute_script(script, argument)
            else:
                self.driver.execute_script(script)
            return SeleniumEventsResponses.success_function()
        except Exception as e:
            if log_error:
                logging.error("Error -{}- while exceuting the script".format(e))
            return SeleniumEventsResponses.error_function(e)

    def create_tab(self):
        """Function that creates new tab and returns True if success"""
        return(self.execute_script("window.open('');"))

    def switch_into_iframe(self, identifier, select_type=SELECTORTYPE.XPATH, log_error=False):
        """Function to switch into iframe
            INPUT:
                identifier: identifier to find the element on page
                select_type: selector type
                log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT:
                IF SUCCESS: returns True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        try:
            # Find the elemt on the web page
            find_res, find_resp = self.check_element_loaded_status(identifier, select_type, log_error)
            # If element not found return
            if not find_res:
                return find_res, find_resp
            # If no error then switch into iframe
            self.driver.switch_to.frame(find_resp[SeleniumUtilsConstants.ELEMENT])
            return SeleniumEventsResponses.success_function()
        except Exception as e:
            if log_error:
                logging.error("Error -{}- while switcing into iframe".format(e))
            return SeleniumEventsResponses.error_function(e)

    def switch_out_of_iframe(self):
        """Function to switch out of iframe"""
        try:
            self.driver.switch_to.default_content()
            return SeleniumEventsResponses.success_function()
        except Exception as e:
            logging.info("Error--{}--occured while exiting iframe".format(e))
            return SeleniumEventsResponses.error_function(e)

    def switch_tab(self, log_error=False):
        """Function to switch between the tabs
            INPUT: log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT: IF SUCCESS: returns True
                                return format is in sucess_function
                    IF ERROR: returns Boolean value False and dict with Exception
                              return format is in error_function
        """
        try:
            self.driver.switch_to.window(self.driver.window_handles[1])
            return SeleniumEventsResponses.success_function()
        except Exception as e:
            if log_error:
                logging.error("Error -{}- while switching the tab".format(e))
            return SeleniumEventsResponses.error_function(e)

    def clear_content(self, element, log_error=False):
        """Function to clear the content over the input element on page
            INPUT: element: input element(object)
                   log_error: Boolean value that specifies to include/exclude logging message
            OUTPUT: IF SUCCESS: returns True
                                return format is in sucess_function
                    IF ERROR: returns Boolean value False and dict with Exception
                              return format is in error_function
        """
        try:
            element.clear()
            return SeleniumEventsResponses.success_function()
        except Exception as e:
            if log_error:
                logging.error("Error -{}- while clearing the content".format(e))
            return SeleniumEventsResponses.error_function(e)

    def check_file_downloaded_status(self, number_of_files_to_be_present=1):
        """ Function to check file downloaded in the download folder
            INPUT:
                number_of_files_to_be_present - Number of files to present in the specified download path
            OUTPUT:
                IF SUCCESS: returns True
                            return format is in sucess_function
                IF ERROR: returns Boolean value False and dict with Exception
                          return format is in error_function
        """
        try:
            timer = True
            waiting_time = 1
            maximum_waiting_time_to_complete_download = TIMINGS.MAXIMUM_WAITING_TIME_TO_COMPLETE_DOWNLOAD
            maximum_waiting_time_to_start_download = TIMINGS.MAXIMUM_WAITING_TIME_TO_START_DOWNLOAD
            interval_time = TIMINGS.INTERVAL_TIME
            while timer:
                # Check for files in downloading status till maximum time exceeds
                logging.info("Downloading is in progress")
                if self.browser == BROWSERTYPE.CHROME:
                    # For chrome browser check for this type of file
                    file_in_downloading_status = glob.glob(os.path.join(self.download_path, "*.crdownload"))
                elif self.browser == BROWSERTYPE.FIREFOX:
                    # For Firefox browser check for this type of file
                    file_in_downloading_status = glob.glob(os.path.join(self.download_path, "*.part"))
                else:
                    browser_exp_object = Exception('Unknown browser type')
                    return SeleniumEventsResponses.error_function(browser_exp_object)
                # If download already started and in progress then no need to wait to start download
                if len(file_in_downloading_status) != 0:
                    maximum_waiting_time_to_start_download = 0

                # If no files in downloading status and waiting time to start download exceeds
                if len(file_in_downloading_status) == 0 and waiting_time > maximum_waiting_time_to_start_download:
                    logging.info("No files are in downloading state")
                    logging.info("waiting time to check download status {}".format(waiting_time))
                    timer = False
                    # Check for all types of files in downlaod folder
                    downloaded_files = glob.glob(os.path.join(self.download_path, "*.*"))
                    # When no files is in downloading state
                    if len(downloaded_files) == number_of_files_to_be_present:
                        return SeleniumEventsResponses.success_function()
                    else:
                        logging.info('files downloaded')
                        for each in downloaded_files:
                            logging.info(each)
                        logging.info('number_of_files_to_be_present - ' + str(number_of_files_to_be_present))
                        # When no files in download state and also required file not in download folder
                        exp_object = Exception('File not downloaded')
                        return SeleniumEventsResponses.error_function(exp_object)

                # downloading is in progres
                sleep(interval_time)
                waiting_time = waiting_time + interval_time
                # When waiting time to complete download exceeds
                if waiting_time > maximum_waiting_time_to_complete_download:
                    logging.error("Maximum waiting time exceeded")
                    # stop the timer
                    timer = False
                    timeout_exp_obj = Exception('TimeoutException while checking downloading status of files')
                    return SeleniumEventsResponses.error_function(timeout_exp_obj)
        except Exception as e:
            logging.error("Error {} occured while downloading the report".format(e))
            return SeleniumEventsResponses.error_function(e)

    def get_current_page_url(self):
        """ Function that returns the current page url"""
        try:
            current_url = self.driver.current_url
            return current_url
        except Exception as e:
            logging.error("Exception occured while getting the current page url", e)
            raise e

    def exit(self):
        """Function to exit already opened window by quiting the driver"""
        self.driver.quit()
