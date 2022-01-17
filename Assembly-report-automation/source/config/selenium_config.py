from config.file_config import PATH as file_path


class BROWSERTYPE:
    """Browser type constants """
    FIREFOX = 'firefox'
    CHROME = 'chrome'


class PATH:
    """ Driver path constants"""
    chrome_driver_path = '/browser_drivers/chromedriver'
    firefox_driver_path = '/browser_drivers/geckodriver'
    download_path = file_path.TEMP_DIRECTORY_PATH + '/'


class TIMINGS:
    """Waiting time constant"""
    MAXIMUM_TIME = 40
    INTERVAL_TIME = 3
    MAXIMUM_WAITING_TIME_TO_START_DOWNLOAD = 25
    MAXIMUM_WAITING_TIME_TO_COMPLETE_DOWNLOAD = 100


class SELECTORTYPE:
    """SELECT ELEMENT BY TYPE CAONTANTS"""
    XPATH = 'xpath'
    ID = 'id'
    CLASS = 'class'


class KEYINPUT:
    """KEYINPUT ELEMENT BY TYPE CAONTANTS"""
    TAB = 'tab'
    ENTER = 'enter'
