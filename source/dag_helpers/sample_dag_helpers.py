from selenium import webdriver


def sample_dag_function(*args, **kwargs):
    sample_variable = kwargs['sample_variable']
    each_input_value = kwargs['each_input_value']

    print(sample_variable)
    print(each_input_value)

    chrome_options = webdriver.ChromeOptions()

    # working with selenium docker
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    # prefs = {'download.default_directory': '/application/source/dag_helpers/downloads'}
    # chrome_options.add_experimental_option('prefs', prefs)
    # chrome_options.add_argument("download.default_directory=/application/source/dag_helpers/downloads")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")

    driver = webdriver.Chrome(options=chrome_options, executable_path='/browser_drivers/chromedriver')
    driver.get('URL')
    print('Site loaded')
    driver.quit()
