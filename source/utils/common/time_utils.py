from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta


def date_range(download_duration, date_format, current_date=None, consider_current_date_as_end_date=False):
    """ Function that sater and end date for specified duration
        INPUT:
            download_duration: specifies duration to find start and end date
            date_format: output format of date
            current_date: current date to consider, By default current date is today's date
            consider_current_date_as_end_date - Boolean value to specify todays date as end date
        output:
         IF SUCCESS:
            Boolean value TRUE
            start_date(date)
            end_date(date)
    """
    try:
        download_duration_list = download_duration.split(":")
        indicator = download_duration_list[0]
        dayspart = int(download_duration_list[1])

        # Check if current date is specified. If not today date will be the current date
        if current_date:
            today_date = datetime.strptime(current_date, date_format).date()
        else:
            today_date = date.today()

        # Consider todays date as end date if consider_current_date_as_end_date is True
        if consider_current_date_as_end_date:
            end_date = today_date
        else:
            end_date = today_date - timedelta(1)

        if indicator == 'D':
            start_date = today_date - timedelta(dayspart)
        elif indicator == 'M':
            relative_date = today_date - relativedelta(months=dayspart)
            start_date = relative_date.replace(day=1)
        elif indicator == 'MTD':
            start_date = today_date.today().replace(day=1)
        else:
            raise Exception("Error parsing the duration input. Please provide valid input")
        # return strat and end date
        return True, start_date.strftime(date_format), end_date.strftime(date_format)

    except Exception as e:
        print("Exception--{}--occured in download_date_range function".format(e))
        raise e


def get_current_datetime():
    """ Function to get current date and time"""
    # Timezone on server is IST
    # Timezone in local is UTC
    return datetime.now()


def get_todays_date_string(date_format='%Y%m%d'):
    """ Function to get todays date in string format
        INPUT:
            date_format - expected date format to return
        OUTPUT:
            today`s date in specified input format
    """
    current_date_time = get_current_datetime()
    return current_date_time.strftime(date_format)
