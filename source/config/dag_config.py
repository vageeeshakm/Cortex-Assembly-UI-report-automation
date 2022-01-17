import os
from datetime import datetime, timedelta


class ARGUMENTS:
    """ DAG AARGUMENTS class"""
    # default arguments for DAG
    DEFAULT_ARGUMENTS = {
        "owner": "airflow",
        'email': os.environ['EMAIL_LIST'].strip('][').split(','),
        'email_on_failure': True,
        "depends_on_past": False,
        "start_date": datetime(2020, 5, 27, 0, 0, 0, 0),  # system time
        'retries': 2,  # Number of times task task should retry if it fails
        'retry_delay': timedelta(minutes=1)   # Time delay for task retry , here it is one minute
    }
