import sys
import os
import json
import logging
# This is to include the other folders paths for python to look in
PROJECT_ROOT = os.path.dirname('/application/')  # NOQA
sys.path.insert(0, os.path.join(PROJECT_ROOT, "source"))  # NOQA

from airflow import DAG
from airflow.models import Variable
from config.pinterest_config import PATH
from config.dag_config import ARGUMENTS
from config.report_status_config import PLATFORM
from dag_helpers.pinterest_report import PinterestReport
from utils.common.consolidate_and_validate_reports import ConsolidateAndValidateReports
from utils.common.dag_utils import create_dummy_operator, create_python_operator
from utils.common.file_read_write import read_json_file

# DAG WILL BE SCHEDULED AT 4PM IST From Monday to Friday
dag = DAG('pinterest_dag',
          default_args=ARGUMENTS.DEFAULT_ARGUMENTS,
          schedule_interval='00 16 * * 1-5', concurrency=2, catchup=False)

# class initialization PinterestReport
pinterest_object = PinterestReport()

# function to create a parent directory in which all the reports for different account will be uploaded.
create_directory_over_ftp_res = create_python_operator(
    dag,
    'create_pinterest_directory_on_ftp_task',
    pinterest_object.create_directory_on_ftp)

# creating the task which indicates start and end ponts of download and upload report process
dummy_task_start = create_dummy_operator(dag, 'start_job_task')
dummy_task_end = create_dummy_operator(dag, 'end_job_task')

# consolidate and validate class intialization
consolidate_and_validate_object = ConsolidateAndValidateReports()

# Create and upload consolidated reports to s3
create_consolidated_report = create_python_operator(
    dag,
    'create_pinterest_consolidated_report',
    consolidate_and_validate_object.download_individual_report_and_consolidate_for_platform,
    op_kwargs={'platform': PLATFORM.PINTEREST},
    trigger_rule_value='all_done')

# Download and Send consolidated report through email
email_consolidated_report = create_python_operator(
    dag,
    'email_pinterest_consolidated_report',
    consolidate_and_validate_object.download_consolidated_report_and_send_email,
    op_kwargs={'platform': PLATFORM.PINTEREST})

# Check for input Variable
# Get the configuration to download the reports from the Variable if exist
input_config_list = Variable.get("pinterest_config_file", None)
if input_config_list:
    input_config_list = json.loads(input_config_list)
else:
    # Read the configuration file from specified path(location)
    input_config_list = read_json_file(file_path=PATH.JSON_FILE_PATH)

task_list = []

# creating the function to download and upload report task for different account in parallel
try:
    for each in input_config_list:
        download_pinterest_report_task = create_python_operator(
            dag,
            "download_pinterest_report_task",
            pinterest_object.download_combine_and_upload_reports,
            op_kwargs={'report_config': each, 'platform': PLATFORM.PINTEREST},
            provide_context=True
        )
        task_list.append(download_pinterest_report_task)

except Exception as e:
    logging.error("Error while executing task {}".format(e))
    raise e

dummy_task_start >> create_directory_over_ftp_res >> task_list >> create_consolidated_report >> email_consolidated_report >> dummy_task_end
