import sys
import os
import json
import logging
# This is to include the other folders paths for python to look in
PROJECT_ROOT = os.path.dirname('/application/')  # NOQA
sys.path.insert(0, os.path.join(PROJECT_ROOT, "source"))  # NOQA

from airflow import DAG
from config.dag_config import ARGUMENTS
from config.yahoo_gemini_config import PATH, PLATFORMTYPE
from dag_helpers.yahoo_gemini_report import YahooGeminiReport
from utils.common.consolidate_and_validate_reports import ConsolidateAndValidateReports
from utils.common.file_read_write import read_json_file
from utils.common.dag_utils import create_dummy_operator, create_python_operator

# DAG WILL BE SCHEDULED AT 2PM IST From Monday to Friday
dag = DAG('yahoo_gemini_verizon_dag',
          default_args=ARGUMENTS.DEFAULT_ARGUMENTS,
          schedule_interval='00 14 * * 1-5', concurrency=1, catchup=False)

yahoo_gemini_object = YahooGeminiReport()

# function to create a parent directory in which all the reports for different account will be uploaded.
create_directory_over_ftp_res = create_python_operator(
    dag,
    'create_yahoo_gemini_directory_on_ftp_task',
    yahoo_gemini_object.create_directory_on_ftp,
    op_kwargs={'platform_type': PLATFORMTYPE.YAHOO_GEMINI_VERIZON})
# creating the task which indicates start and end ponts of download and upload report process
dummy_task_start = create_dummy_operator(dag, 'start_job_task')
dummy_task_end = create_dummy_operator(dag, 'end_job_task')

# consolidate and validate class intialization
consolidate_and_validate_object = ConsolidateAndValidateReports()

# Create and upload consolidated reports to s3
create_consolidated_report = create_python_operator(
    dag,
    'create_yahoo_verizon_daily_consolidated_report',
    consolidate_and_validate_object.download_individual_report_and_consolidate_for_platform,
    op_kwargs={'platform': PLATFORMTYPE.YAHOO_GEMINI_VERIZON},
    trigger_rule_value='all_done')

# Download and Send consolidated report through email
email_consolidated_report = create_python_operator(
    dag,
    'email_yahoo_verizon_daily_consolidated_report',
    consolidate_and_validate_object.download_consolidated_report_and_send_email,
    op_kwargs={'platform': PLATFORMTYPE.YAHOO_GEMINI_VERIZON})


# Read input from config file
input_config_list = read_json_file(file_path=PATH.YAHOO_VERIZON_JSON_FILE_PATH)

task_list = []

# creating the function to download and upload report task for different account in parallel
try:
    for each in input_config_list:
        download_pinterest_report_task = create_python_operator(
            dag,
            f"download_yahoo_gemini_verizon_report_task_{each['report_name']}",
            yahoo_gemini_object.download_combine_and_upload_reports,
            op_kwargs={'report_config': each, 'platform_type': PLATFORMTYPE.YAHOO_GEMINI_VERIZON},
            provide_context=True
        )
        task_list.append(download_pinterest_report_task)

except Exception as e:
    logging.error("Error while executing task {}".format(e))
    raise e

dummy_task_start >> create_directory_over_ftp_res >> task_list >> create_consolidated_report >> email_consolidated_report >> dummy_task_end
