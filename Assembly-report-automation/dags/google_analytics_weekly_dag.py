import sys
import os
import logging

# This is to include the other folders paths for python to look in
PROJECT_ROOT = os.path.dirname('/application/')  # NOQA
sys.path.insert(0, os.path.join(PROJECT_ROOT, "source"))  # NOQA

from airflow import DAG
from config.google_analytics_config import PATH
from config.dag_config import ARGUMENTS
from config.report_status_config import PLATFORM
from dag_helpers.google_analytics_report import GoogleAnalyticsReport
from utils.common.consolidate_and_validate_reports import ConsolidateAndValidateReports
from utils.common.file_read_write import read_json_file
from utils.common.dag_utils import create_dummy_operator, create_python_operator

# DAG WILL BE SCHEDULED AT 6PM IST on every Monday
dag = DAG('google_analytics_weekly_dag',
          default_args=ARGUMENTS.DEFAULT_ARGUMENTS,
          schedule_interval='00 18 * * 1', concurrency=2, catchup=False)

# start and end dummy operator task to indicate start and end process.
dummy_task_start = create_dummy_operator(dag, 'start_google_job_task')
dummy_task_end = create_dummy_operator(dag, 'end_google_job_task')

# consolidate and validate class intialization
consolidate_and_validate_object = ConsolidateAndValidateReports()

# Create and upload consolidated reports to s3
create_consolidated_report = create_python_operator(
    dag,
    'create_google_consolidated_report',
    consolidate_and_validate_object.download_individual_report_and_consolidate_for_platform,
    op_kwargs={'platform': PLATFORM.GOOGLE_ANALYTICS_WEEKLY},
    trigger_rule_value='all_done')

# Download and Send consolidated report through email
email_consolidated_report = create_python_operator(
    dag,
    'email_google_consolidated_report',
    consolidate_and_validate_object.download_consolidated_report_and_send_email,
    op_kwargs={'platform': PLATFORM.GOOGLE_ANALYTICS_WEEKLY})

# Read the configuration file
input_config_list = read_json_file(file_path=PATH.WEEKLY_JSON_FILE_PATH)

# google analytics class intializtion
google_analytics_report_object = GoogleAnalyticsReport()

# function to create a parent directory in which all the reports for different account will be uploaded.
create_directory_over_ftp_res = create_python_operator(
    dag,
    'create_google_directory_on_ftp_task',
    google_analytics_report_object.create_directory_on_ftp,
    op_kwargs={'parent_ftp_directory': PATH.FTP_PARENT_DIRECTORY_TO_UPLOAD_FILES_INNOVAGE},
    provide_context=True
)

# Call the download and upload report for different accounts in parellel
task_list = []
try:
    for each in input_config_list:
        # function call to create python operator
        download_google_analytics_report_task = create_python_operator(
            dag,
            f"download_google_analytics_template_{each['report_template_name']}",
            google_analytics_report_object.download_combine_and_upload_reports,
            op_kwargs={'report_config': each, 'platform': PLATFORM.GOOGLE_ANALYTICS_WEEKLY},
            provide_context=True
        )

        task_list.append(download_google_analytics_report_task)

except Exception as e:
    logging.error("Error while executing task {}".format(e))
    raise e

# indicates order of execution
dummy_task_start >> create_directory_over_ftp_res >> task_list >> create_consolidated_report >> email_consolidated_report >> dummy_task_end
