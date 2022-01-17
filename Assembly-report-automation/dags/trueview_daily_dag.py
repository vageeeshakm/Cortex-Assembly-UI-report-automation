import sys
import os
import logging
# This is to include the other folders paths for python to look in
PROJECT_ROOT = os.path.dirname('/application/')  # NOQA
sys.path.insert(0, os.path.join(PROJECT_ROOT, "source"))  # NOQA
from airflow import DAG
from config.trueview_config import PATH
from config.dag_config import ARGUMENTS
from config.report_status_config import PLATFORM
from dag_helpers.trueview_report import TrueviewReport
from utils.common.consolidate_and_validate_reports import ConsolidateAndValidateReports
from utils.common.file_read_write import read_json_file
from utils.common.dag_utils import create_dummy_operator, create_python_operator

dag = DAG('trueview_daily_dag',
          default_args=ARGUMENTS.DEFAULT_ARGUMENTS,
          schedule_interval='00 15 * * 1-5', concurrency=2, catchup=False)

# start and end dummy operator task to indicate start and end process.
dummy_task_start = create_dummy_operator(dag, 'start_trueview_daily_task')
dummy_task_end = create_dummy_operator(dag, 'end_trueview_daily_task')

# consolidate and validate class intialization
consolidate_and_validate_object = ConsolidateAndValidateReports()

# Create and upload consolidated reports to s3
create_consolidated_report = create_python_operator(
    dag,
    'create_trueview_consolidated_report',
    consolidate_and_validate_object.download_individual_report_and_consolidate_for_platform,
    op_kwargs={'platform': PLATFORM.TRUEVIEW},
    trigger_rule_value='all_done')

# Download and Send consolidated report through email
email_consolidated_report = create_python_operator(
    dag,
    'email_trueview_consolidated_report',
    consolidate_and_validate_object.download_consolidated_report_and_send_email,
    op_kwargs={'platform': PLATFORM.TRUEVIEW})

# Read the configuration file
input_config_list = read_json_file(file_path=PATH.JSON_FILE_PATH)

# Trueview class intializtion
trueview_report_object = TrueviewReport()

# function to create a parent directory in which all the reports for different account will be uploaded.
create_directory_over_ftp_res = create_python_operator(
    dag,
    'create_trueview_directory_on_ftp',
    trueview_report_object.create_directory_on_ftp,
    provide_context=True
)

# Call the download and upload report for different accounts in parellel
task_list = []
try:
    for each in input_config_list:
        # function call to create python operator
        download_trueview_report_task = create_python_operator(
            dag,
            f"download_trueview_report_task_{each['report_type']}",
            trueview_report_object.download_segregate_and_upload_reports,
            op_kwargs={'report_config': each, 'platform': PLATFORM.TRUEVIEW},
            provide_context=True
        )

        task_list.append(download_trueview_report_task)

except Exception as e:
    logging.error("Error while executing task {}".format(e))
    raise e
# indicates order of execution
dummy_task_start >> create_directory_over_ftp_res >> task_list >> create_consolidated_report >> email_consolidated_report >> dummy_task_end
