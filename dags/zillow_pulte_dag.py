import logging
import os
import sys

# This is to include the other folders paths for python to look in
PROJECT_ROOT = os.path.dirname('/application/')  # NOQA
sys.path.insert(0, os.path.join(PROJECT_ROOT, "source"))  # NOQA

from airflow import DAG
from config.zillow_pulte_config import PATH
from config.dag_config import ARGUMENTS
from config.report_status_config import PLATFORM
from dag_helpers.zillow_pulte_report import ZillowPulteReport
from utils.common.consolidate_and_validate_reports import ConsolidateAndValidateReports
from utils.common.dag_utils import create_dummy_operator, create_python_operator
from utils.common.file_read_write import read_json_file

# DAG intialization
# Job will be scheduled at 7PM PM IST
dag = DAG('zillow_pulte_dag',
          default_args=ARGUMENTS.DEFAULT_ARGUMENTS,
          schedule_interval='00 19 * * *', catchup=False)

# start and end dummy operator task to indicate start and end process.
dummy_task_start = create_dummy_operator(dag, 'start_zillow_pulte_task')
dummy_task_end = create_dummy_operator(dag, 'end_zillow_pulte_task')

# consolidate and validate class intialization
consolidate_and_validate_object = ConsolidateAndValidateReports()

# Create and upload consolidated reports to s3
create_consolidated_report = create_python_operator(
    dag,
    "create_consolidated_report",
    consolidate_and_validate_object.download_individual_report_and_consolidate_for_platform,
    op_kwargs={'platform': PLATFORM.ZILLOW_PULTE},
    trigger_rule_value="all_done")

# Download and Send consolidated report through email
email_consolidated_report = create_python_operator(
    dag,
    "email_consolidated_report",
    consolidate_and_validate_object.download_consolidated_report_and_send_email,
    op_kwargs={'platform': PLATFORM.ZILLOW_PULTE})

# Read the configuration file
input_config_list = read_json_file(file_path=PATH.JSON_FILE_PATH)

# ZillowPulte class intialization
zillow_pulte_report_object = ZillowPulteReport()

task_list = []
try:
    for each in input_config_list:
        # function call to create python operator
        zillow_pulte_task = create_python_operator(
            dag,
            "download_zillow_report_task",
            zillow_pulte_report_object.download_and_upload_report,
            op_kwargs={'report_config': each, 'platform': PLATFORM.ZILLOW_PULTE},
            provide_context=True)

        task_list.append(zillow_pulte_task)

except Exception as e:
    logging.error("Error while executing task {}".format(e))
    raise e

# indicates order of execution
dummy_task_start >> task_list >> create_consolidated_report >> email_consolidated_report >> dummy_task_end
