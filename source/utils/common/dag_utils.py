from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def create_dummy_operator(dag, task_id):
    """ create a task with dummy operator and return
        INPUT:
            dag - dag
            task_id - task id to be created
        OUTPUT:
            creates the task using dummy operator and returns
    """
    dummy_task_created = DummyOperator(dag=dag, task_id=task_id)
    return dummy_task_created


def create_python_operator(dag, task_id, python_callable, op_kwargs={}, provide_context=False, trigger_rule_value="all_success"):
    """ create and returns python operator
`       INPUT:
            dag - dag
            task_id - unique id for task
            python_callable - python function to be execute
            op_kwargs - {dict} keyword arguments
            provide_context - Boolean; If true context will be passed to the calling task
        OUTPUT:
            creates the task using python operator and returns

    """
    download_google_analytics_report_task = PythonOperator(
        dag=dag,
        task_id=task_id,
        provide_context=provide_context,
        python_callable=python_callable,
        trigger_rule=trigger_rule_value,
        op_kwargs=op_kwargs
    )
    return download_google_analytics_report_task
