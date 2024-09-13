from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id=45,
) as dag_2:
    EmptyOperator(task_id="simpleton_task")