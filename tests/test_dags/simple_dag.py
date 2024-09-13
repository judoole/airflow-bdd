from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="simple_dag",
) as dag_1:
    EmptyOperator(task_id="simpleton_task")
