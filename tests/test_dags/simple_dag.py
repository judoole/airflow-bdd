from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="simple_dag",
) as dag_1:
    EmptyOperator(task_id="simpleton_task")

    BashOperator(
        task_id="bash_task",
        bash_command="echo {{ds_nodash}}",
    )
