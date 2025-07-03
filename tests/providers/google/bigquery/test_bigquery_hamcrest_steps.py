from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import (
    given_a_dag,
    given_execution_date,
    given_a_task,
    when_I_render_the_task,
    it,
)
from airflow_bdd.steps.providers.google.bigquery.bigquery_hamcrest_steps import has_query
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from hamcrest import assert_that as then

@feature()
def test_has_query():
    """As a developer
    I want to have a quick hamcrest matcher for the query part of the BigQueryInsertJobOperator
    So that I can create readable tests
    And that I don't have to write so much
    """
    given_a_dag()
    given_execution_date("2021-01-01")
    given_a_task(
        BigQueryInsertJobOperator(
            task_id="test",
            configuration={
                "query": {
                    "query": "SELECT '{{ ds }}' as date",
                    "useLegacySql": False
                }
            }
        )
    )
    when_I_render_the_task()
    then(it(), has_query("SELECT '2021-01-01' as date"))