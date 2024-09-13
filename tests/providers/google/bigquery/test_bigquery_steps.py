from airflow_bdd import airflow_bdd
from airflow_bdd.core.scenario import Scenario
from airflow_bdd.steps.dag_steps import a_dag, a_task, render_the_task, execution_date
from airflow_bdd.steps.providers.hamcrest.hamcrest_steps import it_
from airflow_bdd.steps.providers.google.bigquery.bigquery_hamcrest_steps import has_query
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

@airflow_bdd()
def test_has_query(bdd: Scenario):
    """As a developer
    I want to have a quick hamcrest matcher for the query part of the BigQueryInsertJobOperator
    So that I can create readable tests
    And that I don't have to write so much
    """
    bdd.given(a_dag())
    bdd.and_given(execution_date("2021-01-01"))
    bdd.and_given(a_task(
        BigQueryInsertJobOperator(
            task_id="test",
            configuration={
                "query": {
                    "query": "SELECT '{{ ds }}' as date",
                    "useLegacySql": False
                }
            }
        )
    ))
    bdd.when_I(render_the_task())
    bdd.then(it_(has_query("SELECT '2021-01-01' as date")))