import json
from airflow.models import Connection
from airflow_bdd.core.decorator import feature
from airflow_bdd.steps.dag_steps import (
    it, given_a_dag, given_execution_date, given_a_task, when_I_execute_the_task, when_I_render_the_task
)
from airflow_bdd.steps.providers.google.bigquery.bigquery_steps import (
    bigquery_table_id,
    given_table,
    given_table_data,
    exists,
    when_I_get_the_content,
    has_query,
    when_I_get_the_job_result,
    when_I_query
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from hamcrest import not_, assert_that as then, contains_exactly, has_entries, has_length
import pytest
pytestmark = pytest.mark.bigquery


@feature()
def test_given_table():
    """As a developer
    I want to create a BigQuery table
    So that I can test the table
    """
    given_table(
        table_name="my_table",
        schema="""[
    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"}
  ]""")
    then(it(), exists())


@feature()
def test_given_table_not_exists():
    """Check for a table that does not exist."""
    then("my-yabbadabbadoo.dataset.yippie_ki_yay", not_(exists()))


@feature()
def test_insert_data_to_table():
    """As a developer
    I want to insert data to a BigQuery table
    So that I can test the table
    """
    given_table(
        table_name="my_table", schema="""[
    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"}
  ]""")
    given_table_data([{"id": 123, "name": "Clark Kent"},
                      {"id": 124, "name": "Lex Luthor"}])
    then(it(), exists())


@feature()
def test_table_content():
    """As a developer
    I want to check the content of a BigQuery table
    So that I can verify data was inserted correctly
    """
    given_table(
        table_name="my_table", schema="""[
    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"}
  ]""")
    given_table_data([
        {"id": 123, "name": "Clark Kent"},
        {"id": 124, "name": "Lex Luthor"}
    ])
    when_I_get_the_content()
    then(it(), contains_exactly(
        has_entries(id=123, name="Clark Kent"),
        has_entries(id=124, name="Lex Luthor")
    ))
    # Test table has correct number of rows
    then(it(), has_length(2))


@feature()
def test_table_content_from_json_file():
    """As a developer
    I want to load data from a jsonfile into a BigQuery table
    So that I can verify data was inserted correctly
    """
    given_table_data(table_name="my_inserted_table",
                     data="tests/providers/google/bigquery/test_input_data.json")
    when_I_get_the_content()
    then(it(), contains_exactly(
        has_entries(id=321, name="Clark Kent"),
        has_entries(id=654, name="Lex Luthor")
    ))
    # Test table has correct number of rows
    then(it(), has_length(2))


@feature()
def test_table_content_from_jsonl_file():
    """As a developer
    I want to load data from a file into a BigQuery table
    So that I can verify data was inserted correctly
    """
    given_table_data(table_name="my_inserted_table",
                     data="tests/providers/google/bigquery/test_input_data.jsonl")
    when_I_get_the_content()
    then(it(), contains_exactly(
        has_entries(id=321, name="Clark Kent"),
        has_entries(id=654, name="Lex Luthor")
    ))
    # Test table has correct number of rows
    then(it(), has_length(2))


@feature()
def test_table_content_from_csv_file():
    """As a developer
    I want to load data from a file into a BigQuery table
    So that I can verify data was inserted correctly
    """
    given_table_data(table_name="my_inserted_table",
                     data="tests/providers/google/bigquery/test_input_data.csv")
    when_I_get_the_content()
    then(it(), contains_exactly(
        has_entries(id=321, name="Clark Kent"),
        has_entries(id=654, name="Lex Luthor")
    ))
    # Test table has correct number of rows
    then(it(), has_length(2))


@feature()
def test_query_table():
    """As a developer
    I want to query the content of a BigQuery table
    So that I can verify data was inserted correctly
    """
    given_table(
        table_name="my_table", schema="""[
    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"}
  ]""")
    given_table_data([
        {"id": 123, "name": "Clark Kent"},
        {"id": 124, "name": "Lex Luthor"}
    ])
    when_I_query(f"SELECT * FROM `{bigquery_table_id('my_table')}`")
    then(it(), contains_exactly(
        has_entries(id=123, name="Clark Kent"),
        has_entries(id=124, name="Lex Luthor")
    ))
    # Test table has correct number of rows
    then(it(), has_length(2))


@feature()
def test_get_result_from_BigQueryInsertJobOperator():
    """As a developer
    I want to query the result of a BigQueryInsertJobOperator
    So that I can verify the result of the job
    """
    given_a_task(
        BigQueryInsertJobOperator(
            task_id="test",
            # project_id will be overridden by the Airflow BDD config
            # along with maximum_bytes_billed, dataset_id and location
            project_id="asdfasdfasdf",
            configuration={
                "query": {
                    "query": "SELECT 'Clark Kent' as name",
                    "useLegacySql": False,
                }
            }
        )
    )
    when_I_execute_the_task()
    when_I_get_the_job_result()
    then(it(), contains_exactly(
        has_entries(name="Clark Kent"),
    ))
    # Test table has correct number of rows
    then(it(), has_length(1))


@feature()
def test_has_query():
    """As a developer
    I want to have a quick hamcrest matcher for the query part of the 
    BigQueryInsertJobOperator
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
