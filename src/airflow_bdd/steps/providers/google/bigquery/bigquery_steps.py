import csv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
import uuid
import json
from typing import Any, Dict, Iterable, List, Optional, Union

from google.cloud.bigquery.job import QueryJob
from google.cloud.bigquery.table import RowIterator, TableReference
from airflow_bdd.core.decorator import bdd
from airflow_bdd.core.context import Context
from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.description import Description
from hamcrest import has_property, has_entry


@bdd
def given_bigquery_client(
        project_id: Optional[str] = None,
        maximum_bytes_billed: Optional[int] = None,
        location: Optional[str] = None,
        context: Optional[Context] = None):
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    # job_config.dry_run = dry_run
    # Set the maximum bytes billed to 30MB
    job_config.maximum_bytes_billed = maximum_bytes_billed or context.config.bigquery.maximum_bytes_billed
    job_config.labels = {"integration_test": "true"}

    context["bigquery_client"] = bigquery.Client(
        project=project_id or context.config.bigquery.project_id,
        location=location or context.config.bigquery.location,
        default_query_job_config=job_config)


@bdd
def given_table(
        table_name: str,
        schema: Union[str, List[Dict[str, Any]]],
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        context: Optional[Context] = None):
    if "bigquery_client" not in context:
        given_bigquery_client(project_id=project_id)

    client: bigquery.Client = context["bigquery_client"]
    unique_table_id = f"{project_id or context.config.bigquery.project_id}.{dataset_id or context.config.bigquery.dataset_id}.{table_name}_{str(uuid.uuid4())[:5]}"

    client.create_table(
        table=bigquery.Table(
            unique_table_id,            
            schema=json.loads(open(schema).read()) if isinstance(schema, str) else schema,
        ),
        # TODO: Maybe not ok
        exists_ok=True
    )

    context["bigquery_table"] = unique_table_id
    context[table_name] = unique_table_id


@bdd
def given_table_data(data: Iterable[Dict[str, Any]], table_name: str = None, project_id: str = None, dataset_id: str = None, context: Context = None):
    """Step to insert data into a table in BigQuery."""
    if "bigquery_client" not in context:
        given_bigquery_client()

    client: bigquery.Client = context["bigquery_client"]

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    if isinstance(data, str):
        path = data.strip()
        if path.endswith(".json"):
            data = json.loads(open(data).read())
        elif path.endswith(".jsonl"):
            data = [json.loads(line) for line in open(data).readlines()]
        elif path.endswith(".csv"):
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                data = list(reader)
        else:
            raise ValueError(f"Unsupported data format: {data}")
        
    if table_name:
        unique_table_id = f"{project_id or context.config.bigquery.project_id}.{dataset_id or context.config.bigquery.dataset_id}.{table_name}_{str(uuid.uuid4())[:5]}"
        destination = TableReference.from_string(unique_table_id)
    else:
        unique_table_id = context["bigquery_table"]
        destination = TableReference.from_string(context["bigquery_table"])
    load_job = client.load_table_from_json(
        json_rows=data,
        destination=destination,
        # job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()    
    context["bigquery_table"] = unique_table_id
    context[table_name] = unique_table_id

@bdd
def when_I_get_the_content(resource: str=None, query: str=None, context: Context = None):
    if "bigquery_client" not in context:
        given_bigquery_client(context=context)

    client: bigquery.Client = context["bigquery_client"]
    
    # Evaulate query based on params
    _query = query if query else f"SELECT * from `{resource or context.it()}`"
    
    results: RowIterator = client.query_and_wait(_query)
    context["query_results"] = [dict(row.items()) for row in results]


def when_I_query(query: str):
    return when_I_get_the_content(query=query)


@bdd
def when_I_get_the_job(job_id: str = None, context: Context = None):
    if "bigquery_client" not in context:
        given_bigquery_client()
    
    client: bigquery.Client = context["bigquery_client"]
    job: QueryJob = client.get_job(job_id=job_id or context["output"])
    context["query_job"] = job

@bdd
def when_I_get_the_job_result(job_id: str = None, context: Context = None):
    if "query_job" not in context:
        when_I_get_the_job()

    context["query_results"] = [dict(row.items()) for row in context["query_job"].result()]    


@bdd
def when_I_get_the_table(table_id: str = None, context: Context = None):
    table_to_get = None
    if table_id:
        table_to_get = context[table_id]
    else:
        if "query_job" not in context:
            when_I_get_the_job()
        table_to_get = context["query_job"].destination

    client: bigquery.Client = context["bigquery_client"]
    context["bigquery_table"] = client.get_table(table_to_get)
    context["table"] = context["bigquery_table"]

@bdd
def bigquery_table_ref(table_name: str, context: Context = None) -> TableReference:
    return TableReference.from_string(context[table_name])

@bdd
def bigquery_table_id(table_name: str, context: Context = None) -> str:
    return context[table_name]



class TableExists(BaseMatcher):
    """Class for inspecting BigQuery table existence."""

    @bdd
    def _matches(self, item, context: Context = None):
        # get or create bigquery client
        if "bigquery_client" not in context:
            given_bigquery_client()
        client: bigquery.Client = context["bigquery_client"]

        try:
            client.get_table(item)
            return True
        except NotFound:
            return False

    def describe_to(self, description: Description) -> None:
        description.append_text("Not existing")


class HasTableContent(BaseMatcher):
    """Class for checking BigQuery table content by querying and delegating to another matcher."""

    @bdd
    def __init__(self, content_matcher, context=None):
        self.content_matcher = content_matcher
        self.context = context
        self._actual_data = None

    def _matches(self, item):
        if self.context is None:
            return False
        
        if "bigquery_client" not in self.context:
            return False
        
        client: bigquery.Client = self.context["bigquery_client"]
        
        try:
            # Query the table to get its content
            query = f"SELECT * FROM `{item}`"
            results: RowIterator = client.query_and_wait(query)
            
            # Convert results to list of dictionaries
            self._actual_data = [dict(row.items()) for row in results]
            
            # Delegate to the provided matcher
            return self.content_matcher.matches(self._actual_data)
            
        except Exception:
            return False

    def describe_to(self, description):
        description.append_text("BigQuery table with content that ")
        self.content_matcher.describe_to(description)
    
    def describe_mismatch(self, item, mismatch_description):
        if self.context is None:
            mismatch_description.append_text("No context provided. Missing a @feature decorator?")
        elif "bigquery_client" not in self.context:
            mismatch_description.append_text(
                "No BigQuery client found in context")
        else:
            if not item:
                mismatch_description.append_text(
                    "No table name specified or found in context")
            else:
                mismatch_description.append_text(
                    f"Table '{item}' content: {self._actual_data}")
                mismatch_description.append_text(". Expected: ")
                self.content_matcher.describe_to(mismatch_description)


class HasQuery(BaseMatcher):
    """Class for inspecting the SQL query in a BigQueryInsertJobOperator.
    It iterates through the configuration object of the class and finds the query.
    """

    def __init__(self, value_matcher):
        self.value_matcher = value_matcher

    def _matches(self, item):
        # Use nested matchers and pass the user-supplied matcher to the innermost match
        return has_property(
            "configuration", has_entry(
                "query", has_entry("query", self.value_matcher))
        )._matches(item)

    def describe_to(self, description):
        description.append_text(
            "an object with a 'configuration' property containing a 'query' entry where 'query' matches: "
        )
        self.value_matcher.describe_to(description)

    def describe_mismatch(self, item, mismatch_description):
        if not has_property("configuration").matches(item):
            mismatch_description.append_text(
                "No 'configuration' property found")
        elif not "query" in item.configuration:
            mismatch_description.append_text(
                "No 'query' entry found in 'configuration'")
        elif not "query" in item.configuration["query"]:
            mismatch_description.append_text(
                "No 'query' entry found in 'configuration.query'")
        else:
            self.value_matcher.describe_mismatch(
                item.configuration["query"]["query"], mismatch_description)


def has_query(value_matcher):
    """
    Convenience function to create a HasQueryWithCustomCondition matcher.

    Args:
        value_matcher: The matcher to apply to the innermost 'query' value.

    Returns:
        A HasQueryWithCustomCondition matcher.
    """
    return HasQuery(value_matcher)


table_exists = TableExists
exists = TableExists
has_table_content = HasTableContent
