"""This is the dataclass for the BigQuery config."""

from dataclasses import dataclass


@dataclass
class BigQueryConfig:
    """This is the dataclass for the BigQuery config."""

    project_id: str = "create-a-airflow-bdd-config-and-set-this"
    dataset_id: str = "airflow_bdd"
    location: str = "EU"
    maximum_bytes_billed: int = 30000000  # 30MB
    use_legacy_sql: bool = False
