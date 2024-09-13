import tempfile
import os

__those_are_inited = set()
__default = None


def init_airflow_db(airflow_home: str):
    """Initialize the Airflow database.
    We try to do this ONLY ONCE
    """
    global __default
    if not airflow_home and not __default:
        __default = tempfile.mkdtemp()
    airflow_home = airflow_home or __default

    if airflow_home not in __those_are_inited:
        os.environ["AIRFLOW_HOME"] = airflow_home
        from airflow.utils import db
        db.initdb()
        __those_are_inited.add(airflow_home)

    return airflow_home
