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

    # Here comes an extremely hacky way
    # to ensure that we use the correct temp folder
    # to initialize the Airflow database
    # and use it through provide_session etc
    os.environ["AIRFLOW_HOME"] = airflow_home
    from airflow import configuration
    configuration.AIRFLOW_HOME = airflow_home
    from airflow import settings
    settings.SQL_ALCHEMY_CONN = "sqlite:///" + \
        os.path.join(airflow_home, "airflow.db")
    settings.reconfigure_orm()

    if airflow_home not in __those_are_inited:
        from airflow.utils import db
        try:
            db.initdb(load_connections=False)
        except TypeError:
            db.initdb()

        # Ensure LogTemplate table is synchronized with current config
        # This fixes an issue in Airflow 2.10+ where LogTemplate may be empty
        # causing TypeError when creating DagRun
        from airflow_bdd.compat import provide_session

        @provide_session
        def ensure_log_template(session=None):
            # synchronize_log_template ensures LogTemplate table has at least one entry
            if hasattr(db, "synchronize_log_template"):
                db.synchronize_log_template(session=session)

        ensure_log_template()

        # Load variables into the database if AIRFLOW__BDD__VARIABLES_FILE is set
        if os.environ.get('AIRFLOW__BDD__VARIABLES_FILE'):
            from airflow_bdd.compat import Variable
            import json

            with open(os.environ.get('AIRFLOW__BDD__VARIABLES_FILE'), "r") as varfile:
                var = json.load(varfile)

            for k, v in var.items():
                if isinstance(v, dict):
                    Variable.set(k, v, serialize_json=True)
                else:
                    Variable.set(k, v)

        __those_are_inited.add(airflow_home)

    return airflow_home
