"""Compatibility imports for Airflow 2.x and 3.x."""

try:  # Airflow 3 stable authoring interface
    from airflow.sdk import DAG  # type: ignore
except ImportError:  # pragma: no cover - exercised in Airflow 2.x
    from airflow.models.dag import DAG

try:
    from airflow.models.dagbag import DagBag
except ImportError:  # pragma: no cover - defensive fallback
    from airflow.models import DagBag

try:
    from airflow.models.connection import Connection
except ImportError:  # pragma: no cover - defensive fallback
    from airflow.models import Connection

try:
    from airflow.models.dagrun import DagRun
except ImportError:  # pragma: no cover - defensive fallback
    from airflow.models import DagRun

try:
    from airflow.models.taskinstance import TaskInstance
except ImportError:  # pragma: no cover - defensive fallback
    from airflow.models import TaskInstance

try:
    from airflow.models.variable import Variable
except ImportError:  # pragma: no cover - defensive fallback
    from airflow.models import Variable

try:
    from airflow.models.xcom import BaseXCom, XComModel, XCOM_RETURN_KEY
except ImportError:  # pragma: no cover - Airflow 2.x fallback
    from airflow.models.xcom import XCom as BaseXCom, XCOM_RETURN_KEY
    from airflow.models import XCom
    XComModel = XCom

XCom = BaseXCom


if not hasattr(DAG, "create_dagrun"):
    def _create_dagrun(  # pragma: no cover - exercised in Airflow 3.x
        self,
        run_id=None,
        logical_date=None,
        data_interval=None,
        run_after=None,
        start_date=None,
        conf=None,
        state=None,
        run_type=None,
        creating_job_id=None,
        session=None,
        **kwargs,
    ):
        from airflow.models.dagrun import DagRun, DagRunType

        if logical_date is None and "execution_date" in kwargs:
            logical_date = kwargs.pop("execution_date")

        if isinstance(run_type, str):
            try:
                run_type = DagRunType(run_type)
            except Exception:
                pass

        dag_run = DagRun(
            dag_id=self.dag_id,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=run_after or logical_date,
            start_date=start_date or logical_date,
            conf=conf,
            state=state,
            run_type=run_type,
            creating_job_id=creating_job_id,
            **kwargs,
        )
        dag_run.dag = self

        if session is not None:
            session.add(dag_run)
            session.flush()
            dag_version_id = kwargs.get("dag_version_id")
            try:
                dag_run.verify_integrity(session=session, dag_version_id=dag_version_id)
            except TypeError:
                dag_run.verify_integrity(session=session, dag_version_id=None)
            session.flush()

        return dag_run

    DAG.create_dagrun = _create_dagrun  # type: ignore[attr-defined]

try:
    from airflow.utils.session import provide_session
except ImportError:  # pragma: no cover - older Airflow fallback
    from airflow.utils.db import provide_session

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # pragma: no cover - Airflow 2.x fallback
    from airflow.operators.empty import EmptyOperator

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # pragma: no cover - Airflow 2.x fallback
    from airflow.operators.bash import BashOperator

__all__ = [
    "BashOperator",
    "Connection",
    "DAG",
    "DagBag",
    "DagRun",
    "EmptyOperator",
    "TaskInstance",
    "Variable",
    "XCOM_RETURN_KEY",
    "XCom",
    "XComModel",
    "provide_session",
]
