from contextvars import ContextVar
from typing import Optional
from airflow.utils.db import provide_session
from airflow_bdd.core.db_init import init_airflow_db
from airflow_bdd.core.config import AirflowBddConfig


_test_context: ContextVar[Optional["Context"]] = ContextVar(
    "test_context", default=None
)
_feature_active: ContextVar[bool] = ContextVar("feature_active", default=False)


class Context:
    """The Context object is just a dict, which has notion of "it".
    "it" is the last value that was put on the context.

    The context object itself keeps all your variables in between steps.
    So that you can reuse values in later steps.
    """

    def __init__(self,
                 airflow_home=None,
                 reset_dagruns=True,
                 reset_xcoms=True,
                 reset_variables=False,
                 config:AirflowBddConfig=None):
        self.context = {}
        self.context["it"] = None
        self.config = config
        # Initialize Airflow
        init_airflow_db(airflow_home)
        self._reset_db(
            reset_dagruns=reset_dagruns,
            reset_xcoms=reset_xcoms,
            reset_variables=reset_variables,
        )

    def __getitem__(self, key):
        return self.context[key]

    def __setitem__(self, key, value):
        self.context[key] = value
        self.context["it"] = value

    def __contains__(self, key):
        return key in self.context

    def set_it(self, value):
        self.context["it"] = value

    def add(self, key, value):
        self[key] = value

    def it(self):
        return self.context["it"]

    @provide_session
    def _reset_db(self, reset_dagruns, reset_xcoms, reset_variables, session=None):
        from airflow.models import DagRun, XCom, Variable
        if reset_dagruns:
            session.query(DagRun).delete()
        if reset_xcoms:
            session.query(XCom).delete()
        if reset_variables:
            session.query(Variable).delete()


def _get_context():
    ctx = _test_context.get()
    if ctx is None:
        ctx = Context()
        _test_context.set(ctx)
    return ctx


def _reset_context(airflow_home=None,
                   reset_dagruns=True,
                   reset_xcoms=True,
                   reset_variables=False,
                   config=None):
    _test_context.set(Context(airflow_home=airflow_home,
                              reset_dagruns=reset_dagruns,
                              reset_xcoms=reset_xcoms,
                              reset_variables=reset_variables,
                              config=config))


def _activate_feature():
    _feature_active.set(True)


def _is_feature_active():
    return _feature_active.get()
