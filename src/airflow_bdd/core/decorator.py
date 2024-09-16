from functools import wraps
import inspect
from airflow_bdd.core.scenario import Scenario
from airflow_bdd.core.db_init import init_airflow_db
from airflow.utils.db import provide_session
from typing import Union


class AirflowBDDecorator:
    def __init__(self,
                 airflow_home: str = None,                 
                 reset_dagruns: bool = True,
                 reset_xcoms: bool = True,
                 reset_variables: bool = False,
                 debug: bool = False,
                 ):
        # The scenario instance that will
        # contain the context and steps for this test
        self.airflow_home = airflow_home
        self.debug = debug
        self.reset_dagruns = reset_dagruns
        self.reset_xcoms = reset_xcoms
        self.reset_variables = reset_variables

    @provide_session
    def _reset_db(self, session=None):
        from airflow.models import DagRun, XCom, Variable
        if self.reset_dagruns: 
            session.query(DagRun).delete()
        if self.reset_xcoms:
            session.query(XCom).delete()
        if self.reset_variables:
            session.query(Variable).delete()

    def __call__(self, func):
        """This is a decorator that wraps the test function.
        It pops the first argument from the args list, which 
        is the scenario instance."""
        # Get the original function signature
        sig = inspect.signature(func)

        # Create a new signature excluding the first parameter
        params = list(sig.parameters.values())[1:]
        new_sig = sig.replace(parameters=params)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Initialize the scenario instance
            scenario = Scenario()
            # Init airflow, first and foremost
            self.airflow_home = init_airflow_db(self.airflow_home)
            # Make the db clean in between tests
            self._reset_db()

            if self.debug:
                return func(self, *args, **kwargs)
            else:
                return func(scenario, *args, **kwargs)

        # Assign the new signature to the wrapped function
        wrapper.__signature__ = new_sig
        return wrapper

    def __enter__(self):
        # Experimental... not sure if this is the right approach
        return self.context


airflow_bdd = AirflowBDDecorator  # Create an instance of the decorator
