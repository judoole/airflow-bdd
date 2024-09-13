from functools import wraps
import inspect
from airflow_bdd.core.scenario import Scenario
from airflow_bdd.core.db_init import init_airflow_db


class AirflowBDDDecorator:
    def __init__(self, airflow_home: str = None):
        # The scenario instance that will
        # contain the context and steps for this test        
        self.airflow_home = airflow_home

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

            # If self.airflow_home is not set, create a temporary directory
            # and set it as the AIRFLOW_HOME environment variable
            # TODO: This does not look good...
            init_airflow_db(self.airflow_home)

            # Remove the first argument from args
            #if args:
            #    args = args[1:]
            return func(scenario, *args, **kwargs)

        # Assign the new signature to the wrapped function
        wrapper.__signature__ = new_sig
        return wrapper

    def __enter__(self):
        # Experimental... not sure if this is the right approach
        return self.context


airflow_bdd = AirflowBDDDecorator  # Create an instance of the decorator
