from functools import wraps
import inspect
from airflow_bdd.scenario import Scenario


class AirflowBDD:
    def __init__(self):
        # The scenario instance that will
        # contain the context and steps for this test
        self.scenario = Scenario()

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
            # Remove the first argument from args
            if args:
                args = args[1:]
            return func(self.scenario, *args, **kwargs)

        # Assign the new signature to the wrapped function
        wrapper.__signature__ = new_sig
        return wrapper

    def __enter__(self):
        # Experimental... not sure if this is the right approach
        return self.context


airflow_bdd = AirflowBDD  # Create an instance of the decorator
