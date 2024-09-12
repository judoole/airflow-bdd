# bdd.py
from functools import wraps
from datetime import datetime
import inspect
from airflow_bdd.scenario import Scenario

    

class AirflowBDD:
    def __init__(self):
        self.singleton = Scenario()  # Singleton object created once

    def __call__(self, func):
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
            return func(self.singleton, *args, **kwargs)

        # Assign the new signature to the wrapped function
        wrapper.__signature__ = new_sig
        return wrapper

    def __enter__(self):        
        return self.context


airflow_bdd = AirflowBDD  # Create an instance of the decorator