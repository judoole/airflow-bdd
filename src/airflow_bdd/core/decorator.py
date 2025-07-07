from functools import wraps
from airflow_bdd.core.context import (
    _get_context,
    _reset_context,
    _activate_feature,
    _is_feature_active,
)
from airflow_bdd.core.config import get_config_for_test_file


def feature(airflow_home=None,
            reset_dagruns=True,
            reset_xcoms=True,
            reset_variables=False):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Get test file path
            import inspect
            test_file_path = inspect.getfile(fn)
            
            # Get config (lazy loaded and cached)
            config = get_config_for_test_file(test_file_path)
            
            # Reset context with config
            _reset_context(airflow_home=airflow_home,
                           reset_dagruns=reset_dagruns,
                           reset_xcoms=reset_xcoms,
                           reset_variables=reset_variables,
                           config=config)
            
            _activate_feature()
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def bdd(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_feature_active():
            raise RuntimeError(
                f"Missing @feature on test function calling `{fn.__name__}`. "
                f"Make sure to decorate your test with @feature."
            )
        context = _get_context()
        return fn(*args, context=context, **kwargs)
    return wrapper
