# src/airflow_bdd/core/config.py
import os
import importlib.util
from dataclasses import dataclass, field
from airflow_bdd.steps.providers.google.bigquery.bigquery_config import (
    BigQueryConfig
)


@dataclass
class AirflowBddConfig:
    """Main configuration wrapper for AirflowBDD."""
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    
    @classmethod
    def from_module(cls, module):
        """
        Create config from a module, using defaults for missing attributes.
        """
        config = cls()
        
        if hasattr(module, 'bigquery'):
            bigquery_obj = module.bigquery
            
            # Check for attributes using getattr to handle dynamic types
            expected_fields = [
                'project_id', 'dataset_id', 'location',
                'maximum_bytes_billed', 'use_legacy_sql'
            ]
            
            for field_name in expected_fields:
                if hasattr(bigquery_obj, field_name):
                    field_value = getattr(bigquery_obj, field_name)
                    setattr(config.bigquery, field_name, field_value)
        
        return config


# Global cache for loaded configs
_config_cache = {}


def find_airflow_bdd_config(test_file_path):
    """Find airflow_bdd_config.py in common locations."""
    test_dir = os.path.dirname(test_file_path)
    home_dir = os.path.expanduser("~")
    
    # Find the tests directory by walking up the directory tree
    def find_tests_directory(start_path):
        """Walk up the directory tree to find the tests directory."""
        current_path = start_path
        while current_path != os.path.dirname(current_path):  # Stop at root
            if os.path.basename(current_path) == "tests":
                return current_path
            current_path = os.path.dirname(current_path)
        return None
    
    tests_dir = find_tests_directory(test_dir)
    
    search_locations = [
        # 1. Root directory (where pytest/unittest is typically run from)
        os.path.join(os.getcwd(), "airflow_bdd_config.py"),
    ]
    
    # 2. Tests directory (if found)
    if tests_dir:
        search_locations.append(
            os.path.join(tests_dir, "airflow_bdd_config.py")
        )
    
    # 3. Parent of tests directory (common project structure)
    if tests_dir:
        search_locations.append(
            os.path.join(os.path.dirname(tests_dir), "airflow_bdd_config.py")
        )
    
    # 4. User's home directory (hidden config file)
    search_locations.append(
        os.path.join(home_dir, ".airflow_bdd_config.py")
    )
    
    for config_path in search_locations:
        if os.path.exists(config_path):
            return config_path
    
    return None


def load_config_from_file(config_file_path):
    """Load configuration from a single file."""
    try:
        spec = importlib.util.spec_from_file_location(
            "airflow_bdd_config", config_file_path
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load spec for {config_file_path}")
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        return AirflowBddConfig.from_module(config_module)
    except Exception as e:
        print(f"Warning: Could not load config from {config_file_path}: {e}")
        return AirflowBddConfig()


def get_config_for_test_file(test_file_path):
    """Get config for a test file, using cache if available."""
    # Check cache first
    if test_file_path in _config_cache:
        return _config_cache[test_file_path]
    
    # Find and load config file
    config_file_path = find_airflow_bdd_config(test_file_path)
    
    if config_file_path:
        config = load_config_from_file(config_file_path)
    else:
        config = AirflowBddConfig()  # Use defaults
    
    # Cache the result
    _config_cache[test_file_path] = config
    
    return config


def clear_config_cache():
    """Clear the config cache (useful for testing)."""
    global _config_cache
    _config_cache.clear()