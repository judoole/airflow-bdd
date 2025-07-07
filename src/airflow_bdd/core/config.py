# src/airflow_bdd/core/config.py
import os
import importlib.util
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from airflow_bdd.steps.providers.google.bigquery.bigquery_config import BigQueryConfig


@dataclass
class AirflowBddConfig:
    """Main configuration wrapper for AirflowBDD."""
    bigquery: BigQueryConfig = field(default_factory=BigQueryConfig)
    
    @classmethod
    def from_module(cls, module):
        """Create config from a module, using defaults for missing attributes."""
        config = cls()
        
        if hasattr(module, 'bigquery'):
            for field_name, field_value in module.bigquery.__dict__.items():
                if not field_name.startswith('_'):
                    setattr(config.bigquery, field_name, field_value)
        
        return config


# Global cache for loaded configs
_config_cache = {}


def find_airflow_bdd_config(test_file_path):
    """Find airflow_bdd_config.py in common locations."""
    test_dir = os.path.dirname(test_file_path)
    home_dir = os.path.expanduser("~")
    
    search_locations = [
        # 1. Root directory (where pytest/unittest is typically run from)
        os.path.join(os.getcwd(), "airflow_bdd_config.py"),
        
        # 2. Tests directory (if test is in a subdirectory)
        os.path.join(test_dir, "tests", "airflow_bdd_config.py"),
        
        # 3. User's home directory (hidden config file)
        os.path.join(home_dir, ".airflow_bdd_config.py"),
    ]
    
    for config_path in search_locations:
        if os.path.exists(config_path):
            return config_path
    
    return None


def load_config_from_file(config_file_path):
    """Load configuration from a single file."""
    try:
        spec = importlib.util.spec_from_file_location("airflow_bdd_config", config_file_path)
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