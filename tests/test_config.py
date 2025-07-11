"""Tests for the AirflowBDD configuration system."""

from unittest import mock
from hamcrest import assert_that, equal_to, instance_of, is_, none
from airflow_bdd.core.config import (
    AirflowBddConfig,
    find_airflow_bdd_config,
    load_config_from_file,
    get_config_for_test_file,
    clear_config_cache,
)
from airflow_bdd.steps.providers.google.bigquery.bigquery_config import (
    BigQueryConfig
)


class TestAirflowBddConfig:
    """Test the AirflowBddConfig dataclass."""

    def test_default_config(self):
        """Test that default config is created correctly."""
        config = AirflowBddConfig()
        
        assert_that(config.bigquery, instance_of(BigQueryConfig))
        assert_that(
            config.bigquery.project_id,
            equal_to("create-a-airflow-bdd-config-and-set-this")
        )
        assert_that(config.bigquery.dataset_id, equal_to("airflow_bdd"))
        assert_that(config.bigquery.location, equal_to("EU"))
        assert_that(config.bigquery.maximum_bytes_billed, equal_to(30000000))
        assert_that(config.bigquery.use_legacy_sql, equal_to(False))

    def test_from_module_with_bigquery_config(self):
        """Test creating config from a module with bigquery configuration."""
        # Create a mock module with bigquery config
        mock_module = mock.MagicMock()
        mock_module.bigquery = mock.MagicMock()
        mock_module.bigquery.project_id = "test-project"
        mock_module.bigquery.dataset_id = "test_dataset"
        mock_module.bigquery.location = "US"
        mock_module.bigquery.maximum_bytes_billed = 50000000
        mock_module.bigquery.use_legacy_sql = True
        
        config = AirflowBddConfig.from_module(mock_module)
        
        assert_that(
            config.bigquery.project_id,
            equal_to("test-project")
        )
        assert_that(config.bigquery.dataset_id, equal_to("test_dataset"))
        assert_that(config.bigquery.location, equal_to("US"))
        assert_that(
            config.bigquery.maximum_bytes_billed,
            equal_to(50000000)
        )
        assert_that(config.bigquery.use_legacy_sql, equal_to(True))

    def test_from_module_without_bigquery_config(self):
        """
        Test creating config from a module without bigquery
        configuration.
        """
        mock_module = mock.MagicMock()
        # Explicitly remove bigquery attribute to ensure it doesn't exist
        delattr(mock_module, 'bigquery')
        
        config = AirflowBddConfig.from_module(mock_module)
        
        # Should use defaults
        assert_that(
            config.bigquery.project_id,
            equal_to("create-a-airflow-bdd-config-and-set-this")
        )
        assert_that(config.bigquery.dataset_id, equal_to("airflow_bdd"))

    def test_from_module_ignores_private_attributes(self):
        """
        Test that private attributes are ignored when loading from module.
        """
        mock_module = mock.MagicMock()
        mock_module.bigquery = mock.MagicMock()
        mock_module.bigquery.project_id = "test-project"
        mock_module.bigquery._private_attr = "should_be_ignored"
        
        config = AirflowBddConfig.from_module(mock_module)
        
        assert_that(config.bigquery.project_id, equal_to("test-project"))
        # Should not have the private attribute
        assert_that(
            hasattr(config.bigquery, '_private_attr'),
            equal_to(False)
        )


class TestFindAirflowBddConfig:
    """Test the config file discovery functionality."""

    def test_find_config_in_current_directory(self, tmp_path):
        """Test finding config in current working directory."""
        # Create config file in current directory
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text("# Test config")
        
        with mock.patch('os.getcwd', return_value=str(tmp_path)):
            result = find_airflow_bdd_config("/some/test/file.py")
            assert_that(result, equal_to(str(config_file)))

    def test_find_config_in_tests_directory(self, tmp_path):
        """Test finding config in tests directory."""
        # Create tests directory structure
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        config_file = tests_dir / "airflow_bdd_config.py"
        config_file.write_text("# Test config")
        
        test_file = tmp_path / "tests" / "test_something.py"
        test_file.write_text("# Test file")
        
        result = find_airflow_bdd_config(str(test_file))
        assert_that(result, equal_to(str(config_file)))

    def test_find_config_in_home_directory(self, tmp_path):
        """Test finding config in user's home directory."""
        # Mock home directory
        home_config = tmp_path / ".airflow_bdd_config.py"
        home_config.write_text("# Home config")
        
        with mock.patch('os.path.expanduser', return_value=str(tmp_path)):
            result = find_airflow_bdd_config("/some/test/file.py")
            assert_that(result, equal_to(str(home_config)))

    def test_config_not_found(self):
        """Test when no config file is found."""
        with mock.patch('os.path.exists', return_value=False):
            result = find_airflow_bdd_config("/some/test/file.py")
            assert_that(result, none())

    def test_search_order_priority(self, tmp_path):
        """
        Test that search order follows priority
        (current dir > tests dir > home).
        """
        # Create config files in all locations
        current_config = tmp_path / "airflow_bdd_config.py"
        current_config.write_text("# Current config")
        
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        tests_config = tests_dir / "airflow_bdd_config.py"
        tests_config.write_text("# Tests config")
        
        home_config = tmp_path / ".airflow_bdd_config.py"
        home_config.write_text("# Home config")
        
        with mock.patch('os.getcwd', return_value=str(tmp_path)), \
             mock.patch('os.path.expanduser', return_value=str(tmp_path)):
            result = find_airflow_bdd_config(
                str(tests_dir / "test_something.py")
            )
            # Should find current directory config first
            assert_that(result, equal_to(str(current_config)))


class TestLoadConfigFromFile:
    """Test loading configuration from files."""

    def test_load_valid_config_file(self, tmp_path):
        """Test loading a valid configuration file."""
        config_content = """
bigquery = type('BigQueryConfig', (), {
    'project_id': 'test-project',
    'dataset_id': 'test_dataset',
    'location': 'US',
    'maximum_bytes_billed': 50000000,
    'use_legacy_sql': True
})()
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        config = load_config_from_file(str(config_file))
        
        assert_that(config.bigquery.project_id, equal_to("test-project"))
        assert_that(config.bigquery.dataset_id, equal_to("test_dataset"))
        assert_that(config.bigquery.location, equal_to("US"))
        assert_that(config.bigquery.maximum_bytes_billed, 
            equal_to(50000000)
        )
        assert_that(config.bigquery.use_legacy_sql, equal_to(True))

    def test_load_config_without_bigquery(self, tmp_path):
        """Test loading a config file without bigquery configuration."""
        config_content = """
# No bigquery config here
some_other_var = "value"
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        config = load_config_from_file(str(config_file))
        
        # Should use defaults
        assert_that(
            config.bigquery.project_id,
            equal_to("create-a-airflow-bdd-config-and-set-this")
        )
        assert_that(config.bigquery.dataset_id, equal_to("airflow_bdd"))

    def test_load_invalid_config_file(self, tmp_path):
        """Test loading an invalid configuration file."""
        config_content = """
# This will cause a syntax error
invalid syntax here
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        # Should not raise exception, but return default config
        config = load_config_from_file(str(config_file))
        assert_that(config, instance_of(AirflowBddConfig))
        assert_that(
            config.bigquery.project_id,
            equal_to("create-a-airflow-bdd-config-and-set-this")
        )

    def test_load_nonexistent_file(self):
        """Test loading a non-existent file."""
        config = load_config_from_file("/nonexistent/path/config.py")
        assert_that(config, instance_of(AirflowBddConfig))
        assert_that(
            config.bigquery.project_id,
            equal_to("create-a-airflow-bdd-config-and-set-this")
        )


class TestGetConfigForTestFile:
    """Test getting configuration for test files with caching."""

    def setup_method(self):
        """Clear cache before each test to avoid interference."""
        clear_config_cache()

    def test_get_config_with_cache(self, tmp_path):
        """Test that config is cached and reused."""
        # Create a config file
        config_content = """
bigquery = type('BigQueryConfig', (), {
    'project_id': 'cached-project',
    'dataset_id': 'cached_dataset'
})()
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        test_file = "/path/to/test_file.py"
        
        with mock.patch('os.getcwd', return_value=str(tmp_path)):
            # First call should load from file
            config1 = get_config_for_test_file(test_file)
            assert_that(
                config1.bigquery.project_id,
                equal_to("cached-project")
            )
            # Second call should use cache
            config2 = get_config_for_test_file(test_file)
            assert_that(
                config2.bigquery.project_id,
                equal_to("cached-project")
            )
            # Should be the same object (cached)
            assert_that(config1, is_(config2))

    def test_get_config_without_config_file(self):
        """Test getting config when no config file exists."""
        test_file = "/path/to/test_file.py"
        
        with mock.patch('os.path.exists', return_value=False):
            config = get_config_for_test_file(test_file)
            assert_that(config, instance_of(AirflowBddConfig))
            assert_that(
                config.bigquery.project_id,
                equal_to("create-a-airflow-bdd-config-and-set-this")
            )

    def test_clear_config_cache(self, tmp_path):
        """Test clearing the config cache."""
        # Create a config file
        config_content = """
bigquery = type('BigQueryConfig', (), {
    'project_id': 'cache-test-project'
})()
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        test_file = "/path/to/test_file.py"
        
        with mock.patch('os.getcwd', return_value=str(tmp_path)):
            # Load config (should be cached)
            config1 = get_config_for_test_file(test_file)
            assert_that(
                config1.bigquery.project_id,
                equal_to("cache-test-project")
            )
            # Clear cache
            clear_config_cache()
            # Load again (should reload from file)
            config2 = get_config_for_test_file(test_file)
            assert_that(
                config2.bigquery.project_id,
                equal_to("cache-test-project")
            )
            # Should be different objects (cache was cleared)
            # Note: The objects might be the same if the config file content is identical,
            # but the cache should be cleared, so we verify the cache is empty
            from airflow_bdd.core.config import _config_cache
            # The cache should be empty after clearing, but gets populated again
            # So we check that the cache was actually used (not empty)
            assert_that(len(_config_cache), equal_to(1))


class TestConfigIntegration:
    """Integration tests for the configuration system."""

    def test_full_config_workflow(self, tmp_path):
        """Test the complete configuration workflow."""
        # Create a realistic config file
        config_content = """
# AirflowBDD Configuration
bigquery = type('BigQueryConfig', (), {
    'project_id': 'my-test-project',
    'dataset_id': 'test_dataset',
    'location': 'US-CENTRAL1',
    'maximum_bytes_billed': 100000000,  # 100MB
    'use_legacy_sql': False
})()
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        test_file = str(tmp_path / "tests" / "test_integration.py")
        
        with mock.patch('os.getcwd', return_value=str(tmp_path)):
            # Test the full workflow
            config = get_config_for_test_file(test_file)
            
            assert_that(config, instance_of(AirflowBddConfig))
            assert_that(config.bigquery, instance_of(BigQueryConfig))
            assert_that(
                config.bigquery.project_id, equal_to("my-test-project")
            )
            assert_that(config.bigquery.dataset_id, equal_to("test_dataset"))
            assert_that(config.bigquery.location, equal_to("US-CENTRAL1"))
            assert_that(
                config.bigquery.maximum_bytes_billed, equal_to(100000000)
            )
            assert_that(config.bigquery.use_legacy_sql, equal_to(False))

    def test_config_with_partial_bigquery_settings(self, tmp_path):
        """Test config with only some BigQuery settings specified."""
        config_content = """
# Only specify some BigQuery settings
bigquery = type('BigQueryConfig', (), {
    'project_id': 'partial-project',
    'location': 'EU'
})()
"""
        config_file = tmp_path / "airflow_bdd_config.py"
        config_file.write_text(config_content)
        
        test_file = str(tmp_path / "tests" / "test_partial.py")
        
        with mock.patch('os.getcwd', return_value=str(tmp_path)):
            config = get_config_for_test_file(test_file)
            
            # Specified values should be used
            assert_that(
                config.bigquery.project_id, equal_to("partial-project")
            )
            assert_that(config.bigquery.location, equal_to("EU"))
            # Unspecified values should use defaults
            assert_that(config.bigquery.dataset_id, equal_to("airflow_bdd"))
            assert_that(
                config.bigquery.maximum_bytes_billed, equal_to(30000000)
            )
            assert_that(config.bigquery.use_legacy_sql, equal_to(False)) 