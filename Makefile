# Configuration
AIRFLOW_VERSION ?= 2.10.5
VENV := .venv
ACTIVATE := . $(VENV)/bin/activate

# Use pyenv if available, otherwise fall back to system Python
PYENV_AVAILABLE := $(shell command -v pyenv 2>/dev/null)
PYTHON_VERSION := $(shell if [ -f .python-version ]; then cat .python-version; elif [ -n "$(PYENV_AVAILABLE)" ]; then pyenv version-name 2>/dev/null | cut -d' ' -f1 || echo "3.11"; else echo "3.11"; fi)

# Get Python executable - prefer pyenv's Python for the specified version
ifeq ($(PYENV_AVAILABLE),)
	PYTHON_EXEC := $(shell command -v python3.11 2>/dev/null || command -v python3 2>/dev/null || command -v python 2>/dev/null)
else
	PYTHON_EXEC := $(shell pyenv which python 2>/dev/null || command -v python3.11 2>/dev/null || command -v python3 2>/dev/null || command -v python 2>/dev/null)
endif

CONSTRAINT_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: help venv pip-install pip-install-airflow pip-install-other test test-html test-ci clean check-python setup-python

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  setup-python      to ensure Python 3.11 is installed via pyenv"
	@echo "  venv              to create a virtual environment using venv"
	@echo "  pip-install       to install all dependencies (Airflow + others)"
	@echo "  pip-install-airflow  to install Airflow with constraints"
	@echo "  pip-install-other    to install other dependencies without constraints"
	@echo "  test              to run the tests, using some BDD helper functions"
	@echo "  test-html         to run the tests, showing a HTML report"
	@echo "  test-ci           to run the tests for CI. Skipping BigQuery tests"
	@echo "  check-python      to check Python version and constraint URL"
	@echo "  clean             to remove virtual environment"
	@echo "  help              to show this message"
	@echo ""
	@echo "Configuration:"
	@echo "  AIRFLOW_VERSION=$(AIRFLOW_VERSION)"
	@echo "  PYTHON_VERSION=$(PYTHON_VERSION)"
	@echo "  CONSTRAINT_URL=$(CONSTRAINT_URL)"
	@if [ -n "$(PYENV_AVAILABLE)" ]; then \
		echo "  pyenv: available"; \
	else \
		echo "  pyenv: not available (using system Python)"; \
	fi

setup-python:
	@if [ -z "$(PYENV_AVAILABLE)" ]; then \
		echo "Warning: pyenv is not installed or not in PATH"; \
		echo "Install pyenv: https://github.com/pyenv/pyenv#installation"; \
		exit 1; \
	fi
	@if pyenv versions --bare | grep -q "^$(PYTHON_VERSION)$$"; then \
		echo "Python $(PYTHON_VERSION) is already installed via pyenv"; \
	else \
		echo "Installing Python $(PYTHON_VERSION) via pyenv..."; \
		pyenv install $(PYTHON_VERSION) || \
			(echo "Error: Failed to install Python $(PYTHON_VERSION). Check pyenv setup." && exit 1); \
	fi
	@echo "Setting local Python version to $(PYTHON_VERSION)..."
	@pyenv local $(PYTHON_VERSION)
	@echo "Python $(PYTHON_VERSION) is now set for this project"

check-python:
	@echo "Python Configuration:"
	@echo "  PYTHON_VERSION: $(PYTHON_VERSION)"
	@if [ -n "$(PYENV_AVAILABLE)" ]; then \
		echo "  pyenv: available"; \
		echo "  pyenv version: $$(pyenv version-name 2>/dev/null || echo 'not set')"; \
	else \
		echo "  pyenv: not available (using system Python)"; \
	fi
	@echo "  Python executable: $(PYTHON_EXEC)"
	@echo ""
	@echo "Airflow Configuration:"
	@echo "  AIRFLOW_VERSION: $(AIRFLOW_VERSION)"
	@echo "  CONSTRAINT_URL: $(CONSTRAINT_URL)"
	@echo ""
	@echo "Testing constraint URL availability..."
	@curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" "$(CONSTRAINT_URL)" || echo "Warning: Could not verify constraint URL"

# Target for creating a virtual environment using venv
venv: check-python
	@if [ -z "$(PYTHON_EXEC)" ]; then \
		echo "Error: Python executable not found. Please install Python $(PYTHON_VERSION)"; \
		if [ -n "$(PYENV_AVAILABLE)" ]; then \
			echo "Hint: Run 'pyenv install $(PYTHON_VERSION)' to install Python $(PYTHON_VERSION)"; \
		fi; \
		exit 1; \
	fi
	@if [ -d "$(VENV)" ]; then \
		echo "Virtual environment already exists at $(VENV)"; \
		echo "Current Python version: $$($(PYTHON_EXEC) --version 2>/dev/null || echo 'unknown')"; \
	else \
		echo "Creating virtual environment with Python $(PYTHON_VERSION)..."; \
		echo "Using Python: $(PYTHON_EXEC)"; \
		$(PYTHON_EXEC) -m venv $(VENV) || \
			(echo "Error: Failed to create virtual environment. Check Python installation." && exit 1); \
		echo "Virtual environment created at $(VENV)"; \
		echo "Python version: $$($(VENV)/bin/python --version)"; \
	fi

# Install Airflow with constraints (reproducible installation)
pip-install-airflow: venv
	@echo "Installing Airflow $(AIRFLOW_VERSION) with constraints..."
	@echo "Using constraint file: $(CONSTRAINT_URL)"
	$(ACTIVATE) && pip install --upgrade pip setuptools wheel
	$(ACTIVATE) && pip install "apache-airflow==$(AIRFLOW_VERSION)" --constraint "$(CONSTRAINT_URL)" || \
		(echo "Error: Failed to install Airflow. Check Python version compatibility." && exit 1)

# Install other dependencies without constraints (as per Airflow best practices)
pip-install-other: venv
	@echo "Installing other dependencies..."
	$(ACTIVATE) && pip install --upgrade pip
	$(ACTIVATE) && pip install "apache-airflow==$(AIRFLOW_VERSION)" apache-airflow-providers-google pytest pytest-html PyHamcrest || \
		(echo "Error: Failed to install dependencies." && exit 1)

# Install everything: Airflow with constraints, then other deps
pip-install: pip-install-airflow pip-install-other
	@echo ""
	@echo "Installation complete!"
	@echo "Run 'make test' to run tests"

test: venv
	@if [ ! -d "$(VENV)" ]; then \
		echo "Error: Virtual environment not found. Run 'make pip-install' first."; \
		exit 1; \
	fi
	$(ACTIVATE) && pytest --continue-on-collection-errors -v -rA --color=yes

test-html: venv
	@if [ ! -d "$(VENV)" ]; then \
		echo "Error: Virtual environment not found. Run 'make pip-install' first."; \
		exit 1; \
	fi
	$(ACTIVATE) && pytest --continue-on-collection-errors -v -rA --color=yes --html=/tmp/turbineflow-test-report.html || true
	@echo "Test report created at /tmp/turbineflow-test-report.html"
	@open /tmp/turbineflow-test-report.html 2>/dev/null || echo "Open /tmp/turbineflow-test-report.html manually"

test-ci: venv
	@if [ ! -d "$(VENV)" ]; then \
		echo "Error: Virtual environment not found. Run 'make pip-install' first."; \
		exit 1; \
	fi
	$(ACTIVATE) && pytest --continue-on-collection-errors -v -rA --color=yes --html=/tmp/turbineflow-test-report.html -m "not bigquery"

clean:
	@echo "Removing virtual environment..."
	@rm -rf $(VENV)
	@echo "Virtual environment removed"	

