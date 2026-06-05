# Configuration
AIRFLOW_VERSION ?= 2.10.5
AIRFLOW2_VERSION ?= 2.10.5
AIRFLOW3_VERSION ?= 3.1.7
VENV := .venv
VENV_AIRFLOW2 := .venv-airflow-$(AIRFLOW2_VERSION)
VENV_AIRFLOW3 := .venv-airflow-$(AIRFLOW3_VERSION)
AIRFLOW2_READY := $(VENV_AIRFLOW2)/.airflow-bdd-ready
AIRFLOW3_READY := $(VENV_AIRFLOW3)/.airflow-bdd-ready
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

.PHONY: help venv venv-airflow2 venv-airflow3 pip-install pip-install-airflow pip-install-other pip-install-airflow2 pip-install-airflow3 test test-html test-ci test-airflow2 test-airflow3 test-all clean check-python setup-python

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  setup-python      to ensure Python 3.11 is installed via pyenv"
	@echo "  venv              to create a virtual environment using venv"
	@echo "  venv-airflow2     to create the Airflow 2.10.5 virtual environment"
	@echo "  venv-airflow3     to create the Airflow 3.1.7 virtual environment"
	@echo "  pip-install       to install all dependencies (Airflow + others)"
	@echo "  pip-install-airflow  to install Airflow with constraints"
	@echo "  pip-install-other    to install other dependencies without constraints"
	@echo "  pip-install-airflow2 to install Airflow 2.10.5 into its own venv"
	@echo "  pip-install-airflow3 to install Airflow 3.1.7 into its own venv"
	@echo "  test              to run the tests, using some BDD helper functions"
	@echo "  test-html         to run the tests, showing a HTML report"
	@echo "  test-ci           to run the tests for CI. Skipping BigQuery tests"
	@echo "  test-airflow2     to install and run the Airflow 2.10.5 CI lane"
	@echo "  test-airflow3     to install and run the Airflow 3.1.7 CI lane"
	@echo "  test-all          to run both Airflow 2.10.5 and 3.1.7 CI lanes"
	@echo "  check-python      to check Python version and constraint URL"
	@echo "  clean             to remove virtual environment"
	@echo "  help              to show this message"
	@echo ""
	@echo "Configuration:"
	@echo "  AIRFLOW_VERSION=$(AIRFLOW_VERSION)"
	@echo "  AIRFLOW2_VERSION=$(AIRFLOW2_VERSION)"
	@echo "  AIRFLOW3_VERSION=$(AIRFLOW3_VERSION)"
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
	$(ACTIVATE) && pip install "apache-airflow[google]==$(AIRFLOW_VERSION)" --constraint "$(CONSTRAINT_URL)" || \
		(echo "Error: Failed to install Airflow. Check Python version compatibility." && exit 1)

# Install other dependencies without constraints (as per Airflow best practices)
pip-install-other: venv
	@echo "Installing other dependencies..."
	$(ACTIVATE) && pip install --upgrade pip
	$(ACTIVATE) && pip install pytest pytest-html PyHamcrest || \
		(echo "Error: Failed to install dependencies." && exit 1)
	$(ACTIVATE) && pip install -e . || \
		(echo "Error: Failed to install local package." && exit 1)

# Install everything: Airflow with constraints, then other deps
pip-install: pip-install-airflow pip-install-other
	@echo ""
	@echo "Installation complete!"
	@echo "Run 'make test' to run tests"

venv-airflow2:
	@$(MAKE) AIRFLOW_VERSION=$(AIRFLOW2_VERSION) VENV=$(VENV_AIRFLOW2) venv

venv-airflow3:
	@$(MAKE) AIRFLOW_VERSION=$(AIRFLOW3_VERSION) VENV=$(VENV_AIRFLOW3) venv

pip-install-airflow2:
	@$(MAKE) $(AIRFLOW2_READY)

pip-install-airflow3:
	@$(MAKE) $(AIRFLOW3_READY)

$(AIRFLOW2_READY): venv-airflow2
	@if [ -x "$(VENV_AIRFLOW2)/bin/python" ] && $(VENV_AIRFLOW2)/bin/python -c "import airflow, airflow_bdd, pytest" >/dev/null 2>&1; then \
		echo "Reusing Airflow 2.10.5 venv at $(VENV_AIRFLOW2)"; \
	else \
		$(MAKE) AIRFLOW_VERSION=$(AIRFLOW2_VERSION) VENV=$(VENV_AIRFLOW2) pip-install; \
	fi
	@touch $@

$(AIRFLOW3_READY): venv-airflow3
	@if [ -x "$(VENV_AIRFLOW3)/bin/python" ] && $(VENV_AIRFLOW3)/bin/python -c "import airflow, airflow_bdd, pytest" >/dev/null 2>&1; then \
		echo "Reusing Airflow 3.1.7 venv at $(VENV_AIRFLOW3)"; \
	else \
		$(MAKE) AIRFLOW_VERSION=$(AIRFLOW3_VERSION) VENV=$(VENV_AIRFLOW3) pip-install; \
	fi
	@touch $@

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

test-airflow2:
	@$(MAKE) $(AIRFLOW2_READY)
	@$(MAKE) AIRFLOW_VERSION=$(AIRFLOW2_VERSION) VENV=$(VENV_AIRFLOW2) test-ci

test-airflow3:
	@$(MAKE) $(AIRFLOW3_READY)
	@$(MAKE) AIRFLOW_VERSION=$(AIRFLOW3_VERSION) VENV=$(VENV_AIRFLOW3) test-ci

test-all: test-airflow2 test-airflow3

clean:
	@echo "Removing virtual environment..."
	@rm -rf $(VENV) $(VENV_AIRFLOW2) $(VENV_AIRFLOW3)
	@echo "Virtual environment removed"	
