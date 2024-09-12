help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  venv          to create a virtual environment using venv"
	@echo "  pip-install   to install the dependencies using pip"
	@echo "  test          to run the tests, using some BDD helper functions"
	@echo "  help          to show this message"

# Target for creating a virtual environment using venv
venv:
	python -m venv .venv

pip-install: venv
	. .venv/bin/activate && pip install --upgrade pip -r requirements.txt

test: 	
	. .venv/bin/activate && pytest --continue-on-collection-errors -v -rA --color=yes

