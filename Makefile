venv/bin/python: requirements.txt
	virtualenv -p python3 venv
	venv/bin/pip install -r requirements.txt

venv: venv/bin/python

test: venv
	venv/bin/python -m tests

lint: venv
	venv/bin/flake8 tpq

