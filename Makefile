venv/bin/python: requirements.txt
	virtualenv -p python3 venv
	venv/bin/pip install -r requirements.txt

venv: venv/bin/python

test: venv
	venv/bin/python -m tests

lint: venv
	venv/bin/flake8 tpq

dependencies:
	pip install -r requirements.txt
	pip install flake8
	pip install coverage
	pip install coveralls

travis:
	flake8 tpq
	coverage run tests.py

coveralls:
	coveralls -v

sdist:
	python setup.py sdist

release:
ifndef VERSION
	@echo "Set VERSION, ex: VERSION=1.XX make release"
else
	git tag ${VERSION}
	git push --tags
endif

