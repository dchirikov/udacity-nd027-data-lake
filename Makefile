.ONESHELL:
createenv:
	set -auxo pipefail
	python3.6 -m venv ./.venv
	source ./.venv/bin/activate
	pip3.6 install --upgrade pip
	pip3.6 install -r requirements.txt

.ONESHELL:
pep8: createenv
	source ./.venv/bin/activate
	flake8 *.py
	pylint *.py

