all: install-env install-dep clean quality run deploy-dash
PIP := pip
POETRY := poetry

install-env:
		${PIP} install -U poetry

install-dep:
		@echo 'Installing dependencies'
		${POETRY} install

tests:
		@echo 'Running test'
		${POETRY} run pytest tests

clean:
		@echo 'Removing python self generated unwanted files'
		find . -name '__pycache__' -exec rm -fr {} +
		find . -name '*pyo' -exec rm -f {} +
		find . -name '*pyc' -exec rm -f {} +
		find . -name 'ipynb_checkpoints' -exec rn -rf {} +

quality:
		@echo 'check for linting, Typing, code formating, safety'
		${POETRY} run tox -e py

run:
		@echo 'Deploying'
		${POETRY} run python main.py

deploy-dash:
		${POETRY} run streamlit run dashboard/app.py &
