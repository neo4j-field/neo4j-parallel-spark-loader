.PHONY: all format lint test tests free_tests integration_tests help format

# Default target executed when no arguments are given to make.
all: help


coverage:
	poetry run coverage run -m pytest tests/unit
	poetry run coverage report --fail-under=85

test:
	poetry run pytest tests

test_integration_local:
	docker compose -f tests/integration/docker-compose.yml up -d
	poetry run pytest tests/integration -s
	docker compose -f tests/integration/docker-compose.yml stop

test_unit:
	poetry run pytest tests/unit

######################
# LINTING AND FORMATTING
######################

format:
	poetry run ruff format
	poetry run ruff check --select I . --fix

######################
# HELP
######################

help:
	@echo '----'
	@echo 'init........................ - initialize the repo for development (must still install Graphviz separately)'
	@echo 'coverage.................... - run coverage report of unit tests'
	@echo 'format...................... - run code formatters'
	@echo 'test........................ - run all unit and integration tests'
	@echo 'test_unit................... - run all free unit tests'
	@echo 'test_integration............ - run all integration tests'