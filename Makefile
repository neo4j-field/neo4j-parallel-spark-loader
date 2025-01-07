.PHONY: all format lint test tests free_tests integration_tests help format

# Default target executed when no arguments are given to make.
all: help

benchmark:
	docker compose -f benchmarking/docker-compose.yml up -d	
	poetry run python3 benchmarking/main.py
	docker compose -f benchmarking/docker-compose.yml stop

coverage:
	poetry run coverage run -m pytest tests/unit
	poetry run coverage report --fail-under=85

test:
	poetry run pytest tests

test_local:
	docker compose -f tests/integration/docker-compose.yml up -d	
	poetry run pytest tests
	docker compose -f tests/integration/docker-compose.yml stop

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
	@echo 'coverage.................... - run coverage report of unit tests'
	@echo 'format...................... - run code formatters'
	@echo 'test........................ - run all unit and integration tests without setting up Neo4j docker instance'
	@echo 'test_local.................. - run all unit and integration tests and set up Neo4j docker instance'
	@echo 'test_integration_local...... - run all integration tests and set up Neo4j docker instance'
	@echo 'test_unit................... - run all free unit tests'
