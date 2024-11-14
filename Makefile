# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

SHELL=/bin/bash

.PHONY: run
run: ## Start Kukur
	python -m kukur.cli

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf build/ dist/

.PHONY: docs
docs: ## Generate a documentation Docker container
	docker run --rm \
		-u $(shell id -u):$(shell id -g) \
		-v $(shell pwd)/docs:/documents/kukur \
		-v $(shell pwd)/docs/out:/documents/kukur/out \
		asciidoctor/docker-asciidoctor \
		asciidoctor -a data-uri -r asciidoctor-diagram --destination-dir kukur/out --out-file index.html kukur/kukur.asciidoc
	docker build -t kukur-documentation:latest docs/

.PHONY: run-docs
run-docs: docs ## Run the documentation in a docker container on port 8080
	docker run --rm \
		-p 8080:80 \
		kukur-documentation:latest

.PHONY: lint
lint: ## Lint the kukur code
	black --check kukur/ tests/
	mypy --ignore-missing-imports kukur/
	ruff check .

.PHONY: test
test: ## Run the unit tests
	python -m pytest --ignore tests/integration

.PHONY: compose
compose: ## Start all containers needed for integration tests
	docker-compose \
		-f tests/test_data/docker-compose-crate.yml \
		-f tests/test_data/docker-compose-elasticsearch.yml \
		-f tests/test_data/docker-compose-influxdb.yml \
		-f tests/test_data/docker-compose-odbc.yml \
		-f tests/test_data/docker-compose-postgres.yml \
	 	up

.PHONY: integration-test
integration-test: ## Run all integration tests (this requires a running Kukur)
	python -m pytest tests/integration

.PHONY: integration-test-crate
integration-test-crate: ## Run CrateDB integration tests
	python -m pytest tests/integration -m crate

.PHONY: integration-test-elasticsearch
integration-test-elasticsearch: ## Run Elasticsearch integration tests
	python -m pytest tests/integration -m elasticsearch

.PHONY: integration-test-influxdb
integration-test-influxdb: ## Run InfluxDB integration tests
	python -m pytest tests/integration -m influxdb

.PHONY: integration-test-kukur
integration-test-kukur: ## Run Kukur (flight) integration tests
	python -m pytest tests/integration -m kukur

.PHONY: integration-test-odbc
integration-test-odbc: ## Run ODBC integration tests
	python -m pytest tests/integration -m odbc

.PHONY: integration-test-postgres
integration-test-postgres: ## Run PostgreSQL integration tests
	python -m pytest tests/integration -m postgresql

.PHONY: deps
deps: ## Install runtime dependencies
	pip install -r requirements.txt

.PHONY: dev-deps
dev-deps: ## Install development dependencies
	pip install -r requirements-dev.txt -r requirements-python.txt

.PHONY: format
format: ## Format the kukur code
	black kukur/ tests/

.PHONY: wheel
wheel: ## Build a Python wheel in dist/
	python setup.py bdist_wheel


.PHONY: build-docker
build-docker: ## Build a Docker container
	docker build -t kukur .

.PHONY: run-docker
run-docker: build-docker ## Run Kukur as a Docker container, with the test data mounted
	docker run --rm \
		-u $(shell id -u):$(shell id -g) \
		-p 8081:8081 \
		-v $(shell pwd)/Kukur.toml:/usr/src/app/Kukur.toml \
		-v $(shell pwd)/db:/usr/src/app/db \
		-v $(shell pwd)/tests/test_data:/usr/src/app/tests/test_data \
		-v $(shell pwd)/data:/usr/src/app/data \
		-it kukur:latest
