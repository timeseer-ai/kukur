# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

SHELL=/bin/bash

.PHONY: docs
docs: ## Generate a documentation Docker container
	docker run --rm \
		-u $(shell id -u):$(shell id -g) \
		-v $(shell pwd)/docs:/documents/kukur \
		-v $(shell pwd)/docs/out:/documents/kukur/out \
		asciidoctor/docker-asciidoctor \
		asciidoctor -r asciidoctor-diagram --destination-dir kukur/out --out-file index.html kukur/kukur.asciidoc
	docker build -t kukur-documentation:latest docs/

.PHONY: run-docs
run-docs: docs ## Run the documentation in a docker container on port 8080
	docker run --rm \
		-p 8080:80 \
		kukur-documentation:latest

.PHONY: lint
lint: format ## Lint the kukur code
	flake8 kukur/
	pylint -j 0 --disable=duplicate-code kukur/
	mypy --ignore-missing-imports kukur/

.PHONY: test
test: ## Run the unit tests
	python -m pytest

.PHONY: deps
deps: ## Install runtime dependencies
	pip install -r requirements.txt

.PHONY: dev-deps
dev-deps: ## Install development dependencies
	pip install -r requirements-dev.txt

.PHONY: format
format: ## Format the kukur code
	black kukur/

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
