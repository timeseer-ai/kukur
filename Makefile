# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

SHELL=/bin/bash

.PHONY: docs
docs: ## Generate a documentation Docker container
	docker run --rm \
		-u $(shell id -u):$(shell id -g) \
		-v $(shell pwd)/docs:/documents/kukur \
		-v $(shell pwd)/docs/source/:/documents/source \
		-v $(shell pwd)/README.asciidoc:/documents/README.asciidoc \
		-v $(shell pwd)/docs/out:/documents/kukur/out \
		asciidoctor/docker-asciidoctor \
		asciidoctor -r asciidoctor-diagram --destination-dir kukur/out --out-file index.html kukur/kukur.asciidoc
	docker build -t kukur-documentation:latest docs/

.PHONY: run-docs
run-docs: docs ## Run the documentation in a docker container on port 8080
	docker run --rm \
		-p 8080:80 \
		kukur-documentation:latest
