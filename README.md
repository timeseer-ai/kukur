# Kukur

Kukur makes time series data and metadata available to the [Apache Arrow](https://arrow.apache.org/) ecosystem.

---
**WARNING**

Kukur is under active development.
Breaking changes to the interfaces are possible.

Kukur uses semantic versioning.
While `< 1.0.0`, changes to the minor version indicate breaking changes.

---

## Usage

Kukur can be used as a Python library or as a standalone application that exposes an Arrow Flight interface.

A [Kukur Docker container](https://hub.docker.com/r/timeseer/kukur) is published to Docker Hub:

```bash
$ docker pull timeseer/kukur
```

## Supported Sources

Multiple types of time series sources are supported:

- ADODB connections (including OLEDB)
- Apache Feather files
- Apache Parquet files
- CSV files
- InfluxDB databases
- Other Kukur or Timeseer instances
- ODBC data sources

Check the [documentation](https://kukur.timeseer.ai) for more info

## Contributing

Kukur welcomes contributions.
For small fixes, just open a pull request on GitHub.
Please discuss major changes in a GitHub issue first.

Each file in Kukur requires an [SPDX](https://spdx.dev/) License Identifier and Copyright Text:

```python
# SPDX-FileCopyrightText: 2021 <you/your company>
# SPDX-License-Identifier: Apache-2.0
```

Community interactions are governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Copyright 2021 [Timeseer.AI](https://www.timeseer.ai)

```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Development

Create a virtualenv and install dependencies:

```bash
$ python -m venv venv
$ source venv/bin/activate
(venv) $ make deps dev-deps
```

Launch Kukur using:

```bash
(venv) $ python -m kukur.cli
```

Lint and format:

```bash
(venv) $ make lint
```

Kukur uses [black](https://github.com/psf/black) to format all code.

Run unit tests:

```bash
(venv) $ make test
```

### Integration Tests

Kukur runs integration tests against real databases where possible.

OS requirements to complete the integration tests are:

- unixodbc
- freetds

Additional Python packages are also required:

```bash
(venv) $ pip install -r requirements-python.txt
```

Integration tests require Kukur to be running with a special configuration.
Some time series databases need to be started using docker-compose.

```bash
$ docker-compose -f tests/test_data/docker-compose.yml up -d
(venv) $ python -m kukur.cli --config-file tests/test_data/Kukur.toml
```

Since the location of the freetds libraries varies and this needs to be hardcoded in the unixodbc configuration,
several configuration profiles exist.
These profiles can be selected using the `KUKUR_INTEGRATION_TARGET` environment variable:

- `/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so`: the default, no `KUKUR_INTEGRATION_TARGET` required
- `/usr/lib/libtdsodbc.so`: use `KUKUR_INTEGRATION_TARGET=linux`
- `/usr/local/lib/libtdsodbc.so`: use `KUKUR_INTEGRATION_TARGET=local`

Then, run the tests:

```bash
(venv) $ KUKUR_INTEGRATION_TARGET=linux make integration-test
```

Stop the databases using:

```bash
$ docker-compose -f tests/test_data/docker-compose.yml down --volumes
```

Alternatively, run Kukur in docker-compose as well to have a known stable setup:

```bash
$ docker-compose -f tests/test_data/docker-compose.yml -f tests/test_data/docker-compose.container.yml up -d
```

or build the Kukur container to have the latest Kukur image:

```bash
$ docker-compose -f tests/test_data/docker-compose.yml -f tests/test_data/docker-compose.container.yml up --build
```

Run the tests using:

```bash
(venv) $ make integration-test
```

Stop all containers:

```bash
$ docker-compose -f tests/test_data/docker-compose.yml -f tests/test_data/docker-compose.container.yml down --volumes
```
