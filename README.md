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
