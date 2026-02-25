# DuckLineage Extension for DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

A community extension for [DuckDB](https://duckdb.org/) that automatically captures and emits [OpenLineage](https://openlineage.io/) events for every query executed. This enables automated data lineage, governance, and observability for DuckDB workloads.

## Supported Features

This extension currently implements the following OpenLineage capabilities:

- **Automatic events:** Emits START, COMPLETE and FAIL events for each query execution.
- **Dataset detection:** Extracts input and output datasets from query plans (tables, CREATE/INSERT/UPDATE/DELETE/COPY, basic file table functions).
- **Schema capture:** Records dataset schema (column names and types) as a dataset facet.
- **Facets:** Job `sql` facet, run `parent` facet (via OPENLINEAGE_PARENT_\* env vars), `processing_engine`, `dataSource` and `catalog` facets, lifecycle change facets (CREATE/DROP/ALTER/OVERWRITE/RENAME/TRUNCATE), and basic `outputStatistics` (row count).
- **Asynchronous delivery:** Background HTTP client with configurable OpenLineage URL, API key, retries, queueing and debug logging.

## Quick Start

> Note: This extension is not yet available in the official DuckDB community extension repository. You will need to build it from source (or download from GitHub Actions artifacts).

The first step is to load the extension in DuckDB:

```sql
LOAD 'build/release/duckdb_openlineage.duckdb_extension';
```

Next, configure the OpenLineage backend URL (e.g., Marquez):

```sql
SET openlineage_url='http://localhost:5000/api/v1/lineage';

-- Set API Key (Optional)
-- SET openlineage_api_key='your-api-key';

-- Set Namespace (Default: duckdb)
-- SET openlineage_namespace='my-data-warehouse';

-- Enable Debug Mode (Logs JSON events to console)
-- SET openlineage_debug=true;
```

Execute your analytical queries. The extension will automatically trace them.

```sql
CREATE TABLE users (id INT, name VARCHAR);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT count(*) FROM users;
```

Check your OpenLineage backend (e.g., Marquez UI) to see the lineage graph and run details!

### Parent Run Integration (Airflow/Dagster)

The extension automatically detects standard OpenLineage environment variables to link queries to a parent job run:

- `OPENLINEAGE_PARENT_RUN_ID`
- `OPENLINEAGE_PARENT_JOB_NAMESPACE`
- `OPENLINEAGE_PARENT_JOB_NAME`

If these are set (e.g., by your Airflow operator), the DuckDB query will appear as a child run in the lineage graph.

## Building

### Prerequisites

For building DuckDB extensions, you will need the following tools installed on your system:

- CMake (3.11+)
- A C++11 compliant compiler (GCC/Clang)

Additionally, this extension uses `vcpkg` to manage dependencies. You can omit it, but it will require you to manually install development libraries for OpenSSL, Curl and JSON (on Ubuntu-based systems - `libssl-dev libcurl4-openssl-dev nlohmann-json3-dev`)

### Build steps

Now to build the extension, run:

```sh
make
```

> NOTE: You may additionally need to provide `VCPKG_TOOLCHAIN_PATH` environment variable pointing to the `vcpkg.cmake` file if using `vcpkg`.

The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/extension/openlineage/openlineage.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `openlineage.duckdb_extension` is the loadable binary as it would be distributed.

## Running the tests

### Integration Tests with Marquez

This extension includes comprehensive integration tests that verify lineage events are correctly sent to Marquez (OpenLineage server).

Since we need marquez server running to execute the tests, the tests require Docker with Docker Compose and `UV` to be installed. UV is used to manage python dependencies, as the tests are written in Python.

**Quick start:**

```sh
make test-all
```

See [test/README.md](test/README.md) for detailed testing documentation.

### Installing the deployed binaries

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:

```shell
duckdb -unsigned
```

Python:

```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:

```js
db = new duckdb.Database(":memory:", { allow_unsigned_extensions: "true" });
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:

```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:

```sql
INSTALL openlineage;
LOAD openlineage;
```

# Extension updating

When cloning the template, the target version of DuckDB should be the latest stable release of DuckDB. However, there
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release
  - `./extension-ci-tools` should be set to updated branch corresponding to latest DuckDB release. So if you're building for DuckDB `v1.1.0` there will be a branch in `extension-ci-tools` named `v1.1.0` to which you should check out.
- Bump versions in `./github/workflows`
  - `duckdb_version` input in `duckdb-stable-build` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - `duckdb_version` input in `duckdb-stable-deploy` job in `MainDistributionPipeline.yml` should be set to latest tagged release
  - the reusable workflow `duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml` for the `duckdb-stable-build` job should be set to latest tagged release

# API changes

DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:

- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed

## Setting up CLion

### Opening project

Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging

To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_exentension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
