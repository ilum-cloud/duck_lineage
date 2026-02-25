# Testing DuckDB OpenLineage Extension

This directory contains integration tests for the DuckDB OpenLineage extension using pytest and the Marquez API.

> Why write tests in Python?
>
> In general the tests need to check whether the lineage events are correctly sent to Marquez (OpenLineage server) after executing SQL queries in DuckDB with the OpenLineage extension loaded. It is impossible to do so using only SQL tests, so we use Python tests with DuckDB's Python API and pytest framework.

## Overview

The test suite:

- Starts Marquez (OpenLineage server) using Docker Compose
- Loads the DuckDB OpenLineage extension
- Executes various SQL queries
- Verifies that lineage events are correctly sent to and stored in Marquez

## Prerequisites

To run the tests, ensure you have the following installed:

- UV (Python package manager)
- Docker and Docker Compose
- Everything needed to build the DuckDB extension (CMake, a C++ compiler, etc.)

## Quick Start

From the main directory:

```bash
make test-all
```
