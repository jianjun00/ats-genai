# Flyte Workflows

This directory contains Flyte workflow definitions and tasks for the ATS-GenAI project.

## Key Scripts

- **flyte_db_connection_test.py** - Flyte workflow for testing database connections with various parameters
- **flyte_instrument_polygon_workflow.py** - Flyte workflow for instrument polygon operations

## Usage

These workflows can be registered with a Flyte backend and executed either locally or in a Kubernetes cluster. They provide a more maintainable and programmatic approach to common tasks compared to raw Kubernetes jobs.
