# ATS-GenAI Project Overview

## Repository Index
- **Top-level**: `pyproject.toml`, `requirements.txt`, `pytest.ini`, `.env*`, `ats.context`, `Dockerfile*`, `docker-compose.yml`
- **Code**: `src/`
  - **Secmaster jobs**: `src/secmaster/` (e.g., `populate_instrument_polygon.py`)
  - **DB & migrations**: `src/db/` (primary: `migration_manager.py`, helper: `environment_migration.py`, SQLs in `src/db/migrations/`)
  - **DAOs**: `src/dao/*.py` (e.g., `db_version_dao.py`, `instrument_polygon_dao.py`)
  - **Config**: `src/config/` (`environment.py`, `database.py`)
  - Other domains: `calendars/`, `universe/`, `market_data/`, `state/`, etc.
- **Kubernetes**: `k8s/`
  - **Dev env**: `k8s/dev/` (e.g., `instrument-agent-job.yaml`, DB jobs)
  - **Generated**: `k8s/generated/` (output from generators)
  - **Job template**: `k8s/instrument-polygon-job.yaml`
  - **ArgoCD**: `k8s/argocd/`
- **Scripts**: `scripts/`
  - **Kubernetes tooling**: `scripts/kubernetes/`
    - Job generator: `k8s_job_generator.py`
    - Instrument polygon generator: `instrument_polygon_job_generator.py`
    - Direct runner: `run_k8s_job.py`
    - DB ops: `manage_k8s_database.py`
  - **Flyte**: `scripts/flyte/` (`flyte_instrument_polygon_workflow.py`, `flyte_db_connection_test.py`)
  - **Database**: `scripts/database/` (inspect, verify, migrate helpers)
- **Tests**: `tests/` (env-aware, `PYTHONPATH=src`, fixtures)
- **Docs**: `docs/` (deployment, cluster, DB setup, Flyte usage)

## Current Focus Areas
- **Ray in Kubernetes Jobs**
  - Ray usage in `src/secmaster/populate_instrument_polygon.py`.
  - Job generator sets Ray env to suppress autoscaler and avoid cluster scaling:
    - `RAY_SCHEDULER_EVENTS=0`
    - `RAY_DISABLE_AUTOMATIC_AUTOSCALING=1`
  - Flyte workflow (`scripts/flyte/flyte_instrument_polygon_workflow.py`) consumes the generator.

- **Postgres Migration (dev_db)**
  - Primary entry: `src/db/migration_manager.py` (handles `db_version` and migrations under `src/db/migrations/`).
  - Supporting utilities: `src/db/environment_migration.py` and scripts under `scripts/database/` (for inspection and environment structuring, if needed).

## Kubernetes Job Tooling
- **Generate job YAML**: `scripts/kubernetes/k8s_job_generator.py`
- **Run job directly**: `scripts/kubernetes/run_k8s_job.py` (supports dry-run/apply; see `README_k8s_job_runner.md`).
- **Parameterized instrument jobs**: `scripts/kubernetes/instrument_polygon_job_generator.py` (examples in `demo_job_generator.py`).

## Configuration Notes
- **DB connection fixes in jobs**:
  - `PYTHONPATH=src`
  - `DB_CONNECTION_PARAMS=sslmode=disable`
  - DB credentials provided via K8s Secrets.
- **Environment-aware tables**: Use `env.get_table_name("<base>")` from `src/config/environment.py`.

## Quick Commands (uv-based)
- Install deps:
  ```bash
  uv pip install -r requirements.txt
  ```
- Run tests:
  ```bash
  PYTHONPATH=src uv run pytest -q
  ```
- Generate a dev backfill job YAML:
  ```bash
  uv run python scripts/kubernetes/k8s_job_generator.py --job-type backfill --output k8s/generated/instrument-polygon-backfill.yaml
  ```
- Apply a generated job:
  ```bash
  kubectl apply -f k8s/generated/instrument-polygon-backfill.yaml
  ```
- Flyte workflow (locally invoke generator path):
  ```bash
  uv run python scripts/flyte/flyte_instrument_polygon_workflow.py --job-type backfill --apply --output-dir k8s/generated
  ```
- Inspect DB connection (dev):
  ```bash
  uv run python scripts/database/test_ats_dev_db_connection.py
  ```

## Next Steps
- **Ray/Flyte**: Keep Ray init in local/non-autoscaling mode for K8s jobs; verify logs are clean in backfill/test runs.
- **DB Migration**: Use `src/db/migration_manager.py` to migrate `dev_db` to the latest `db_version` and verify via `db_version_dao.py`.
- **Monitoring**: Add/verify logging for instrument polygon ingestion and DB writes.
