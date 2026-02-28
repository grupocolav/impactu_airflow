# DevOps Strategy for ImpactU Airflow

This document details the professional CI/CD strategy, environment management, and deployment for the `impactu_airflow` monorepo.

## 1. DAG Bundles Architecture (Airflow 3.x)

For the production environment, we use DAG Bundles:
- **DAG Bundles**: This native Airflow 3 feature allows the Scheduler and Webserver to consume DAGs directly from Git. This allows updating DAG logic automatically without restarting services.
- **Dependencies**: All project dependencies are installed directly in the Airflow environment.
- **Logic**: Changes in the `main` branch are automatically reflected via DAG Bundles, and the DAG Processor synchronizes files in real-time.

## 2. Production Strategy

| Environment | Branch | URL | Bundle Name |
| :--- | :--- | :--- | :--- |
| **Production** | `main` | `airflow.colav.co` | `impactu_prod` |

### Workflow
1. Developers perform a **fork** of the official repository.
2. Changes are developed in fork branches and validated locally.
3. A **Pull Request (PR)** is opened from the fork to the `main` branch of the official repository.
4. CI runs automatic tests and, if necessary, a remote validation in the development environment.
5. Once approved and merged into `main`, the Airflow DAG Processor in production detects the update and synchronizes files in real-time.

## 3. Production Server Configuration

To enable automatic synchronization and ensure that internal dependencies (such as the `extract/` package) are importable, the following environment variable must be configured on the production server.

**Note:** We use `"subdir": "."` so that the repository root is added to the `PYTHONPATH`, allowing DAGs to import modules from other folders. An `.airflowignore` file is included to prevent Airflow from attempting to process folders that do not contain DAGs.

```bash
AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_list='[
    {
        "name": "impactu_prod",
        "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
        "kwargs": {
            "git_conn_id": "git_impactu",
            "tracking_ref": "main",
            "refresh_interval": 300,
            "subdir": "."
        }
    }
]'
```

### Requirement: Git Connection
In the Airflow interface (`Admin -> Connections`), a connection with ID `git_impactu` must exist:
- **Conn Id**: `git_impactu`
- **Conn Type**: `Git`
- **Host**: `https://github.com/colav/impactu_airflow.git`

## 4. CI/CD Pipeline (GitHub Actions)

The `.github/workflows/deploy.yml` file manages the lifecycle:

### Test Phase
- **DAG Integrity**: Verifies that all files in `dags/` are parseable by Airflow.
- **Unit Tests**: Runs `pytest` on extractors and transformation logic using `mongomock`.
- **Remote Validation (PRs)**: When a PR is opened, CI notifies the Development Airflow API to run the `pr_validator` DAG. This DAG downloads the PR changes and validates them in the real Dev environment without needing to deploy the full image.

### Deployment Phase
- **PyPI Publication**: When a new release is published on GitHub, a workflow automatically builds and publishes the package to PyPI.
- **Production Sync**: The production server automatically synchronizes the DAG code via the `impactu_prod` bundle.

## 5. Secrets and Configuration Management

### GitHub Secrets
- `PYPI_PASSWORD`: PyPI API token for publishing the package.
- `AIRFLOW_API_TOKEN`: Bearer token for CI to trigger validations on the Development server.
- `GH_TOKEN_PR_VALIDATOR`: GitHub Token (Personal Access Token) with read permissions for the validator DAG to download PR files.
- `GDRIVE_TOKEN_PICKLE_B64`: **Base64-encoded** Google Drive credentials pickle used by capture DAGs that read from Drive (e.g., `dags/staff_capture.py`, `dags/ciarp_capture.py`).  
  **Runtime rule**: the Airflow environment must decode this secret and write it to a file path available to tasks (e.g., `/path/to/pickle_file`). The pickle file must **never** be committed to the repository.

### Airflow (Runtime)
- **Variables**: Non-sensitive configurations (e.g., `scimagojr_cache_dir`, `staff_cache_dir`, `staff_drive_root_folder_id`, `ciarp_drive_root_folder_id`).
- **Connections**: Database credentials (MongoDB, Postgres) and APIs. **Never** hardcode credentials in the code.

## 6. Monitoring and Maintenance

- **Logs**: Centralized in `/storage/airflow/data/dev/logs` and `/storage/airflow/data/prod/logs`.
- **Backups**: MongoDB and Postgres volumes must be backed up periodically (see `backups/` folder).
- **Alerts**: It is recommended to configure `Sentry` or a Slack Notifier in Airflow for critical DAG failures.
