# GRIZL Databricks Provisioning

Provisioning helpers, config templates, and manifests for the GRIZL Databricks
observability stack.

## Directory structure

```
databricks/
├── databricks.yml             Asset Bundle: DLT pipeline + Workflow job
├── config/
│   └── grizl.databricks.env.example
├── scripts/
│   ├── lib.sh                 Shared utilities (auth, logging, JSON helpers)
│   ├── check.sh               Local validation (syntax, manifests, SQL files)
│   ├── preflight.sh           Pre-run checks (CLI, auth, warehouse)
│   ├── provision.sh           Create catalog, schema, volume
│   ├── sql-exec.sh            Execute SQL against a Databricks SQL warehouse
│   ├── genie-mgmt.sh          List, inspect, and smoke-test the Genie space
│   ├── monitor-mgmt.sh        Inspect and refresh the Lakehouse Monitor
│   └── teardown.sh            Drop views, tables, and schemas
└── manifests/
    ├── genie-space-settings.example.json
    ├── lakehouse-monitor-config.example.json
    └── manual-resources.json
```

## Setup sequence

### 1. Configure

```bash
cp config/grizl.databricks.env.example config/grizl.databricks.env
# Edit config/grizl.databricks.env
```

### 2. Authenticate

```bash
databricks auth login --host https://<YOUR_WORKSPACE>.cloud.databricks.com
```

### 3. Validate

```bash
npm run check
npm run preflight
```

### 4. Provision Unity Catalog resources

```bash
npm run provision:dry-run
npm run provision
```

### 5. Apply SQL views

```bash
npm run sql:observability:dry-run
npm run sql:observability
```

### 6. Apply anomaly signal views

```bash
npm run sql:anomaly-signals:dry-run
npm run sql:anomaly-signals
```

### 7. Deploy DLT pipeline and Workflow job

```bash
cd ..
databricks bundle deploy --target dev
```

Start the DLT pipeline in the Databricks UI:
Workflows → Delta Live Tables → grizl-autoloader-pipeline → Start

### 8. Complete manual steps

See `manifests/manual-resources.json` for the resources that must be created in the UI:
- Genie space configuration (AI/BI → Genie)
- Lakehouse Monitor (run `notebooks/02_lakehouse_monitor_setup.py`)
- SQL Alerts and webhook destination
- Databricks service principal

## Authentication

All scripts authenticate using `databricks auth token`, which uses the current
Databricks CLI profile. This is equivalent to `az account get-access-token` in
the Fabric version. For CI/CD, set:

```bash
export DATABRICKS_HOST=https://...
export DATABRICKS_TOKEN=<pat-or-oauth-token>
```

Or use the service principal OAuth M2M flow — the CLI handles both.

## Scripts reference

| Script | Purpose |
|---|---|
| `check.sh` | Syntax-check all scripts, validate JSON manifests, confirm SQL/notebook files exist, scan for credential material |
| `preflight.sh` | Verify CLI tools, auth, warehouse ID, and catalog access before provisioning |
| `provision.sh` | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME` via SQL warehouse |
| `sql-exec.sh` | Execute a SQL statement or file against a SQL warehouse |
| `genie-mgmt.sh` | List Genie spaces, get space details, send a smoke-test question |
| `monitor-mgmt.sh` | Get monitor status, trigger refresh, list runs |
| `teardown.sh` | Drop views, optional table/schema/catalog cleanup |

## Teardown flags

`teardown.sh` is conservative by default — it only drops views, not tables or schemas.
To drop everything, set these env vars before running `teardown.sh --yes`:

```bash
DATABRICKS_DROP_RAW_LOGS=true   # drop raw_logs Delta table (deletes all log data)
DATABRICKS_DROP_SCHEMAS=true    # drop observability and observability_monitors schemas
DATABRICKS_DROP_CATALOG=true    # drop grizl catalog (drops everything inside it)
```

The DLT pipeline, Workflow job, Genie space, and SQL Alerts must be deleted
manually in the Databricks UI.
