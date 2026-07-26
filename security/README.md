# GRIZL Platform Security

IP-based access controls for all three observability platforms.
Each platform has its own directory with a config template, apply script, and reference SQL/docs.

## Quick start

```
security/
  snowflake/    Network policy (CIS 3.1) — snow CLI
  databricks/   IP Access List — Databricks REST API
  fabric/       Conditional Access — Azure portal (manual)
```

## Snowflake — Network Policy (CIS 3.1)

**Status:** Applied. Resolves `SECURITY_ESSENTIALS_CIS3_1`.

```bash
# 1. Copy and fill in your config
cp security/snowflake/config/grizl.snowflake.security.env.example \
   security/snowflake/config/grizl.snowflake.security.env

# 2. Dry-run first
bash security/snowflake/scripts/network-policy.sh --dry-run apply

# 3. Apply
bash security/snowflake/scripts/network-policy.sh --yes apply

# 4. Verify
bash security/snowflake/scripts/network-policy.sh verify
```

Find your current IP: `curl -s https://checkip.amazonaws.com`

## Databricks — IP Access List

**Status:** Not yet applied. Requires IP Access Lists to be enabled in Admin Console first.

```bash
# 0. Enable in workspace: Admin Console → Security → IP Access Lists → Enable

# 1. Copy and fill in your config
cp security/databricks/config/grizl.databricks.security.env.example \
   security/databricks/config/grizl.databricks.security.env

# 2. Dry-run first
bash security/databricks/scripts/ip-access-list.sh --dry-run apply

# 3. Apply
bash security/databricks/scripts/ip-access-list.sh --yes apply

# 4. Verify
bash security/databricks/scripts/ip-access-list.sh verify
```

## Microsoft Fabric — Conditional Access

**Status:** Not yet applied. Managed via Azure portal — no CLI script.

See [`fabric/README.md`](fabric/README.md) for steps.

## Updating your IP

If your trusted IP changes, update `SNOWFLAKE_TRUSTED_IPS` and/or
`DATABRICKS_TRUSTED_IPS` in your local `.env` files and re-run `apply`.
The scripts use `CREATE OR REPLACE` / upsert semantics — safe to re-run.

## What is NOT in this repo

- Real IP addresses (in `.env` files, which are gitignored)
- Snowflake private key or credentials
- Databricks OAuth tokens
