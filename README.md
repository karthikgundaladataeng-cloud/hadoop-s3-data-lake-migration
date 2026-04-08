# hadoop-s3-data-lake-migration

Enterprise Hadoop HDFS to AWS S3 Data Lake migration toolkit — with data validation, reconciliation, and multi-zone architecture support.

**Stack:** Python · PyArrow · Pandas · Boto3 · SQLite (checkpointing)

## Architecture

```
[HDFS Source]
     |
 HDFSReader          ← reads ORC/Parquet/CSV from HDFS in batches
     |
 MigrationOrchestrator  ← retry logic, parallelism, progress checkpointing
     |
 S3Writer            ← partitioned Parquet with Snappy compression
     |
 [S3 Raw Zone]       → SanitizedLoader → [S3 Sanitized Zone]
                                       → ConformedLoader  → [S3 Conformed Zone]
     |
 ReconciliationValidator  ← row counts, checksums, sample comparison
 SchemaComparator         ← source vs target schema diff
```

## Quick Start

```bash
pip install -r requirements.txt
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...

python -c "
from migrator.migration_orchestrator import MigrationOrchestrator
orch = MigrationOrchestrator('namenode.internal', 'my-datalake-bucket')
results = orch.migrate_directory('/data/raw/orders', 'raw/orders', partition_cols=['year','month'])
print(results)
"
```

## Zone Architecture

| Zone | Description |
|------|-------------|
| Raw | Direct copy of HDFS data — no transformations |
| Sanitized | Deduped, type-coerced, null-handled |
| Conformed | Business rules applied, standardised keys |
