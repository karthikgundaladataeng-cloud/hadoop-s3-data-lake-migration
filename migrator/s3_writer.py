"""S3 writer — writes to S3 with partitioning, compression, and format conversion."""
import logging
from io import BytesIO
from typing import Dict, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class S3Writer:
    """Writes Parquet files to S3 with partition paths and compression."""

    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.region = region
        self._client = None

    def _get_client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def write_batch(
        self,
        df: pd.DataFrame,
        s3_prefix: str,
        partition_cols: Optional[list] = None,
        compression: str = "snappy",
        file_name: str = "data.parquet",
    ) -> Dict[str, Any]:
        """
        Write a DataFrame to S3 as Parquet.

        For ORC → Parquet conversion: pass the DataFrame from HDFSReader (which converts ORC to pandas).

        Returns dict with s3_uri and rows_written.
        """
        table = pa.Table.from_pandas(df, preserve_index=False)
        buffer = BytesIO()

        if partition_cols:
            # Build partitioned path
            partition_parts = "/".join([
                f"{col}={df[col].iloc[0]}" for col in partition_cols if col in df.columns
            ])
            s3_key = f"{s3_prefix}/{partition_parts}/{file_name}"
        else:
            s3_key = f"{s3_prefix}/{file_name}"

        pq.write_table(table, buffer, compression=compression)
        buffer.seek(0)

        client = self._get_client()
        client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        s3_uri = f"s3://{self.bucket}/{s3_key}"
        logger.info(f"Written {len(df)} rows → {s3_uri}")
        return {"s3_uri": s3_uri, "rows_written": len(df), "size_bytes": len(buffer.getvalue())}

    def write_checkpoint(self, run_id: str, hdfs_path: str, status: str) -> None:
        """Record migration progress as an S3 checkpoint file."""
        key = f"_migration_checkpoints/{run_id}/{hdfs_path.replace('/', '_')}.json"
        import json
        from datetime import datetime
        payload = json.dumps({"hdfs_path": hdfs_path, "status": status, "ts": datetime.utcnow().isoformat()})
        self._get_client().put_object(Bucket=self.bucket, Key=key, Body=payload.encode())
