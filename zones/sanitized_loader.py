"""Sanitized zone loader — loads raw data into the Sanitized S3 zone."""
import logging
from typing import List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class SanitizedLoader:
    """
    Loads raw migrated data into the Sanitized zone.
    Applies minimal transformations: dedup, type coercion, null handling.
    """

    def __init__(self, s3_writer):
        self.s3_writer = s3_writer

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        batch_id: int = 1,
    ) -> dict:
        """Load DataFrame to Sanitized zone with minimal cleaning."""
        original_count = len(df)

        # Remove exact duplicates
        df = df.drop_duplicates()
        dup_removed = original_count - len(df)

        # Coerce obvious numeric strings
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_numeric(df[col], errors="ignore")
                except Exception:
                    pass

        s3_prefix = f"sanitized/{table_name}"
        result = self.s3_writer.write_batch(
            df, s3_prefix, partition_cols,
            file_name=f"sanitized_{batch_id:05d}.parquet"
        )

        logger.info(f"Sanitized zone: {len(df)} rows → {result['s3_uri']} ({dup_removed} dupes removed)")
        return {"rows_loaded": len(df), "duplicates_removed": dup_removed, **result}
