"""Reconciliation validator — row count, checksum, sample-based comparison."""
import logging
import hashlib
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class ReconciliationValidator:
    """
    Validates data integrity between HDFS source and S3 target post-migration.
    Performs row count, checksum, and sample-based spot checks.
    """

    def validate(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: list,
        table_name: str = "unknown",
    ) -> Dict[str, Any]:
        """
        Run full reconciliation between source and target DataFrames.

        Args:
            source_df: Original HDFS data.
            target_df: Migrated S3 data (read back as Parquet).
            key_columns: Business key columns for row-level matching.
            table_name: Table name for reporting.

        Returns:
            Reconciliation report dict.
        """
        results = {"table_name": table_name, "checks": {}}

        # 1. Row count check
        source_count = len(source_df)
        target_count = len(target_df)
        row_match = source_count == target_count
        results["checks"]["row_count"] = {
            "passed": row_match,
            "source": source_count,
            "target": target_count,
            "diff": abs(source_count - target_count),
        }

        # 2. Column check
        source_cols = set(source_df.columns)
        target_cols = set(target_df.columns)
        col_match = source_cols == target_cols
        results["checks"]["columns"] = {
            "passed": col_match,
            "missing_in_target": list(source_cols - target_cols),
            "extra_in_target": list(target_cols - source_cols),
        }

        # 3. Checksum check on key columns
        if key_columns and all(c in source_df.columns and c in target_df.columns for c in key_columns):
            src_checksum = self._compute_checksum(source_df, key_columns)
            tgt_checksum = self._compute_checksum(target_df, key_columns)
            results["checks"]["checksum"] = {
                "passed": src_checksum == tgt_checksum,
                "source_checksum": src_checksum,
                "target_checksum": tgt_checksum,
            }

        # 4. Sample check (first 100 rows)
        sample_size = min(100, source_count, target_count)
        if sample_size > 0 and col_match:
            sample_match = self._compare_samples(source_df, target_df, key_columns, sample_size)
            results["checks"]["sample_match"] = {"passed": sample_match, "sample_size": sample_size}

        results["overall_passed"] = all(c.get("passed", False) for c in results["checks"].values())
        return results

    def _compute_checksum(self, df: pd.DataFrame, key_cols: list) -> str:
        """Compute MD5 checksum over sorted key column values."""
        sorted_keys = df[key_cols].dropna().sort_values(by=key_cols).astype(str)
        concatenated = sorted_keys.to_csv(index=False).encode("utf-8")
        return hashlib.md5(concatenated).hexdigest()

    def _compare_samples(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame, key_cols: list, n: int
    ) -> bool:
        """Compare first N rows from both DataFrames after sorting by key columns."""
        try:
            if key_cols:
                src = source_df.sort_values(by=key_cols).head(n).reset_index(drop=True)
                tgt = target_df.sort_values(by=key_cols).head(n).reset_index(drop=True)
            else:
                src = source_df.head(n).reset_index(drop=True)
                tgt = target_df.head(n).reset_index(drop=True)
            common_cols = list(set(src.columns) & set(tgt.columns))
            return src[common_cols].equals(tgt[common_cols])
        except Exception as e:
            logger.warning(f"Sample comparison error: {e}")
            return False
