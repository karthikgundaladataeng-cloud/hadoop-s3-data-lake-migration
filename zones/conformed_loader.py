"""Conformed zone loader — applies business rules for the Conformed zone."""
import logging
from typing import Dict, Callable, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class ConformedLoader:
    """
    Loads data into the Conformed zone with business rule transformations.
    Conformed = cleansed, standardised, business-key aligned data.
    """

    def __init__(self, s3_writer, business_rules: Dict[str, Callable] = None):
        self.s3_writer = s3_writer
        self.business_rules = business_rules or {}

    def add_rule(self, column: str, transform_fn: Callable) -> None:
        """Register a column-level business transformation function."""
        self.business_rules[column] = transform_fn

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        batch_id: int = 1,
    ) -> dict:
        """Apply business rules and load to Conformed zone."""
        df = df.copy()
        applied_rules = []

        for col, transform_fn in self.business_rules.items():
            if col in df.columns:
                try:
                    df[col] = df[col].apply(transform_fn)
                    applied_rules.append(col)
                except Exception as e:
                    logger.warning(f"Rule failed for column '{col}': {e}")

        # Add conformed timestamp
        df["_conformed_ts"] = pd.Timestamp.utcnow()

        s3_prefix = f"conformed/{table_name}"
        result = self.s3_writer.write_batch(
            df, s3_prefix, partition_cols,
            file_name=f"conformed_{batch_id:05d}.parquet"
        )

        logger.info(f"Conformed zone: {len(df)} rows, {len(applied_rules)} rules applied → {result['s3_uri']}")
        return {"rows_loaded": len(df), "rules_applied": applied_rules, **result}
