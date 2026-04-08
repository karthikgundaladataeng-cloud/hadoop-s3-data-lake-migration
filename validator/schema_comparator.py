"""Schema comparator — compares source and target schemas post-migration."""
import logging
from typing import Dict, Any
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class SchemaComparator:
    """Compares Parquet schemas between HDFS source and S3 target."""

    def compare(self, source_schema: Dict[str, str], target_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Compare two schema dicts (column_name → dtype) and report differences.
        """
        src_cols = set(source_schema.keys())
        tgt_cols = set(target_schema.keys())

        missing = list(src_cols - tgt_cols)
        extra = list(tgt_cols - src_cols)
        type_mismatches = {
            col: {"source": source_schema[col], "target": target_schema[col]}
            for col in (src_cols & tgt_cols)
            if source_schema[col] != target_schema[col]
        }

        passed = not missing and not extra and not type_mismatches
        return {
            "schemas_match": passed,
            "missing_in_target": missing,
            "extra_in_target": extra,
            "type_mismatches": type_mismatches,
        }

    def extract_parquet_schema(self, parquet_path: str, filesystem=None) -> Dict[str, str]:
        """Extract column name → dtype from a Parquet file schema."""
        schema = pq.read_schema(parquet_path, filesystem=filesystem)
        return {field.name: str(field.type) for field in schema}
