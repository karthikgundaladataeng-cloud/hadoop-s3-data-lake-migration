"""Migration orchestrator — batch migration with retry logic and progress tracking."""
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

from migrator.hdfs_reader import HDFSReader
from migrator.s3_writer import S3Writer
from utils.progress_tracker import ProgressTracker

logger = logging.getLogger(__name__)


class MigrationOrchestrator:
    """
    Orchestrates the migration of HDFS files to S3.
    Supports batched migration, retry on failure, and progress checkpointing.
    """

    def __init__(
        self,
        hdfs_host: str,
        s3_bucket: str,
        run_id: str = None,
        max_retries: int = 3,
        batch_size: int = 100_000,
        parallelism: int = 4,
    ):
        self.reader = HDFSReader(hdfs_host)
        self.writer = S3Writer(s3_bucket)
        self.tracker = ProgressTracker(run_id or f"migration_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.parallelism = parallelism

    def migrate_path(
        self,
        hdfs_source: str,
        s3_prefix: str,
        partition_cols: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Migrate a single HDFS path to S3.

        Returns:
            Stats dict with rows_migrated, batches_written, duration_seconds.
        """
        run_status = self.tracker.get_status(hdfs_source)
        if run_status == "complete":
            logger.info(f"Skipping already migrated path: {hdfs_source}")
            return {"hdfs_source": hdfs_source, "skipped": True}

        logger.info(f"Migrating: {hdfs_source} → s3://{self.writer.bucket}/{s3_prefix}")
        start = time.time()
        total_rows = 0
        batch_num = 0
        errors = []

        try:
            for batch_df in self.reader.read_path(hdfs_source, self.batch_size):
                batch_num += 1
                attempt = 0
                while attempt < self.max_retries:
                    try:
                        file_name = f"batch_{batch_num:05d}.parquet"
                        self.writer.write_batch(batch_df, s3_prefix, partition_cols, file_name=file_name)
                        total_rows += len(batch_df)
                        logger.info(f"Batch {batch_num}: {len(batch_df)} rows → {s3_prefix}/{file_name}")
                        break
                    except Exception as e:
                        attempt += 1
                        logger.warning(f"Batch {batch_num} attempt {attempt} failed: {e}")
                        if attempt == self.max_retries:
                            errors.append(str(e))
                        else:
                            time.sleep(2 ** attempt)

            elapsed = round(time.time() - start, 2)
            status = "complete" if not errors else "partial"
            self.tracker.update(hdfs_source, status, total_rows)
            self.writer.write_checkpoint(self.tracker.run_id, hdfs_source, status)

            return {
                "hdfs_source": hdfs_source,
                "s3_prefix": s3_prefix,
                "rows_migrated": total_rows,
                "batches_written": batch_num,
                "duration_seconds": elapsed,
                "status": status,
                "errors": errors,
            }
        except Exception as e:
            logger.error(f"Migration failed for {hdfs_source}: {e}")
            self.tracker.update(hdfs_source, "failed", 0)
            return {"hdfs_source": hdfs_source, "status": "failed", "error": str(e)}

    def migrate_directory(
        self,
        hdfs_dir: str,
        s3_base_prefix: str,
        partition_cols: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Migrate all files under an HDFS directory to S3."""
        paths = self.reader.list_paths(hdfs_dir, recursive=True)
        logger.info(f"Found {len(paths)} files to migrate from {hdfs_dir}")
        results = []
        for path in paths:
            relative = path.replace(hdfs_dir, "").lstrip("/")
            s3_prefix = f"{s3_base_prefix}/{'/'.join(relative.split('/')[:-1])}"
            result = self.migrate_path(path, s3_prefix, partition_cols)
            results.append(result)
        return results
