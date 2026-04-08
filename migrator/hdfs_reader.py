"""HDFS reader — reads data from HDFS using PyArrow filesystem API."""
import logging
from typing import Iterator, List, Optional
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)


class HDFSReader:
    """Reads data from HDFS with format detection and batch support."""

    SUPPORTED_FORMATS = {"orc", "parquet", "csv", "json", "avro"}

    def __init__(self, hdfs_host: str = "namenode", hdfs_port: int = 8020):
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self._fs = None

    def _get_filesystem(self):
        if self._fs is None:
            try:
                import pyarrow.fs as pa_fs
                self._fs = pa_fs.HadoopFileSystem(
                    host=self.hdfs_host,
                    port=self.hdfs_port,
                )
                logger.info(f"Connected to HDFS: {self.hdfs_host}:{self.hdfs_port}")
            except Exception as e:
                logger.error(f"HDFS connection failed: {e}")
                raise
        return self._fs

    def read_path(self, hdfs_path: str, batch_size: int = 100_000) -> Iterator[pd.DataFrame]:
        """
        Read HDFS path into pandas DataFrames, yielding batches.
        Supports ORC, Parquet, CSV, and JSON formats.
        """
        fmt = self._detect_format(hdfs_path)
        logger.info(f"Reading {hdfs_path} as {fmt.upper()}")

        try:
            fs = self._get_filesystem()
            if fmt == "parquet":
                yield from self._read_parquet(fs, hdfs_path, batch_size)
            elif fmt == "orc":
                yield from self._read_orc(fs, hdfs_path, batch_size)
            elif fmt == "csv":
                yield from self._read_csv(fs, hdfs_path, batch_size)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        except Exception as e:
            logger.error(f"HDFS read failed for {hdfs_path}: {e}")
            raise

    def _detect_format(self, path: str) -> str:
        path_lower = path.lower()
        for fmt in self.SUPPORTED_FORMATS:
            if f".{fmt}" in path_lower or f"/{fmt}" in path_lower:
                return fmt
        return "parquet"  # default

    def _read_parquet(self, fs, path: str, batch_size: int) -> Iterator[pd.DataFrame]:
        import pyarrow.dataset as ds
        dataset = ds.dataset(path, filesystem=fs, format="parquet")
        for batch in dataset.to_batches(batch_size=batch_size):
            yield pa.Table.from_batches([batch]).to_pandas()

    def _read_orc(self, fs, path: str, batch_size: int) -> Iterator[pd.DataFrame]:
        import pyarrow.orc as orc
        with fs.open_input_file(path) as f:
            orc_file = orc.ORCFile(f)
            total_rows = orc_file.nrows
            for start in range(0, total_rows, batch_size):
                end = min(start + batch_size, total_rows)
                yield orc_file.read(columns=None).slice(start, end - start).to_pandas()

    def _read_csv(self, fs, path: str, batch_size: int) -> Iterator[pd.DataFrame]:
        from pyarrow import csv as pa_csv
        with fs.open_input_file(path) as f:
            reader = pa_csv.open_csv(f)
            while True:
                try:
                    batch = reader.read_next_batch()
                    yield pa.Table.from_batches([batch]).to_pandas()
                except StopIteration:
                    break

    def list_paths(self, hdfs_dir: str, recursive: bool = True) -> List[str]:
        """List all file paths under an HDFS directory."""
        fs = self._get_filesystem()
        try:
            selector = pa.fs.FileSelector(hdfs_dir, recursive=recursive)
            file_infos = fs.get_file_info(selector)
            return [fi.path for fi in file_infos if fi.type == pa.fs.FileType.File]
        except Exception as e:
            logger.error(f"Failed to list {hdfs_dir}: {e}")
            return []
