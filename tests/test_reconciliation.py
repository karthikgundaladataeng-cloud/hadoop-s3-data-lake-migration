"""Tests for reconciliation and progress tracker."""
import pytest
import pandas as pd
from validator.reconciliation import ReconciliationValidator
from utils.progress_tracker import ProgressTracker


@pytest.fixture
def sample_source():
    return pd.DataFrame({
        "order_id": [1, 2, 3, 4, 5],
        "customer_id": [10, 20, 30, 40, 50],
        "amount": [100.0, 200.0, 300.0, 400.0, 500.0],
        "status": ["ACTIVE", "PENDING", "ACTIVE", "CANCELLED", "SHIPPED"],
    })


class TestReconciliationValidator:
    def test_identical_dfs_pass(self, sample_source):
        validator = ReconciliationValidator()
        result = validator.validate(sample_source, sample_source.copy(), ["order_id"], "orders")
        assert result["overall_passed"] is True

    def test_row_count_mismatch_detected(self, sample_source):
        validator = ReconciliationValidator()
        target = sample_source.head(3).copy()
        result = validator.validate(sample_source, target, ["order_id"], "orders")
        assert result["checks"]["row_count"]["passed"] is False
        assert result["checks"]["row_count"]["diff"] == 2

    def test_missing_column_detected(self, sample_source):
        validator = ReconciliationValidator()
        target = sample_source.drop(columns=["status"])
        result = validator.validate(sample_source, target, ["order_id"], "orders")
        assert result["checks"]["columns"]["passed"] is False
        assert "status" in result["checks"]["columns"]["missing_in_target"]

    def test_checksum_matches_identical_data(self, sample_source):
        validator = ReconciliationValidator()
        result = validator.validate(sample_source, sample_source.copy(), ["order_id"], "orders")
        assert result["checks"]["checksum"]["passed"] is True

    def test_checksum_fails_on_modified_data(self, sample_source):
        validator = ReconciliationValidator()
        target = sample_source.copy()
        target.loc[0, "amount"] = 9999.0
        result = validator.validate(sample_source, target, ["order_id"], "orders")
        assert result["checks"]["checksum"]["passed"] is False


class TestProgressTracker:
    def test_initial_status_is_none(self, tmp_path):
        tracker = ProgressTracker("test_run", db_path=str(tmp_path / "state.db"))
        assert tracker.get_status("/data/orders") is None

    def test_status_updates(self, tmp_path):
        tracker = ProgressTracker("test_run", db_path=str(tmp_path / "state.db"))
        tracker.update("/data/orders", "complete", 1000)
        assert tracker.get_status("/data/orders") == "complete"

    def test_summary_counts(self, tmp_path):
        tracker = ProgressTracker("test_run", db_path=str(tmp_path / "state.db"))
        tracker.update("/data/orders", "complete", 500)
        tracker.update("/data/customers", "complete", 200)
        tracker.update("/data/products", "failed", 0)
        summary = tracker.get_summary()
        assert summary["complete"]["count"] == 2
        assert summary["failed"]["count"] == 1
