"""Tests for manifest module."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from deribit_data.manifest import ManifestEntry, ManifestManager


class TestManifestEntry:
    """Tests for ManifestEntry."""

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        entry = ManifestEntry(
            sha256="abc123",
            size_bytes=1000,
            row_count=100,
            timestamp_range=("2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z"),
        )

        data = entry.to_dict()

        assert data["sha256"] == "abc123"
        assert data["size_bytes"] == 1000
        assert data["row_count"] == 100
        assert data["timestamp_range"] == ["2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z"]

    def test_from_dict(self) -> None:
        """Test deserialization from dict."""
        data = {
            "sha256": "def456",
            "size_bytes": 2000,
            "row_count": 200,
            "timestamp_range": ["2024-02-01T00:00:00Z", "2024-02-01T23:59:59Z"],
        }

        entry = ManifestEntry.from_dict(data)

        assert entry.sha256 == "def456"
        assert entry.size_bytes == 2000
        assert entry.row_count == 200
        assert entry.timestamp_range == ("2024-02-01T00:00:00Z", "2024-02-01T23:59:59Z")

    def test_from_dict_no_timestamp_range(self) -> None:
        """Test deserialization without timestamp range."""
        data = {
            "sha256": "abc123",
            "size_bytes": 1000,
            "row_count": 100,
        }

        entry = ManifestEntry.from_dict(data)

        assert entry.timestamp_range is None


class TestManifestManager:
    """Tests for ManifestManager."""

    def test_init_empty_catalog(self, tmp_catalog: Path) -> None:
        """Test initialization with empty catalog."""
        mgr = ManifestManager(tmp_catalog)

        assert mgr.file_count == 0
        assert mgr.total_size == 0
        assert mgr.total_rows == 0

    def test_update_file(self, tmp_catalog: Path) -> None:
        """Test updating manifest with a file."""
        # Create a parquet file
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)

        mgr = ManifestManager(tmp_catalog)
        entry = mgr.update_file(file_path)

        assert entry.sha256 is not None
        assert len(entry.sha256) == 64  # SHA256 hex length
        assert entry.size_bytes > 0
        assert entry.row_count == 3
        assert mgr.file_count == 1

    def test_save_and_load(self, tmp_catalog: Path) -> None:
        """Test saving and loading manifest."""
        # Create a file and update manifest
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)

        mgr1 = ManifestManager(tmp_catalog)
        mgr1.update_file(file_path)
        mgr1.save()

        # Load in new manager
        mgr2 = ManifestManager(tmp_catalog)

        assert mgr2.file_count == 1
        entry = mgr2.get_entry("test.parquet")
        assert entry is not None
        assert entry.row_count == 3

    def test_verify_file_success(self, tmp_catalog: Path) -> None:
        """Test verifying a valid file."""
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)

        mgr = ManifestManager(tmp_catalog)
        mgr.update_file(file_path)

        assert mgr.verify_file(file_path) is True

    def test_verify_file_modified(self, tmp_catalog: Path) -> None:
        """Test verification fails for modified file."""
        # Create and register file
        table1 = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table1, file_path)

        mgr = ManifestManager(tmp_catalog)
        mgr.update_file(file_path)

        # Modify file
        table2 = pa.table({"x": [4, 5, 6]})
        pq.write_table(table2, file_path)

        assert mgr.verify_file(file_path) is False

    def test_verify_file_not_in_manifest(self, tmp_catalog: Path) -> None:
        """Test verification fails for unregistered file."""
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)

        mgr = ManifestManager(tmp_catalog)
        # Don't update manifest

        assert mgr.verify_file(file_path) is False

    def test_verify_all(self, tmp_catalog: Path) -> None:
        """Test verifying all files."""
        mgr = ManifestManager(tmp_catalog)

        # Create multiple files
        for i in range(3):
            table = pa.table({"x": list(range(i + 1))})
            file_path = tmp_catalog / f"test_{i}.parquet"
            pq.write_table(table, file_path)
            mgr.update_file(file_path)

        passed, failed, failed_files = mgr.verify_all()

        assert passed == 3
        assert failed == 0
        assert failed_files == []

    def test_verify_all_with_missing_file(self, tmp_catalog: Path) -> None:
        """Test verification reports missing files."""
        mgr = ManifestManager(tmp_catalog)

        # Create file and register
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)
        mgr.update_file(file_path)

        # Delete file
        file_path.unlink()

        passed, failed, failed_files = mgr.verify_all()

        assert passed == 0
        assert failed == 1
        assert "test.parquet" in failed_files

    def test_remove_file(self, tmp_catalog: Path) -> None:
        """Test removing file from manifest."""
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)

        mgr = ManifestManager(tmp_catalog)
        mgr.update_file(file_path)
        assert mgr.file_count == 1

        result = mgr.remove_file(file_path)
        assert result is True
        assert mgr.file_count == 0

    def test_atomic_save(self, tmp_catalog: Path) -> None:
        """Test manifest save is atomic."""
        table = pa.table({"x": [1, 2, 3]})
        file_path = tmp_catalog / "test.parquet"
        pq.write_table(table, file_path)

        mgr = ManifestManager(tmp_catalog)
        mgr.update_file(file_path)
        mgr.save()

        # No temp files should remain
        tmp_files = list(tmp_catalog.glob("*.tmp"))
        assert len(tmp_files) == 0

        # Manifest file should exist
        assert (tmp_catalog / "manifest.json").exists()

    def test_totals(self, tmp_catalog: Path) -> None:
        """Test total size and rows."""
        mgr = ManifestManager(tmp_catalog)

        # Create files with different sizes
        for i in range(3):
            table = pa.table({"x": list(range((i + 1) * 10))})
            file_path = tmp_catalog / f"test_{i}.parquet"
            pq.write_table(table, file_path)
            mgr.update_file(file_path)

        assert mgr.total_rows == 10 + 20 + 30  # 60
        assert mgr.total_size > 0
