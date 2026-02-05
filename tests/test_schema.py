"""Tests for schema module."""

import pyarrow as pa

from deribit_data.schema import (
    DVOL_SCHEMA_V1,
    SCHEMA_VERSION,
    TRADES_SCHEMA_V1,
    get_schema_version,
    validate_dvol_schema,
    validate_trades_schema,
)


class TestSchemaConstants:
    """Tests for schema constants."""

    def test_schema_version(self) -> None:
        """Test schema version is set."""
        assert SCHEMA_VERSION == "1"

    def test_trades_schema_fields(self) -> None:
        """Test trades schema has required fields."""
        field_names = [f.name for f in TRADES_SCHEMA_V1]
        assert "timestamp" in field_names
        assert "trade_id" in field_names
        assert "instrument_id" in field_names
        assert "underlying" in field_names
        assert "strike" in field_names
        assert "expiry" in field_names
        assert "option_type" in field_names
        assert "price" in field_names
        assert "iv" in field_names
        assert "amount" in field_names
        assert "direction" in field_names

    def test_trades_schema_metadata(self) -> None:
        """Test trades schema has metadata."""
        assert TRADES_SCHEMA_V1.metadata is not None
        assert b"schema_version" in TRADES_SCHEMA_V1.metadata
        assert b"producer" in TRADES_SCHEMA_V1.metadata

    def test_dvol_schema_fields(self) -> None:
        """Test DVOL schema has required fields."""
        field_names = [f.name for f in DVOL_SCHEMA_V1]
        assert "timestamp" in field_names
        assert "open" in field_names
        assert "high" in field_names
        assert "low" in field_names
        assert "close" in field_names

    def test_dvol_schema_metadata(self) -> None:
        """Test DVOL schema has metadata."""
        assert DVOL_SCHEMA_V1.metadata is not None
        assert b"schema_version" in DVOL_SCHEMA_V1.metadata


class TestSchemaFunctions:
    """Tests for schema functions."""

    def test_get_schema_version(self) -> None:
        """Test getting schema version from metadata."""
        version = get_schema_version(TRADES_SCHEMA_V1)
        assert version == "1"

    def test_get_schema_version_no_metadata(self) -> None:
        """Test getting version from schema without metadata."""
        schema = pa.schema([pa.field("test", pa.int32())])
        version = get_schema_version(schema)
        assert version is None

    def test_validate_trades_schema_valid(self) -> None:
        """Test validating valid trades schema."""
        assert validate_trades_schema(TRADES_SCHEMA_V1)

    def test_validate_trades_schema_subset(self) -> None:
        """Test validating schema with subset of fields."""
        schema = pa.schema(
            [
                pa.field("timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("instrument_id", pa.string()),
                pa.field("underlying", pa.string()),
                pa.field("strike", pa.float64()),
                pa.field("price", pa.float64()),
                pa.field("amount", pa.float64()),
            ]
        )
        assert validate_trades_schema(schema)

    def test_validate_trades_schema_missing_required(self) -> None:
        """Test validating schema missing required fields."""
        schema = pa.schema(
            [
                pa.field("timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("price", pa.float64()),
            ]
        )
        assert not validate_trades_schema(schema)

    def test_validate_dvol_schema_valid(self) -> None:
        """Test validating valid DVOL schema."""
        assert validate_dvol_schema(DVOL_SCHEMA_V1)

    def test_validate_dvol_schema_invalid(self) -> None:
        """Test validating invalid DVOL schema."""
        schema = pa.schema([pa.field("timestamp", pa.timestamp("us", tz="UTC"))])
        assert not validate_dvol_schema(schema)
