"""Parquet schema definitions for Deribit data.

These schemas define the contract between the data producer (this package)
and consumers (e.g., NautilusTrader). Version them to maintain compatibility.
"""

import pyarrow as pa

# Schema version for compatibility checking
SCHEMA_VERSION = "1"

# Options trades schema
TRADES_SCHEMA_V1 = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("underlying", pa.string(), nullable=False),
        pa.field("strike", pa.float64(), nullable=False),
        pa.field("expiry", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("option_type", pa.string(), nullable=False),  # "call" or "put"
        pa.field("price", pa.float64(), nullable=False),
        pa.field("iv", pa.float64(), nullable=True),  # May be missing in old data
        pa.field("amount", pa.float64(), nullable=False),
        pa.field("direction", pa.string(), nullable=False),  # "buy" or "sell"
        pa.field("index_price", pa.float64(), nullable=True),
        pa.field("mark_price", pa.float64(), nullable=True),
    ],
    metadata={
        "schema_version": SCHEMA_VERSION,
        "producer": "deribit-data-downloader",
    },
)

# DVOL (Deribit Volatility Index) schema
DVOL_SCHEMA_V1 = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
    ],
    metadata={
        "schema_version": SCHEMA_VERSION,
        "producer": "deribit-data-downloader",
    },
)


def get_schema_version(schema: pa.Schema) -> str | None:
    """Extract schema version from metadata."""
    if schema.metadata is None:
        return None
    return schema.metadata.get(b"schema_version", b"").decode("utf-8") or None


def validate_trades_schema(schema: pa.Schema) -> bool:
    """Check if a schema is compatible with TRADES_SCHEMA_V1."""
    required_fields = {"timestamp", "instrument_id", "underlying", "strike", "price", "amount"}
    schema_fields = {field.name for field in schema}
    return required_fields.issubset(schema_fields)


def validate_dvol_schema(schema: pa.Schema) -> bool:
    """Check if a schema is compatible with DVOL_SCHEMA_V1."""
    required_fields = {"timestamp", "open", "high", "low", "close"}
    schema_fields = {field.name for field in schema}
    return required_fields.issubset(schema_fields)
