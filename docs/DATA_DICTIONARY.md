# Deribit Data Dictionary

This document describes all data schemas, fields, and constraints for the Deribit Options Data Catalog.

## Schema Versions

| Schema | Version | Status |
|--------|---------|--------|
| Trades | V1 | Active |
| DVOL | V1 | Active |

## Trades Schema (TRADES_SCHEMA_V1)

Options trade data from Deribit History API.

### Fields

| Field | Type | Nullable | Description | Valid Range | Example |
|-------|------|----------|-------------|-------------|---------|
| `timestamp` | timestamp[us, UTC] | No | Trade execution time | 2016-01-01 to present | `2024-01-15T14:30:00.123456Z` |
| `trade_id` | string | No | Unique Deribit trade ID | - | `"ETH-12345678"` |
| `instrument_id` | string | No | Deribit instrument name | Pattern: `{CURRENCY}-{DDMMMYY}-{STRIKE}-{C\|P}` | `"BTC-27DEC24-100000-C"` |
| `underlying` | string | No | Underlying asset | `BTC`, `ETH`, `SOL`, `USDC` | `"BTC"` |
| `strike` | float64 | No | Strike price in USD | > 0 | `100000.0` |
| `expiry` | timestamp[us, UTC] | No | Option expiration (08:00 UTC) | Future date at trade time | `2024-12-27T08:00:00Z` |
| `option_type` | string | No | Option type | `call`, `put` | `"call"` |
| `price` | float64 | No | Trade price (premium/notional) | >= 0 | `0.0525` |
| `iv` | float64 | Yes | Implied volatility (decimal) | 0.0 - 5.0 (0-500%) | `0.65` (65%) |
| `amount` | float64 | No | Trade size in contracts | > 0 | `10.0` |
| `direction` | string | No | Trade direction | `buy`, `sell` | `"buy"` |
| `index_price` | float64 | Yes | Underlying index price | > 0 | `45000.0` |
| `mark_price` | float64 | Yes | Mark price at trade time | >= 0 | `0.051` |

### Notes

- **IV Conversion**: Raw API returns IV as percentage (e.g., 65 for 65%). Stored as decimal (0.65).
- **IV Nullable**: Some early trades (pre-2017) may not have IV data.
- **Expiry Time**: All options expire at 08:00 UTC on expiry date.
- **Instrument Pattern**: `{UNDERLYING}-{DDMMMYY}-{STRIKE}-{TYPE}`
  - UNDERLYING: BTC, ETH, etc.
  - DDMMMYY: Day + 3-letter month + 2-digit year (e.g., 27DEC24)
  - STRIKE: Integer strike price
  - TYPE: C (Call) or P (Put)

### File Format

- **Codec**: Parquet with zstd compression (level 3)
- **Partitioning**: Daily by trade timestamp (`{CURRENCY}/trades/YYYY-MM-DD.parquet`)
- **Row Group Size**: Default PyArrow settings

---

## DVOL Schema (DVOL_SCHEMA_V1)

Deribit Volatility Index OHLC candles.

### Fields

| Field | Type | Nullable | Description | Valid Range | Example |
|-------|------|----------|-------------|-------------|---------|
| `timestamp` | timestamp[us, UTC] | No | Candle open time | 2019-03-01 to present | `2024-01-15T14:00:00Z` |
| `open` | float64 | No | Opening IV value | > 0 | `55.5` |
| `high` | float64 | No | Highest IV value | >= open, low, close | `58.2` |
| `low` | float64 | No | Lowest IV value | <= open, high, close | `54.1` |
| `close` | float64 | No | Closing IV value | > 0 | `56.8` |

### Notes

- **Index Start**: DVOL index available from March 2019.
- **Resolution**: 1-hour candles (3600 seconds).
- **Values**: IV expressed as percentage points (e.g., 55.5 = 55.5% volatility).

### File Format

- **Codec**: Parquet with zstd compression
- **Partitioning**: Single file per currency (`{CURRENCY}/dvol/dvol.parquet`)

---

## Data Quality Constraints

### Validation Rules

| Rule | Severity | Threshold |
|------|----------|-----------|
| IV out of range | Medium | < 0.01 or > 5.0 |
| Data gap (critical) | Critical | >= 7 days |
| Data gap (high) | High | >= 3 days |
| Data gap (medium) | Medium | >= 1 day |
| Completeness (critical) | Critical | < 50% |
| Completeness (warning) | Warning | < 80% |
| Duplicate trades | High | > 5% |

### Constraints

1. **Timestamp ordering**: Trades within a file must be ascending by timestamp.
2. **No duplicate trade_ids**: Within and across files.
3. **Valid instrument format**: Must match `{CURRENCY}-{DDMMMYY}-{STRIKE}-{C|P}`.
4. **Positive amounts**: Trade amounts must be > 0.
5. **Non-negative prices**: Trade prices must be >= 0.

---

## Catalog Structure

```
{CATALOG_PATH}/
├── BTC/
│   ├── trades/
│   │   ├── 2016-11-29.parquet
│   │   ├── 2016-11-30.parquet
│   │   └── ...
│   └── dvol/
│       └── dvol.parquet
├── ETH/
│   ├── trades/
│   │   └── ...
│   └── dvol/
│       └── dvol.parquet
├── _dead_letters/          # Failed trade parsing
│   ├── btc_dead_letters_2024-01-15.jsonl
│   └── ...
├── _audit/                 # Operation audit logs
│   ├── audit_2024-01.jsonl
│   └── ...
└── manifest.json           # SHA256 checksums
```

---

## API Response Mapping

### Trade Response → OptionTrade

| API Field | Schema Field | Transformation |
|-----------|--------------|----------------|
| `trade_id` | `trade_id` | String conversion |
| `instrument_name` | `instrument_id` | Direct copy |
| `instrument_name` | `underlying`, `strike`, `expiry`, `option_type` | Parsed from pattern |
| `timestamp` | `timestamp` | ms → datetime UTC |
| `price` | `price` | Direct copy |
| `iv` | `iv` | Divide by 100 (% → decimal) |
| `amount` | `amount` | Direct copy |
| `direction` | `direction` | Direct copy |
| `index_price` | `index_price` | Direct copy (nullable) |
| `mark_price` | `mark_price` | Direct copy (nullable) |

### DVOL Response → DVOLCandle

| API Field | Schema Field | Transformation |
|-----------|--------------|----------------|
| `data[0]` | `timestamp` | ms → datetime UTC |
| `data[1]` | `open` | Direct copy |
| `data[2]` | `high` | Direct copy |
| `data[3]` | `low` | Direct copy |
| `data[4]` | `close` | Direct copy |

---

## Usage Examples

### Python (PyArrow)

```python
import pyarrow.parquet as pq

# Read trades
table = pq.read_table("/path/catalog/BTC/trades/2024-01-15.parquet")
df = table.to_pandas()

# Filter by option type
calls = df[df["option_type"] == "call"]

# Get high IV trades
high_iv = df[df["iv"] > 1.0]  # > 100% IV
```

### DuckDB

```sql
-- Load trades
SELECT * FROM read_parquet('/path/catalog/BTC/trades/*.parquet')
WHERE iv > 0.5  -- > 50% IV
ORDER BY timestamp;

-- Daily volume
SELECT
    DATE_TRUNC('day', timestamp) as date,
    SUM(amount) as total_contracts
FROM read_parquet('/path/catalog/ETH/trades/*.parquet')
GROUP BY 1
ORDER BY 1;
```

---

## Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2026-02-05 | V1 | Initial schema definition |
