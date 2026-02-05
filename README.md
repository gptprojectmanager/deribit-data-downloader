# deribit-data-downloader

High-performance Deribit options data downloader with crash recovery, streaming writes, and NautilusTrader compatibility.

## Features

- **Streaming downloads** - No RAM accumulation, writes as it fetches
- **Crash recovery** - Resume from checkpoint after interruption
- **Atomic writes** - No data corruption on crash (tmp + rename pattern)
- **DVOL index** - Download Deribit Volatility Index (DVOL) data
- **Data validation** - Configurable quality checks
- **SHA256 manifest** - Verify data integrity

## Installation

```bash
pip install deribit-data-downloader
```

Or with uv:

```bash
uv pip install deribit-data-downloader
```

## Quick Start

```bash
# Full historical backfill (from 2016)
deribit-data backfill --currency ETH --catalog ./data/deribit

# Resume interrupted download
deribit-data backfill --currency ETH --catalog ./data/deribit --resume

# Daily incremental sync
deribit-data sync --currency BTC --catalog ./data/deribit

# Download DVOL index
deribit-data dvol --currency BTC --start 2024-01-01 --catalog ./data/deribit

# Validate data integrity
deribit-data validate --catalog ./data/deribit

# Show catalog info
deribit-data info --catalog ./data/deribit
```

## Configuration

All parameters are configurable via environment variables or CLI flags:

```bash
# Environment variables
export DERIBIT_CATALOG_PATH=/media/sam/2TB-NVMe/data/deribit_options
export DERIBIT_HTTP_TIMEOUT=30
export DERIBIT_MAX_RETRIES=3
export DERIBIT_COMPRESSION=zstd
export DERIBIT_FLUSH_EVERY_PAGES=100

# Run with env config
deribit-data backfill --currency ETH
```

## Output Format

Data is written as Parquet files with daily partitioning:

```
catalog/
├── BTC/
│   ├── trades/
│   │   ├── 2024-01-01.parquet
│   │   ├── 2024-01-02.parquet
│   │   └── ...
│   └── dvol/
│       └── dvol.parquet
├── ETH/
│   ├── trades/
│   └── dvol/
└── manifest.json
```

### Schema (v1)

**Trades:**
| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp[us, UTC] | Trade execution time |
| instrument_id | string | Full instrument name |
| underlying | string | BTC or ETH |
| strike | float64 | Strike price |
| expiry | timestamp[us, UTC] | Option expiry |
| option_type | string | call or put |
| price | float64 | Trade price |
| iv | float64 | Implied volatility |
| amount | float64 | Trade size |
| direction | string | buy or sell |

**DVOL:**
| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp[us, UTC] | Candle time |
| open | float64 | Opening IV |
| high | float64 | High IV |
| low | float64 | Low IV |
| close | float64 | Closing IV |

## Docker

```bash
# Build
docker build -t deribit-data-downloader .

# Run with volume mount
docker run -v /path/to/data:/data deribit-data-downloader \
    backfill --currency ETH --catalog /data/deribit
```

## Development

```bash
# Clone
git clone https://github.com/yourusername/deribit-data-downloader
cd deribit-data-downloader

# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v --cov

# Lint
uv run ruff check .
uv run ruff format .
```

## License

MIT
