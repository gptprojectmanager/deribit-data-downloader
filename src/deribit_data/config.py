"""Configuration management with Pydantic and environment variable support."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class ValidationConfig(BaseModel):
    """Data validation thresholds."""

    # IV bounds
    iv_min: float = Field(default=0.01, ge=0.0, description="Minimum valid IV")
    iv_max: float = Field(default=5.0, le=10.0, description="Maximum valid IV")

    # Volume thresholds
    volume_spike_threshold: float = Field(
        default=10.0, ge=1.0, description="Volume spike detection multiplier"
    )

    # Gap detection (days)
    gap_critical_days: int = Field(default=7, ge=1, description="Critical gap threshold in days")
    gap_high_days: int = Field(default=3, ge=1, description="High severity gap in days")
    gap_medium_days: int = Field(default=1, ge=1, description="Medium severity gap in days")
    gap_low_hours: int = Field(default=1, ge=1, description="Low severity gap in hours")

    # Completeness thresholds
    critical_completeness: float = Field(
        default=50.0, ge=0.0, le=100.0, description="Critical completeness threshold %"
    )
    warning_completeness: float = Field(
        default=80.0, ge=0.0, le=100.0, description="Warning completeness threshold %"
    )

    # Statistical thresholds
    iqr_multiplier: float = Field(default=1.5, ge=1.0, description="IQR multiplier for outliers")
    rolling_window: int = Field(default=100, ge=10, description="Rolling window size for stats")
    duplicate_threshold: float = Field(
        default=5.0, ge=0.0, le=100.0, description="Max duplicate % allowed"
    )


class DeribitConfig(BaseModel):
    """Main configuration for Deribit data downloader."""

    model_config = {"frozen": True, "extra": "forbid"}

    # API settings
    base_url: str = Field(
        default="https://history.deribit.com/api/v2/public",
        description="Deribit history API base URL",
    )
    http_timeout: float = Field(default=30.0, ge=5.0, le=120.0, description="HTTP timeout seconds")
    max_retries: int = Field(default=3, ge=1, le=10, description="Max retry attempts")
    rate_limit_delay: float = Field(
        default=0.1, ge=0.0, le=5.0, description="Delay between requests"
    )
    batch_size: int = Field(default=10000, ge=100, le=100000, description="Trades per page")
    max_pages: int = Field(default=20000, ge=100, description="Safety limit on pages")
    backoff_base: float = Field(default=2.0, ge=1.0, le=5.0, description="Exponential backoff base")

    # Backfill settings
    historical_start_date: date = Field(
        default=date(2016, 1, 1), description="Start date for historical backfill"
    )
    flush_every_pages: int = Field(
        default=100, ge=10, le=1000, description="Flush to disk every N pages"
    )
    checkpoint_dir: Path = Field(
        default=Path(".checkpoints"), description="Directory for checkpoint files"
    )

    # Storage settings
    catalog_path: Path = Field(
        default=Path("./data/deribit_options"), description="Output catalog path"
    )
    compression: Literal["zstd", "snappy", "gzip", "none"] = Field(
        default="zstd", description="Parquet compression codec"
    )
    compression_level: int = Field(
        default=3, ge=1, le=22, description="Compression level (zstd: 1-22)"
    )

    # Validation settings
    validation: ValidationConfig = Field(default_factory=ValidationConfig)

    # Metrics settings
    standard_tenors: list[int] = Field(
        default=[1, 7, 30, 90], description="Standard option tenors in days"
    )
    risk_free_rate: float = Field(default=0.05, ge=0.0, le=0.5, description="Risk-free rate")

    # Data settings
    currencies: list[str] = Field(default=["BTC", "ETH"], description="Currencies to download")

    @classmethod
    def from_env(cls) -> DeribitConfig:
        """Load configuration from environment variables."""
        env_values: dict = {}

        # API settings
        if val := os.getenv("DERIBIT_BASE_URL"):
            env_values["base_url"] = val
        if val := os.getenv("DERIBIT_HTTP_TIMEOUT"):
            env_values["http_timeout"] = float(val)
        if val := os.getenv("DERIBIT_MAX_RETRIES"):
            env_values["max_retries"] = int(val)
        if val := os.getenv("DERIBIT_RATE_LIMIT_DELAY"):
            env_values["rate_limit_delay"] = float(val)
        if val := os.getenv("DERIBIT_BATCH_SIZE"):
            env_values["batch_size"] = int(val)
        if val := os.getenv("DERIBIT_MAX_PAGES"):
            env_values["max_pages"] = int(val)
        if val := os.getenv("DERIBIT_BACKOFF_BASE"):
            env_values["backoff_base"] = float(val)

        # Backfill settings
        if val := os.getenv("DERIBIT_FLUSH_EVERY_PAGES"):
            env_values["flush_every_pages"] = int(val)
        if val := os.getenv("DERIBIT_CHECKPOINT_DIR"):
            env_values["checkpoint_dir"] = Path(val)

        # Storage settings
        if val := os.getenv("DERIBIT_CATALOG_PATH"):
            env_values["catalog_path"] = Path(val)
        if val := os.getenv("DERIBIT_COMPRESSION"):
            env_values["compression"] = val
        if val := os.getenv("DERIBIT_COMPRESSION_LEVEL"):
            env_values["compression_level"] = int(val)

        # Currencies
        if val := os.getenv("DERIBIT_CURRENCIES"):
            env_values["currencies"] = [c.strip() for c in val.split(",")]

        return cls(**env_values)

    @classmethod
    def from_file(cls, path: Path) -> DeribitConfig:
        """Load configuration from YAML or JSON file."""
        import json

        content = path.read_text()

        if path.suffix in (".yaml", ".yml"):
            try:
                import yaml

                data = yaml.safe_load(content)
            except ImportError:
                raise ImportError("PyYAML required for YAML config files") from None
        else:
            data = json.loads(content)

        return cls(**data)
