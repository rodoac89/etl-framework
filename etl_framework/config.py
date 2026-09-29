"""Configuración del validador. Los valores por defecto salen de las guías Hive/Spark."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


# Claves de partición usadas en las guías (anio/mes/dia) y en la reingeniería
# (year, ingestion_year/month/day).
DEFAULT_PARTITION_COLUMNS = (
    "anio",
    "año",
    "mes",
    "dia",
    "día",
    "year",
    "month",
    "day",
    "ingestion_year",
    "ingestion_month",
    "ingestion_day",
)

DEFAULT_TEMP_PREFIXES = ("IN_", "TMP_", "TEMP_", "AGG_")
DEFAULT_SPARK_PREFIXES = ("in_", "tmp_", "temp_", "agg_", "df_")
DEFAULT_STORAGE_FORMATS = ("PARQUET", "ORC")


@dataclass
class Config:
    partition_columns: tuple[str, ...] = DEFAULT_PARTITION_COLUMNS
    hive_max_joins: int = 1
    spark_max_joins: int = 2
    temp_prefixes: tuple[str, ...] = DEFAULT_TEMP_PREFIXES
    spark_prefixes: tuple[str, ...] = DEFAULT_SPARK_PREFIXES
    storage_formats: tuple[str, ...] = DEFAULT_STORAGE_FORMATS
    overrides: dict[str, dict] = field(default_factory=dict)

    def partition_set(self) -> set[str]:
        return {col.lower() for col in self.partition_columns}

    def apply(self, rule_id: str, severity: str) -> str | None:
        """Devuelve la severidad efectiva, o None si la regla está apagada."""
        override = self.overrides.get(rule_id) or {}
        if override.get("enabled") is False:
            return None
        return str(override.get("severity") or severity)


def load_config(path: str | Path | None) -> Config:
    config = Config()
    if not path:
        return config
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if "partition_columns" in data:
        config.partition_columns = tuple(str(c) for c in data["partition_columns"])
    if "hive_max_joins" in data:
        config.hive_max_joins = int(data["hive_max_joins"])
    if "spark_max_joins" in data:
        config.spark_max_joins = int(data["spark_max_joins"])
    if "temp_prefixes" in data:
        config.temp_prefixes = tuple(str(p).upper() for p in data["temp_prefixes"])
    if "spark_prefixes" in data:
        config.spark_prefixes = tuple(str(p) for p in data["spark_prefixes"])
    if "storage_formats" in data:
        config.storage_formats = tuple(str(f).upper() for f in data["storage_formats"])
    if "rules" in data:
        config.overrides = {str(k): dict(v) for k, v in data["rules"].items()}
    return config
