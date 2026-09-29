"""Orquesta la detección de motor y la aplicación de reglas."""

from __future__ import annotations

import re
from pathlib import Path

from etl_framework.config import Config
from etl_framework.model import FileReport, sort_findings
from etl_framework.rules import check_hive_source, check_spark_source


HIVE_EXTENSIONS = {".sql", ".hql", ".q", ".ddl"}
SPARK_EXTENSIONS = {".py"}
SKIP_DIRS = {"__pycache__", ".git", ".venv", "venv", ".hg", ".idea"}
_SPARK_HINT = re.compile(r"(?i)\bpyspark\b|SparkSession|\bspark\s*\.\s*(read|table|sql)\b")


def detect_engine(path: Path, source: str, forced: str | None = None) -> str:
    """Devuelve 'hive', 'spark' u 'omit'."""
    if forced and forced != "auto":
        return forced
    suffix = path.suffix.lower()
    if suffix in HIVE_EXTENSIONS:
        return "hive"
    if suffix in SPARK_EXTENSIONS:
        if _SPARK_HINT.search(source):
            return "spark"
        return "omit"
    return "omit"


def check_source(path: Path, source: str, config: Config, *, engine: str = "auto") -> FileReport:
    chosen = detect_engine(path, source, engine)
    report = FileReport(path=str(path), engine=chosen)
    if chosen == "omit":
        report.omitted_reason = (
            "No parece un script Hive (.sql/.hql) ni un script PySpark "
            "(hace falta SparkSession, pyspark o spark.table/read/sql)."
        )
        return report
    if chosen == "hive":
        report.findings = sort_findings(check_hive_source(source, config))
    elif chosen == "spark":
        report.findings = sort_findings(check_spark_source(source, config))
    else:
        report.engine = "omit"
        report.omitted_reason = f"Motor desconocido: {chosen}"
    return report


def check_path(path: Path, config: Config, *, engine: str = "auto") -> FileReport:
    source = path.read_text(encoding="utf-8-sig")
    return check_source(path, source, config, engine=engine)


def iter_scripts(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    found: list[Path] = []
    for candidate in path.rglob("*"):
        if not candidate.is_file():
            continue
        if any(part in SKIP_DIRS for part in candidate.parts):
            continue
        if candidate.suffix.lower() in HIVE_EXTENSIONS | SPARK_EXTENSIONS:
            found.append(candidate)
    return sorted(found)


def check_paths(paths: list[Path], config: Config, *, engine: str = "auto") -> list[FileReport]:
    reports: list[FileReport] = []
    for path in paths:
        for script in iter_scripts(path):
            reports.append(check_path(script, config, engine=engine))
    return reports
