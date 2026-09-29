"""Interfaz de línea de comandos.

    python -m etl_framework check ruta.sql
    python -m etl_framework check carpeta/ --format json
    python -m etl_framework rules
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from etl_framework import __version__
from etl_framework.config import load_config
from etl_framework.engine import check_paths
from etl_framework.report import exit_code, render_json, render_text
from etl_framework.rules import RULE_CATALOG


def _configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8")
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl-check",
        description="Detecta si un ETL HiveQL o PySpark cumple las buenas prácticas del framework.",
    )
    parser.add_argument("--version", action="version", version=f"etl-framework {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="Valida uno o más scripts o un directorio.")
    check.add_argument("paths", nargs="+", help="Archivos .sql/.hql/.py o directorios.")
    check.add_argument(
        "--engine",
        choices=("auto", "hive", "spark"),
        default="auto",
        help="Motor a forzar. Por defecto se infiere por extensión y contenido.",
    )
    check.add_argument("--config", help="JSON con columnas de partición, límites y severidad por regla.")
    check.add_argument("--format", choices=("text", "json"), default="text")
    check.add_argument("--output", help="Además de la consola, escribe el reporte en este archivo.")
    check.add_argument(
        "--strict",
        action="store_true",
        help="Las advertencias también rechazan el script (código de salida 1).",
    )

    rules = sub.add_parser("rules", help="Lista las reglas y su severidad por defecto.")
    rules.add_argument("--engine", choices=("hive", "spark", "all"), default="all")
    return parser


def _print_rules(engine: str) -> int:
    print(f"{'Regla':<28} {'Motor':<8} {'Severidad':<12} Título")
    for rule in RULE_CATALOG:
        if engine != "all" and rule.engine != engine:
            continue
        print(f"{rule.rule_id:<28} {rule.engine:<8} {rule.severity:<12} {rule.title}")
        print(f"  {rule.summary}")
    return 0


def main(argv: list[str] | None = None) -> int:
    _configure_stdio()
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "rules":
        return _print_rules(args.engine)

    config = load_config(args.config)
    paths = [Path(path) for path in args.paths]
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        print("No existe: " + ", ".join(missing), file=sys.stderr)
        return 2
    reports = check_paths(paths, config, engine=args.engine)
    rendered = render_json(reports, strict=args.strict) if args.format == "json" else render_text(reports, strict=args.strict)
    print(rendered, end="")
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    return exit_code(reports, strict=args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
