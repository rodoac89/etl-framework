"""Salida en texto y JSON del reporte."""

from __future__ import annotations

import json

from etl_framework.model import SEVERITY_LABEL, FileReport


def _counts(report: FileReport) -> str:
    return (
        f"{report.errors} error(es), "
        f"{report.warnings} advertencia(s), "
        f"{report.infos} info"
    )


def render_text(reports: list[FileReport], *, strict: bool = False) -> str:
    if not reports:
        return "No se encontraron scripts .sql, .hql o .py para validar.\n"

    rejected = sum(1 for report in reports if report.status(strict) == "RECHAZADO")
    observed = sum(1 for report in reports if report.status(strict) == "CON OBSERVACIONES")
    approved = sum(1 for report in reports if report.status(strict) == "APROBADO")
    omitted = sum(1 for report in reports if report.status(strict) == "OMITIDO")
    lines = [
        "Validador de buenas prácticas Hive / Spark",
        (
            f"{len(reports)} archivo(s): {approved} aprobado(s), "
            f"{observed} con observaciones, {rejected} rechazado(s), {omitted} omitido(s)."
        ),
        "",
    ]
    for report in reports:
        status = report.status(strict)
        lines.append(f"[{status}] {report.path}  ({report.engine})")
        if report.omitted_reason:
            lines.append(f"  {report.omitted_reason}")
            lines.append("")
            continue
        lines.append(f"  {_counts(report)}")
        if not report.findings:
            lines.append("  Sin hallazgos.")
        for finding in report.findings:
            label = SEVERITY_LABEL.get(finding.severity, finding.severity.upper())
            lines.append(
                f"  [{label}] {finding.rule_id}  línea {finding.line}: {finding.message}"
            )
            if finding.snippet:
                lines.append(f"    Fragmento: {finding.snippet}")
            if finding.suggestion:
                lines.append(f"    Sugerencia: {finding.suggestion}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_json(reports: list[FileReport], *, strict: bool = False) -> str:
    rejected = any(report.status(strict) == "RECHAZADO" for report in reports)
    observed = any(report.status(strict) == "CON OBSERVACIONES" for report in reports)
    if not reports:
        status = "VACIO"
    elif rejected:
        status = "RECHAZADO"
    elif observed:
        status = "CON OBSERVACIONES"
    else:
        status = "APROBADO"
    payload = {
        "estado": status,
        "estricto": strict,
        "archivos": [report.to_dict(strict) for report in reports],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def exit_code(reports: list[FileReport], *, strict: bool = False) -> int:
    if not reports:
        return 2
    if any(report.status(strict) == "RECHAZADO" for report in reports):
        return 1
    return 0
