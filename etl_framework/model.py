"""Estructuras compartidas del reporte de validación."""

from __future__ import annotations

from dataclasses import dataclass, field


SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}
SEVERITY_LABEL = {
    "error": "ERROR",
    "warning": "ADVERTENCIA",
    "info": "INFO",
}


@dataclass
class Finding:
    rule_id: str
    severity: str
    line: int
    message: str
    suggestion: str = ""
    snippet: str = ""
    engine: str = ""

    def to_dict(self) -> dict:
        return {
            "regla": self.rule_id,
            "severidad": self.severity,
            "linea": self.line,
            "mensaje": self.message,
            "sugerencia": self.suggestion,
            "fragmento": self.snippet,
            "motor": self.engine,
        }


@dataclass
class FileReport:
    path: str
    engine: str
    findings: list[Finding] = field(default_factory=list)
    omitted_reason: str = ""

    @property
    def errors(self) -> int:
        return sum(1 for f in self.findings if f.severity == "error")

    @property
    def warnings(self) -> int:
        return sum(1 for f in self.findings if f.severity == "warning")

    @property
    def infos(self) -> int:
        return sum(1 for f in self.findings if f.severity == "info")

    def status(self, strict: bool = False) -> str:
        if self.omitted_reason:
            return "OMITIDO"
        if self.errors or (strict and self.warnings):
            return "RECHAZADO"
        if self.warnings:
            return "CON OBSERVACIONES"
        return "APROBADO"

    def to_dict(self, strict: bool = False) -> dict:
        return {
            "archivo": self.path,
            "motor": self.engine,
            "estado": self.status(strict),
            "omitido": self.omitted_reason or None,
            "resumen": {
                "errores": self.errors,
                "advertencias": self.warnings,
                "informativos": self.infos,
            },
            "hallazgos": [f.to_dict() for f in self.findings],
        }


def snippet_at(source: str, line: int, limit: int = 220) -> str:
    lines = source.splitlines()
    if line < 1 or line > len(lines):
        return ""
    text = lines[line - 1].strip()
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def sort_findings(findings: list[Finding]) -> list[Finding]:
    return sorted(findings, key=lambda f: (f.line, SEVERITY_ORDER.get(f.severity, 9), f.rule_id))
