"""La CLI es la puerta de entrada del framework."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "etl_framework", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


class CliTest(unittest.TestCase):
    def test_ok_sql_exits_zero(self) -> None:
        result = run_cli("check", "examples/hive/etl_ok.sql", "examples/spark/etl_ok.py")
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        self.assertIn("APROBADO", result.stdout)
        self.assertNotIn("RECHAZADO", result.stdout)

    def test_bad_sql_exits_one_and_json_report(self) -> None:
        result = run_cli("check", "examples/hive/etl_malo.sql", "--format", "json")
        self.assertEqual(result.returncode, 1, msg=result.stdout + result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["estado"], "RECHAZADO")
        self.assertGreater(payload["archivos"][0]["resumen"]["errores"], 0)

    def test_strict_promotes_warnings(self) -> None:
        script = ROOT / "examples" / "hive" / "_solo_advertencia.sql"
        script.write_text(
            "SELECT DISTINCT id FROM datos.ventas WHERE anio = 2025;\n",
            encoding="utf-8",
        )
        try:
            normal = run_cli("check", str(script))
            strict = run_cli("check", str(script), "--strict")
        finally:
            script.unlink(missing_ok=True)
        self.assertEqual(normal.returncode, 0, msg=normal.stdout)
        self.assertIn("CON OBSERVACIONES", normal.stdout)
        self.assertEqual(strict.returncode, 1, msg=strict.stdout)
        self.assertIn("RECHAZADO", strict.stdout)

    def test_rules_lists_hive_and_spark(self) -> None:
        result = run_cli("rules")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("HIVE-MAX-JOINS", result.stdout)
        self.assertIn("SPARK-MAX-JOINS", result.stdout)

    def test_missing_path_exits_two(self) -> None:
        result = run_cli("check", "no-existe.sql")
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
