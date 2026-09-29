"""Reglas Hive sobre SQL de las guías y sobre la reingeniería."""

from __future__ import annotations

import unittest
from pathlib import Path

from etl_framework.config import Config, load_config
from etl_framework.engine import check_path, check_paths
from etl_framework.rules import catalog_ids, check_hive_source

ROOT = Path(__file__).resolve().parents[1]
REING = ROOT / "03. Entregables" / "08. Reingeniería" / "hive"


def ids(source: str, config: Config | None = None) -> set[str]:
    findings = check_hive_source(source, config or Config())
    unknown = {f.rule_id for f in findings} - catalog_ids()
    if unknown:
        raise AssertionError(f"reglas fuera del catálogo: {unknown}")
    return {f.rule_id for f in findings}


def by_severity(source: str) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {"error": set(), "warning": set(), "info": set()}
    for finding in check_hive_source(source, Config()):
        grouped[finding.severity].add(finding.rule_id)
    return grouped


class HiveRulesTest(unittest.TestCase):
    def test_count_star_and_multiplication_are_not_select_star(self) -> None:
        source = """
            SELECT monto * cantidad AS total, COUNT(*) AS n
            FROM datos.ventas
            WHERE anio = 2025;
        """
        self.assertNotIn("HIVE-SELECT-STAR", ids(source))
        self.assertNotIn("HIVE-PARTITION-FILTER", ids(source))

    def test_select_star_is_error_even_inside_a_string_or_comment_is_ignored(self) -> None:
        source = """
            -- SELECT * FROM secreto
            SELECT 'SELECT *' AS ejemplo, id
            FROM datos.ventas
            WHERE anio = 2025;
            SELECT * FROM datos.ventas WHERE anio = 2025;
        """
        found = ids(source)
        self.assertIn("HIVE-SELECT-STAR", found)
        findings = check_hive_source(source, Config())
        stars = [f for f in findings if f.rule_id == "HIVE-SELECT-STAR"]
        self.assertEqual(len(stars), 1)
        self.assertIn("SELECT *", stars[0].snippet)

    def test_alias_star_is_warning(self) -> None:
        source = """
            SELECT a.*, b.id
            FROM IN_TABLA a
            JOIN datos.clientes b ON a.id = b.id
            WHERE b.anio = 2025;
        """
        grouped = by_severity(source)
        self.assertIn("HIVE-ALIAS-STAR", grouped["warning"])
        self.assertNotIn("HIVE-SELECT-STAR", grouped["error"])

    def test_two_joins_in_one_select_fail_but_one_per_cte_passes(self) -> None:
        monolithic = """
            SELECT a.id
            FROM datos.t1 a
            JOIN datos.t2 b ON a.id = b.id
            JOIN datos.t3 c ON a.id = c.id
            WHERE a.anio = 2025;
        """
        modular = """
            WITH paso_b AS (
                SELECT a.id
                FROM datos.t1 a
                JOIN datos.t2 b ON a.id = b.id
                WHERE a.year = 2025
            ),
            paso_c AS (
                SELECT a.id
                FROM datos.t3 a
                JOIN datos.t4 b ON a.id = b.id
                WHERE a.ingestion_year = 2025
            )
            SELECT a.id
            FROM paso_b a
            JOIN paso_c b ON a.id = b.id;
        """
        self.assertIn("HIVE-MAX-JOINS", ids(monolithic))
        self.assertNotIn("HIVE-MAX-JOINS", ids(modular))

    def test_comma_from_counts_as_joins(self) -> None:
        source = """
            SELECT a.id
            FROM datos.t1 a, datos.t2 b, datos.t3 c
            WHERE a.anio = 2025;
        """
        self.assertIn("HIVE-MAX-JOINS", ids(source))

    def test_cross_join_and_distinct_and_unfiltered_agg(self) -> None:
        source = """
            SELECT DISTINCT region, SUM(monto)
            FROM datos.ventas
            CROSS JOIN datos.tipos
            GROUP BY region;
        """
        found = ids(source)
        self.assertIn("HIVE-CROSS-JOIN", found)
        self.assertIn("HIVE-DISTINCT", found)
        self.assertIn("HIVE-AGG-WITHOUT-FILTER", found)
        self.assertIn("HIVE-PARTITION-FILTER", found)

    def test_temp_prefixes_accept_guide_names(self) -> None:
        source = """
            CREATE TEMPORARY TABLE Tmp1_TablaMaestra AS
            SELECT id FROM datos.t WHERE anio = 2025;
            CREATE TEMPORARY TABLE Agg1_Pagos AS
            SELECT id FROM datos.t WHERE mes = 6;
            CREATE TEMPORARY TABLE temp_avisos AS
            SELECT id FROM datos.t WHERE ingestion_day = 1;
            CREATE TEMPORARY TABLE IN_TABLA1 AS
            SELECT id FROM datos.t WHERE year = 2025;
        """
        self.assertNotIn("HIVE-TEMP-PREFIX", ids(source))
        bad = "CREATE TEMPORARY TABLE SalarioPromedio AS SELECT id FROM datos.t WHERE anio = 1;"
        self.assertIn("HIVE-TEMP-PREFIX", ids(bad))

    def test_storage_and_insert_partition(self) -> None:
        bad = """
            CREATE TABLE resultado.ventas (id STRING, anio INT)
            PARTITIONED BY (anio INT);
            INSERT INTO resultado.ventas SELECT id, anio FROM datos.ventas;
        """
        found = ids(bad)
        self.assertIn("HIVE-STORAGE-FORMAT", found)
        self.assertIn("HIVE-INSERT-PARTITION", found)
        self.assertIn("HIVE-SMALL-FILES", found)

        good = """
            SET hive.merge.mapfiles=true;
            CREATE TABLE resultado.ventas (id STRING)
            PARTITIONED BY (anio INT)
            STORED AS PARQUET;
            INSERT OVERWRITE TABLE resultado.ventas
            PARTITION (anio)
            SELECT id, anio FROM IN_VENTAS;
        """
        found_good = ids(good)
        self.assertNotIn("HIVE-STORAGE-FORMAT", found_good)
        self.assertNotIn("HIVE-INSERT-PARTITION", found_good)
        self.assertNotIn("HIVE-SMALL-FILES", found_good)

    def test_placeholder_partition_and_insert_without_table_keyword(self) -> None:
        source = """
            CREATE TABLE IF NOT EXISTS {zona_procesamiento}.ALERTAS (
                id INT
            )
            PARTITIONED BY (ingestion_year INT)
            STORED AS PARQUET;
            INSERT OVERWRITE {zona_procesamiento}.ALERTAS
            PARTITION (ingestion_year)
            SELECT id, ingestion_year
            FROM s_clientes.alertas
            WHERE year = {year_table};
        """
        found = ids(source)
        self.assertNotIn("HIVE-PARTITION-FILTER", found)
        self.assertNotIn("HIVE-INSERT-PARTITION", found)
        self.assertNotIn("HIVE-STORAGE-FORMAT", found)

    def test_reference_example_is_clean(self) -> None:
        report = check_path(ROOT / "examples" / "hive" / "etl_ok.sql", Config())
        self.assertEqual(report.findings, [], msg=report.findings)
        self.assertEqual(report.status(), "APROBADO")

    def test_bad_example_is_rejected(self) -> None:
        report = check_path(ROOT / "examples" / "hive" / "etl_malo.sql", Config())
        self.assertEqual(report.status(), "RECHAZADO")
        found = {f.rule_id for f in report.findings}
        self.assertIn("HIVE-SELECT-STAR", found)
        self.assertIn("HIVE-MAX-JOINS", found)
        self.assertIn("HIVE-CROSS-JOIN", found)
        self.assertIn("HIVE-TEMP-PREFIX", found)
        self.assertIn("HIVE-STORAGE-FORMAT", found)

    def test_semicolon_inside_with_is_an_error(self) -> None:
        source = """
            WITH ALERTAS_MASTER AS (
                SELECT id FROM s_clientes.t WHERE year = {year_table};
            )
            SELECT id FROM ALERTAS_MASTER WHERE rn = 1;
        """
        found = ids(source)
        self.assertIn("HIVE-BROKEN-CTE", found)
        self.assertNotIn("HIVE-PARTITION-FILTER", found)

    def test_reingenieria_only_flags_the_broken_with_in_002(self) -> None:
        reports = check_paths([REING], Config())
        self.assertGreaterEqual(len(reports), 9)
        for report in reports:
            error_ids = {f.rule_id for f in report.findings if f.severity == "error"}
            name = Path(report.path).name
            if name.startswith("002_"):
                self.assertEqual(error_ids, {"HIVE-BROKEN-CTE"})
                continue
            self.assertEqual(
                error_ids,
                set(),
                msg=f"{report.path}\n" + "\n".join(f"{f.rule_id}: {f.message}" for f in report.findings),
            )

    def test_config_can_promote_partition_filter_to_error(self) -> None:
        source = "SELECT id FROM datos.ventas WHERE producto = 'X';"
        config = load_config(ROOT / "config.example.json")
        findings = check_hive_source(source, config)
        partition = [f for f in findings if f.rule_id == "HIVE-PARTITION-FILTER"]
        self.assertEqual(len(partition), 1)
        self.assertEqual(partition[0].severity, "error")
        self.assertNotIn("HIVE-SMALL-FILES", {f.rule_id for f in findings})


if __name__ == "__main__":
    unittest.main()
