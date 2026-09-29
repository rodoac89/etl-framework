"""Reglas PySpark. No hace falta tener pyspark instalado: solo se lee el AST."""

from __future__ import annotations

import textwrap
import unittest
from pathlib import Path

from etl_framework.config import Config
from etl_framework.engine import check_path
from etl_framework.rules import catalog_ids, check_spark_source

ROOT = Path(__file__).resolve().parents[1]


def code(source: str) -> str:
    return textwrap.dedent(source).strip() + "\n"


def ids(source: str) -> set[str]:
    findings = check_spark_source(source, Config())
    unknown = {item.rule_id for item in findings} - catalog_ids()
    if unknown:
        raise AssertionError(unknown)
    return {item.rule_id for item in findings}


class SparkRulesTest(unittest.TestCase):
    def test_reference_example_is_clean(self) -> None:
        report = check_path(ROOT / "examples" / "spark" / "etl_ok.py", Config())
        self.assertEqual(
            [(f.rule_id, f.message) for f in report.findings],
            [],
        )
        self.assertEqual(report.status(), "APROBADO")

    def test_bad_example_is_rejected(self) -> None:
        report = check_path(ROOT / "examples" / "spark" / "etl_malo.py", Config())
        self.assertEqual(report.status(), "RECHAZADO")
        found = {f.rule_id for f in report.findings}
        self.assertIn("SPARK-SELECT-STAR", found)
        self.assertIn("SPARK-MAX-JOINS", found)
        self.assertIn("SPARK-CROSS-JOIN", found)
        self.assertIn("SPARK-RDD", found)
        self.assertIn("SPARK-PARTITION-FILTER", found)
        self.assertIn("SPARK-AGG-WITHOUT-FILTER", found)
        self.assertIn("SPARK-STORAGE-FORMAT", found)

    def test_two_joins_are_allowed_three_are_not(self) -> None:
        two = code(
            """
            df = spark.table("t").filter(col("anio") == 2025)
            a = spark.table("a").filter(col("anio") == 2025)
            b = spark.table("b").filter(col("anio") == 2025)
            out = df.join(broadcast(a), "id").join(b, "id")
            """
        )
        three = code(
            """
            df = spark.table("t").where("year = 2025")
            out = df.join(a, "id").join(b, "id").join(c, "id")
            """
        )
        self.assertNotIn("SPARK-SYNTAX", ids(two))
        self.assertNotIn("SPARK-MAX-JOINS", ids(two))
        self.assertNotIn("SPARK-SYNTAX", ids(three))
        self.assertIn("SPARK-MAX-JOINS", ids(three))

    def test_alias_star_is_warning_and_cache_reuse(self) -> None:
        source = code(
            """
            df = spark.table("datos.ventas").filter(col("anio") == 2025)
            df.select("id")
            otros = df.select(col("a.*"))
            """
        )
        found = ids(source)
        self.assertNotIn("SPARK-SYNTAX", found)
        self.assertIn("SPARK-ALIAS-STAR", found)
        self.assertNotIn("SPARK-SELECT-STAR", found)
        self.assertIn("SPARK-CACHE-REUSE", found)
        self.assertNotIn("SPARK-PARTITION-FILTER", found)

    def test_embedded_sql_uses_spark_join_limit(self) -> None:
        two_joins = code(
            '''
            spark.sql("""
                SELECT a.id, b.nombre, c.zona
                FROM datos.t a
                JOIN datos.u b ON a.id = b.id
                JOIN datos.v c ON a.id = c.id
                WHERE a.anio = 2025
            """)
            '''
        )
        three_joins = code(
            '''
            spark.sql("""
                SELECT a.id
                FROM datos.t a
                JOIN datos.u b ON a.id = b.id
                JOIN datos.v c ON a.id = c.id
                JOIN datos.w d ON a.id = d.id
                WHERE a.anio = 2025
            """)
            '''
        )
        self.assertNotIn("SPARK-SYNTAX", ids(two_joins))
        self.assertNotIn("HIVE-MAX-JOINS", ids(two_joins))
        self.assertIn("HIVE-MAX-JOINS", ids(three_joins))
        self.assertIn("HIVE-SELECT-STAR", ids('spark.sql("SELECT * FROM datos.t WHERE anio = 1")'))

    def test_dynamic_sql_is_partial(self) -> None:
        source = code(
            """
            anio = 2025
            spark.sql(f"SELECT * FROM datos.t WHERE anio = {anio}")
            """
        )
        found = ids(source)
        self.assertNotIn("SPARK-SYNTAX", found)
        self.assertIn("SPARK-DYNAMIC-SQL", found)
        self.assertIn("HIVE-SELECT-STAR", found)

    def test_syntax_error_rejects_the_file(self) -> None:
        found = ids("def etl(:\n    pass\n")
        self.assertEqual(found, {"SPARK-SYNTAX"})

    def test_write_without_format_and_unprefixed_join(self) -> None:
        source = code(
            """
            df = spark.table("datos.t").filter("ingestion_year = 2025")
            resultado = df.join(broadcast(df), "id")
            resultado.coalesce(1).write.mode("overwrite").save("hdfs://out")
            """
        )
        found = ids(source)
        self.assertNotIn("SPARK-SYNTAX", found)
        self.assertIn("SPARK-NAMING", found)
        self.assertIn("SPARK-STORAGE-FORMAT", found)
        self.assertIn("SPARK-WRITE-PARTITION", found)
        self.assertNotIn("SPARK-SMALL-FILES", found)
        self.assertNotIn("SPARK-BROADCAST", found)

    def test_coalesce_on_a_previous_line_counts(self) -> None:
        source = code(
            """
            df = spark.table("datos.t").filter(col("dia") == 1).coalesce(4)
            df.write.format("orc").partitionBy("dia").mode("overwrite").save("hdfs://out")
            """
        )
        found = ids(source)
        self.assertNotIn("SPARK-SYNTAX", found)
        self.assertNotIn("SPARK-SMALL-FILES", found)
        self.assertNotIn("SPARK-STORAGE-FORMAT", found)
        self.assertNotIn("SPARK-WRITE-PARTITION", found)


if __name__ == "__main__":
    unittest.main()
