"""Reglas de buenas prácticas, trazadas a las guías de HiveQL y Spark."""

from __future__ import annotations

import re
from dataclasses import dataclass

from etl_framework.config import Config
from etl_framework.model import Finding, snippet_at
from etl_framework.sql_analyzer import HiveModel, analyze_sql
from etl_framework.spark_analyzer import SparkModel, analyze_spark


_TEMP_PREFIX_RE = re.compile(r"(?i)^(?:IN|TMP|TEMP|AGG)\d*_")

# Reglas que no se pueden afirmar cuando el SQL llegó armado con un f-string.
_PARTIAL_SQL = {
    "HIVE-PARTITION-FILTER",
    "HIVE-AGG-WITHOUT-FILTER",
    "HIVE-SMALL-FILES",
    "HIVE-INSERT-PARTITION",
    "HIVE-STORAGE-FORMAT",
    "HIVE-EMPTY",
}


@dataclass(frozen=True)
class RuleInfo:
    rule_id: str
    engine: str
    severity: str
    title: str
    summary: str


RULE_CATALOG: tuple[RuleInfo, ...] = (
    RuleInfo("HIVE-SELECT-STAR", "hive", "error", "No usar SELECT *", "Proyectar todas las columnas obliga a leer datos que el ETL no necesita."),
    RuleInfo("HIVE-ALIAS-STAR", "hive", "warning", "No usar alias.*", "Arrastrar alias.* evita listar columnas y esconde el contrato del paso."),
    RuleInfo("HIVE-MAX-JOINS", "hive", "error", "Máximo un JOIN por SELECT", "Cada SELECT (o CTE) debe unir como máximo dos tablas. El resto va en pasos TMP_/AGG_."),
    RuleInfo("HIVE-CROSS-JOIN", "hive", "error", "No usar CROSS JOIN", "Un producto cartesiano explota el volumen intermedio."),
    RuleInfo("HIVE-BROKEN-CTE", "hive", "error", "El ';' corta el WITH", "Un punto y coma dentro del WITH deja la CTE fuera de alcance del SELECT que la usa."),
    RuleInfo("HIVE-TEMP-PREFIX", "hive", "error", "Prefijo de tabla temporal", "Las temporales se nombran IN_, TMP_/TMP1_, TEMP_ o AGG_/AGG1_."),
    RuleInfo("HIVE-STORAGE-FORMAT", "hive", "error", "Tabla final en Parquet u ORC", "La tabla permanente debe declararse STORED AS PARQUET u ORC."),
    RuleInfo("HIVE-PARTITION-FILTER", "hive", "warning", "Filtro por partición en la extracción", "La tabla de origen del FROM debe filtrar anio/mes/dia, year o ingestion_*."),
    RuleInfo("HIVE-AGG-WITHOUT-FILTER", "hive", "warning", "Agregación sin filtro previo", "GROUP BY o SUM/COUNT sobre una tabla externa sin WHERE mueve todo el dataset."),
    RuleInfo("HIVE-DISTINCT", "hive", "warning", "DISTINCT es costoso", "Preferir GROUP BY o ROW_NUMBER() para deduplicar."),
    RuleInfo("HIVE-INSERT-PARTITION", "hive", "warning", "INSERT sin PARTITION", "Si la tabla se creó particionada en el mismo script, el INSERT debe declarar PARTITION."),
    RuleInfo("HIVE-SMALL-FILES", "hive", "info", "Merge de archivos pequeños", "Conviene activar hive.merge.mapfiles (o tez/spark files) antes de insertar."),
    RuleInfo("HIVE-EMPTY", "hive", "info", "Script sin sentencias ETL", "No se encontró SELECT, CREATE ni INSERT para revisar."),
    RuleInfo("SPARK-SELECT-STAR", "spark", "error", "No usar select(\"*\")", "Equivalente Spark de SELECT *: proyectar solo las columnas necesarias."),
    RuleInfo("SPARK-ALIAS-STAR", "spark", "warning", "No usar col(\"a.*\")", "El template de referencia usa a.*, pero la regla pide columnas explícitas."),
    RuleInfo("SPARK-MAX-JOINS", "spark", "error", "Máximo dos JOIN por etapa", "Una sentencia no debe encadenar más de dos join. El resto va en DataFrames intermedios."),
    RuleInfo("SPARK-CROSS-JOIN", "spark", "error", "No usar cross join", "how='cross' o crossJoin() genera un producto cartesiano."),
    RuleInfo("SPARK-RDD", "spark", "warning", "Preferir DataFrames sobre RDD", "Los RDD no pasan por Catalyst. Dejarlos solo para datos no estructurados."),
    RuleInfo("SPARK-PARTITION-FILTER", "spark", "warning", "Lectura sin filtro de partición", "spark.table/spark.read debe filtrar la clave de partición en la misma expresión."),
    RuleInfo("SPARK-AGG-WITHOUT-FILTER", "spark", "warning", "Agregación sin filtro previo", "groupBy sobre datos crudos dispara un shuffle evitable."),
    RuleInfo("SPARK-SMALL-FILES", "spark", "warning", "Escritura sin coalesce o repartition", "Sin coalesce(n) o repartition(n) el write puede dejar miles de archivos pequeños."),
    RuleInfo("SPARK-STORAGE-FORMAT", "spark", "warning", "Escribir Parquet u ORC", "El formato de salida del ETL es Parquet u ORC, no CSV/JSON/texto."),
    RuleInfo("SPARK-WRITE-PARTITION", "spark", "warning", "Escritura sin partitionBy", "La tabla final se particiona (partitionBy) para podar lecturas posteriores."),
    RuleInfo("SPARK-CACHE-REUSE", "spark", "warning", "Reutilizar un DataFrame sin cache", "Si un DataFrame entra en más de una acción o transformación, hay que cache() o persist()."),
    RuleInfo("SPARK-NAMING", "spark", "warning", "Prefijo del DataFrame de join", "El resultado de un join se nombra in_, tmp_, temp_, agg_ o df_."),
    RuleInfo("SPARK-AQE", "spark", "info", "Activar Adaptive Query Execution", "En joins o agregaciones conviene spark.sql.adaptive.enabled=true."),
    RuleInfo("SPARK-SHUFFLE-PARTITIONS", "spark", "info", "Fijar shuffle.partitions", "Dejar spark.sql.shuffle.partitions en el default suele desbalancear el ETL."),
    RuleInfo("SPARK-BROADCAST", "spark", "info", "Broadcast de la tabla chica", "Si una pata del join cabe en memoria, broadcast() evita el shuffle."),
    RuleInfo("SPARK-DYNAMIC-SQL", "spark", "info", "SQL armado en runtime", "Un spark.sql() con variable o f-string solo se revisa de forma parcial."),
    RuleInfo("SPARK-SYNTAX", "spark", "error", "El script no compila", "El analizador necesita un Python válido para leer el AST."),
)


def catalog_ids() -> set[str]:
    return {rule.rule_id for rule in RULE_CATALOG}


def _emit(
    findings: list[Finding],
    config: Config,
    *,
    rule_id: str,
    severity: str,
    line: int,
    message: str,
    suggestion: str,
    snippet: str,
    engine: str,
) -> None:
    effective = config.apply(rule_id, severity)
    if effective is None:
        return
    findings.append(
        Finding(
            rule_id=rule_id,
            severity=effective,
            line=line,
            message=message,
            suggestion=suggestion,
            snippet=snippet,
            engine=engine,
        )
    )


def hive_findings(
    model: HiveModel,
    source: str,
    config: Config,
    *,
    engine: str = "hive",
    line_base: int = 1,
    suppress: set[str] | None = None,
    embedded: bool = False,
) -> list[Finding]:
    findings: list[Finding] = []
    blocked = set(suppress or ())
    prefix = "En spark.sql(): " if embedded else ""

    def add(rule_id: str, severity: str, line: int, message: str, suggestion: str) -> None:
        if rule_id in blocked:
            return
        _emit(
            findings,
            config,
            rule_id=rule_id,
            severity=severity,
            line=line_base + line - 1,
            message=prefix + message,
            suggestion=suggestion,
            snippet=snippet_at(source, line),
            engine=engine,
        )

    if not model.has_activity and not embedded:
        add(
            "HIVE-EMPTY",
            "info",
            1,
            "No hay SELECT, CREATE ni INSERT que validar.",
            "Pase un script ETL completo o un directorio con archivos .sql/.py.",
        )

    for scope in model.scopes:
        for line in sorted(set(scope.bare_stars)):
            add(
                "HIVE-SELECT-STAR",
                "error",
                line,
                "Se proyecta SELECT *. Hay que listar solo las columnas que el paso necesita.",
                "Reemplace * por la lista de columnas. COUNT(*) no cuenta como SELECT *.",
            )
        for line in sorted(set(scope.qualified_stars)):
            add(
                "HIVE-ALIAS-STAR",
                "warning",
                line,
                "Se usa alias.*. El paso debería declarar las columnas que entrega.",
                "Aunque el origen sea una temporal, escriba las columnas. Evita arrastrar campos de más.",
            )
        if scope.join_count > model.max_joins:
            extra = scope.join_lines[model.max_joins] if len(scope.join_lines) > model.max_joins else scope.line
            add(
                "HIVE-MAX-JOINS",
                "error",
                extra,
                f"Este SELECT tiene {scope.join_count} JOIN; el máximo es {model.max_joins} por bloque.",
                "Deje un JOIN por SELECT. Materialice el resultado en IN_, TMP_ o AGG_ y una la siguiente tabla en otro paso.",
            )
        if scope.cross_join:
            add(
                "HIVE-CROSS-JOIN",
                "error",
                scope.cross_line or scope.line,
                "CROSS JOIN materializa un producto cartesiano.",
                "Relacione las tablas con ON, o agregue antes si la relación es de uno a muchos.",
            )
        broken = [ref for ref in scope.tables if ref.broken_cte]
        if broken:
            names = ", ".join(dict.fromkeys(ref.base for ref in broken))
            add(
                "HIVE-BROKEN-CTE",
                "error",
                broken[0].line,
                f"Un ';' cierra el WITH antes de usar {names}. En este SELECT ese nombre ya no es una CTE.",
                "Quite el ';' que quedó dentro del paréntesis del WITH. El INSERT debe ir en el mismo statement.",
            )
        if scope.has_distinct:
            add(
                "HIVE-DISTINCT",
                "warning",
                scope.line,
                "DISTINCT (o COUNT DISTINCT) obliga a un shuffle y un sort global.",
                "Deduplique con GROUP BY o con ROW_NUMBER() sobre la clave, como en la reingeniería.",
            )
        driving = scope.external_from
        if driving and not scope.partition_filtered:
            names = ", ".join(ref.name for ref in driving)
            add(
                "HIVE-PARTITION-FILTER",
                "warning",
                driving[0].line,
                f"La extracción de {names} no filtra por clave de partición.",
                "Agregue al WHERE una condición sobre anio/mes/dia, year o ingestion_year/month/day.",
            )
        if driving and (scope.group_by or scope.has_aggregate) and not scope.has_where:
            add(
                "HIVE-AGG-WITHOUT-FILTER",
                "warning",
                scope.line,
                "Hay una agregación sobre una tabla externa y el SELECT no tiene WHERE.",
                "Filtre por partición (y por la regla de negocio) antes del GROUP BY o del SUM/COUNT.",
            )

    for temp in model.temp_tables:
        if not _TEMP_PREFIX_RE.match(temp.base):
            add(
                "HIVE-TEMP-PREFIX",
                "error",
                temp.line,
                f"La temporal {temp.name} no usa prefijo IN_, TMP_/TMPn_, TEMP_ o AGG_/AGGn_.",
                "IN_ para la extracción, TMP_ para la maestra y los joins, AGG_ para el agregado previo al join.",
            )

    allowed = {fmt.upper() for fmt in config.storage_formats}
    partitioned_bases = {
        created.base.lower()
        for created in model.creates
        if created.partitioned and not created.temporary
    }
    for created in model.creates:
        if created.temporary:
            continue
        fmt = (created.storage_format or "").upper()
        if fmt not in allowed:
            shown = created.storage_format or "sin formato (TEXTFILE por defecto)"
            add(
                "HIVE-STORAGE-FORMAT",
                "error",
                created.line,
                f"La tabla permanente {created.name} queda en {shown}.",
                "Declare STORED AS PARQUET o STORED AS ORC.",
            )

    for insert in model.inserts:
        if insert.base.lower() in partitioned_bases and not insert.has_partition:
            add(
                "HIVE-INSERT-PARTITION",
                "warning",
                insert.line,
                f"El INSERT hacia {insert.target} no declara PARTITION, pero la tabla se creó particionada en este script.",
                "Use INSERT OVERWRITE TABLE ... PARTITION (anio, mes) o la partición estática que corresponda.",
            )

    if model.inserts and not model.has_merge_config:
        add(
            "HIVE-SMALL-FILES",
            "info",
            model.inserts[0].line,
            "El script inserta datos y no configura hive.merge.* para fusionar archivos pequeños.",
            "Antes del INSERT: SET hive.merge.mapfiles=true; (o hive.merge.tezfiles / hive.merge.sparkfiles).",
        )
    return findings


def check_hive_source(source: str, config: Config) -> list[Finding]:
    return hive_findings(analyze_sql(source, config), source, config)


def check_spark_source(source: str, config: Config) -> list[Finding]:
    model = analyze_spark(source, config)
    return spark_findings(model, source, config)


def spark_findings(model: SparkModel, source: str, config: Config) -> list[Finding]:
    findings: list[Finding] = []

    def add(rule_id: str, severity: str, line: int, message: str, suggestion: str) -> None:
        _emit(
            findings,
            config,
            rule_id=rule_id,
            severity=severity,
            line=line,
            message=message,
            suggestion=suggestion,
            snippet=snippet_at(source, line),
            engine="spark",
        )

    if model.syntax_error:
        add(
            "SPARK-SYNTAX",
            "error",
            model.syntax_line or 1,
            f"No se pudo leer el script: {model.syntax_error}",
            "Corrija la sintaxis Python. El validador no ejecuta el ETL, solo lee el AST.",
        )
        return findings

    for star in model.stars:
        if star.qualified:
            add(
                "SPARK-ALIAS-STAR",
                "warning",
                star.line,
                "select/col con alias.* arrastra todas las columnas del paso anterior.",
                "Liste las columnas en .select(col(\"a.id\"), ...).",
            )
        else:
            add(
                "SPARK-SELECT-STAR",
                "error",
                star.line,
                "select(\"*\") lee todas las columnas.",
                "Proyecte solo los campos que el paso siguiente usa.",
            )

    for stage in model.joins:
        if stage.count > config.spark_max_joins:
            add(
                "SPARK-MAX-JOINS",
                "error",
                stage.line,
                f"Esta sentencia encadena {stage.count} join; el máximo por etapa es {config.spark_max_joins}.",
                "Corte el encadenado: asigne el resultado a un DataFrame tmp_/df_ y haga el siguiente join en otra sentencia.",
            )
        if stage.cross:
            add(
                "SPARK-CROSS-JOIN",
                "error",
                stage.line,
                "La etapa usa un cross join.",
                "Indique la clave con on= y how='left' o 'inner'. broadcast() no convierte un cross join en un join válido.",
            )

    for line in sorted(set(model.rdd_lines)):
        add(
            "SPARK-RDD",
            "warning",
            line,
            "Se baja a RDD (.rdd). Esa transformación no la optimiza Catalyst.",
            "Mantenga el flujo en DataFrame. Use RDD solo si el dato no tiene esquema.",
        )

    for read in model.reads:
        if read.partition_filtered:
            continue
        add(
            "SPARK-PARTITION-FILTER",
            "warning",
            read.line,
            f"La lectura de {read.source!r} no muestra un filtro por columna de partición en la misma expresión.",
            "Encadene .filter(col(\"anio\") == ...) o .where(\"anio = ...\") justo después de spark.table/spark.read.",
        )

    for agg in model.unfiltered_aggs:
        add(
            "SPARK-AGG-WITHOUT-FILTER",
            "warning",
            agg.line,
            "groupBy/agg sobre un DataFrame sin filter() o where() previo.",
            "Filtre por partición y por regla de negocio antes de agregar. Si la relación es 1:N, agregue y después haga el join.",
        )

    allowed = {fmt.lower() for fmt in config.storage_formats}
    for write in model.writes:
        if not write.coalesced:
            add(
                "SPARK-SMALL-FILES",
                "warning",
                write.line,
                "La escritura no pasa por coalesce(n) ni repartition(n).",
                "Reduzca archivos con .coalesce(n) antes de write, como indica la guía de Spark.",
            )
        if not write.insert_into:
            fmt = (write.format or "").lower()
            if fmt not in allowed:
                shown = write.format or "sin formato explícito"
                add(
                    "SPARK-STORAGE-FORMAT",
                    "warning",
                    write.line,
                    f"La salida queda en {shown}.",
                    "Use .format(\"parquet\") / .parquet() o ORC, y saveAsTable si la tabla vive en el metastore.",
                )
            if not write.partitioned:
                add(
                    "SPARK-WRITE-PARTITION",
                    "warning",
                    write.line,
                    "La escritura no declara partitionBy.",
                    "Particione la salida (.partitionBy(\"anio\", \"mes\")) para que las lecturas posteriores poden.",
                )

    for reuse in model.reuses:
        add(
            "SPARK-CACHE-REUSE",
            "warning",
            reuse.line,
            f"El DataFrame {reuse.variable} se usa {reuse.uses} veces y no está cacheado ni persistido.",
            "Llame .cache() o .persist(StorageLevel.MEMORY_AND_DISK) si el linaje se reutiliza.",
        )

    for name in model.bad_names:
        add(
            "SPARK-NAMING",
            "warning",
            name.line,
            f"El resultado del join quedó en {name.variable!r}, fuera del prefijo in_/tmp_/temp_/agg_/df_.",
            "Use tmp_ para la maestra, agg_ para el agregado y df_ si el script sigue la guía en Python.",
        )

    if model.has_shuffle_work and not model.has_aqe:
        line = model.joins[0].line if model.joins else 1
        add(
            "SPARK-AQE",
            "info",
            line,
            "No se activa spark.sql.adaptive.enabled.",
            "En el SparkSession: .config(\"spark.sql.adaptive.enabled\", \"true\"). AQE recorta shuffles sesgados.",
        )
    if model.has_shuffle_work and not model.has_shuffle_partitions:
        line = model.joins[0].line if model.joins else 1
        add(
            "SPARK-SHUFFLE-PARTITIONS",
            "info",
            line,
            "No se fija spark.sql.shuffle.partitions.",
            "Ajuste el valor al volumen (la guía de ejemplo usa 200) en lugar de heredar el default.",
        )
    if model.joins and not model.has_broadcast:
        add(
            "SPARK-BROADCAST",
            "info",
            model.joins[0].line,
            "Hay joins y no aparece broadcast() ni hint('broadcast').",
            "Si una de las tablas es chica (la guía habla de menos de ~10 MB), únala con broadcast(df).",
        )
    for line in sorted(set(model.dynamic_sql_lines)):
        add(
            "SPARK-DYNAMIC-SQL",
            "info",
            line,
            "spark.sql() recibe una variable o un f-string. La revisión de ese SQL es parcial.",
            "Si el texto es estático, pase un string literal para que se apliquen las reglas Hive dentro del SQL.",
        )

    for block in model.embedded:
        suppress = _PARTIAL_SQL if block.partial else {"HIVE-EMPTY"}
        nested = hive_findings(
            block.model,
            block.sql,
            config,
            engine="spark",
            line_base=block.line,
            suppress=suppress,
            embedded=True,
        )
        findings.extend(nested)
    return findings
