# Reglas del validador

El comando `python -m etl_framework rules` imprime este catálogo. La severidad de cada regla se puede apagar o subir en un JSON; `config.example.json` muestra el formato. `--strict` hace que cualquier advertencia rechace el script.

Esto es detección estática. No ejecuta Hive ni Spark, no consulta el metastore y no mira el plan. Por eso varias reglas que en la guía son obligatorias quedan en advertencia o en nota: sin el catálogo de tablas no se sabe si una fuente está particionada ni si un lado del join es chico.

## Hive

| Regla | Severidad | Práctica |
| --- | --- | --- |
| `HIVE-SELECT-STAR` | error | Proyectar columnas. `COUNT(*)` no cuenta como `SELECT *`. |
| `HIVE-ALIAS-STAR` | advertencia | No arrastrar `alias.*`. |
| `HIVE-MAX-JOINS` | error | Un JOIN por `SELECT` o CTE. El máximo se cambia con `hive_max_joins`. |
| `HIVE-CROSS-JOIN` | error | Sin producto cartesiano. |
| `HIVE-BROKEN-CTE` | error | Un `;` dentro del `WITH` deja la CTE fuera de la sentencia siguiente. |
| `HIVE-TEMP-PREFIX` | error | Temporales `IN_`, `TMP_`, `TEMP_` o `AGG_`, con número opcional (`TMP1_`). Las CTE no exigen prefijo. |
| `HIVE-STORAGE-FORMAT` | error | La tabla permanente se declara `STORED AS PARQUET` u `ORC`. |
| `HIVE-PARTITION-FILTER` | advertencia | La tabla externa del `FROM` filtra la clave de partición. No se exige en la dimensión unida. |
| `HIVE-AGG-WITHOUT-FILTER` | advertencia | `GROUP BY` o una agregación sobre una tabla externa pide `WHERE`. |
| `HIVE-DISTINCT` | advertencia | Preferir `GROUP BY` o `ROW_NUMBER()`. |
| `HIVE-INSERT-PARTITION` | advertencia | Si el mismo script creó la tabla con `PARTITIONED BY`, el `INSERT` declara `PARTITION`. |
| `HIVE-SMALL-FILES` | nota | Un `INSERT` sin `hive.merge.*` (u otra compactación del motor) conviene activarla. |
| `HIVE-EMPTY` | nota | No había `SELECT`, `CREATE` ni `INSERT`. |

Un `FROM` separado por comas cuenta como join. Comentarios y literales de texto no. Las columnas de partición por defecto son `anio`, `año`, `mes`, `dia`, `día`, `year`, `month`, `day`, `ingestion_year`, `ingestion_month` e `ingestion_day`.

## Spark

| Regla | Severidad | Práctica |
| --- | --- | --- |
| `SPARK-SELECT-STAR` | error | Sin `select("*")`. |
| `SPARK-ALIAS-STAR` | advertencia | Sin `col("a.*")`. |
| `SPARK-MAX-JOINS` | error | Como máximo dos `join` por sentencia. El máximo se cambia con `spark_max_joins`. |
| `SPARK-CROSS-JOIN` | error | Sin `how="cross"` ni `crossJoin()`. |
| `SPARK-SYNTAX` | error | El archivo tiene que ser Python que `ast` pueda leer. |
| `SPARK-RDD` | advertencia | No pasar por `.rdd` en un ETL tabular. |
| `SPARK-PARTITION-FILTER` | advertencia | El filtro de partición va en la misma cadena que `spark.table` o `spark.read`. |
| `SPARK-AGG-WITHOUT-FILTER` | advertencia | `groupBy` sin un filtro previo en el linaje. |
| `SPARK-SMALL-FILES` | advertencia | `coalesce` o `repartition` en la escritura, o en la asignación anterior de ese DataFrame. |
| `SPARK-STORAGE-FORMAT` | advertencia | Parquet u ORC, no CSV, JSON ni texto. |
| `SPARK-WRITE-PARTITION` | advertencia | `partitionBy` en la escritura. No se exige en `insertInto`, que ya escribe sobre una tabla definida. |
| `SPARK-CACHE-REUSE` | advertencia | Un DataFrame usado en más de una acción o rama lleva `cache` o `persist`. |
| `SPARK-NAMING` | advertencia | El resultado de un join se nombra `in_`, `tmp_`, `temp_`, `agg_` o `df_`. |
| `SPARK-AQE` | nota | Si hay joins o agregaciones y no se activa `spark.sql.adaptive.enabled`. |
| `SPARK-SHUFFLE-PARTITIONS` | nota | Conviene fijar `spark.sql.shuffle.partitions`. |
| `SPARK-BROADCAST` | nota | Si hay joins y no aparece `broadcast()` ni un hint, se recuerda el lado chico. |
| `SPARK-DYNAMIC-SQL` | nota | Un `spark.sql()` armado con variable o f-string solo se revisa en parte. |

El SQL embebido en `spark.sql("...")` se analiza con las reglas Hive y el tope de joins de Spark. En un f-string no se afirman el filtro de partición, la agregación sin filtro, el formato, el `INSERT` ni los archivos pequeños, porque el texto está incompleto.

## Estados

| Estado | Cuándo | Código de salida |
| --- | --- | --- |
| APROBADO | Sin errores. Puede haber notas. | 0 |
| CON OBSERVACIONES | Solo advertencias. | 0, o 1 con `--strict` |
| RECHAZADO | Al menos un error. | 1 |
| OMITIDO | El archivo no es Hive ni un PySpark reconocible. | 0. No rechaza el proceso. |

Si la ruta no existe, o el directorio no tiene scripts `.sql`, `.hql`, `.q`, `.ddl` o `.py`, el proceso termina con código 2.

## Lo que queda para el plan de pruebas

Conteos antes y después del join, sumas, estadísticas reales, tiempo, shuffle y sesgo no se pueden decidir leyendo el archivo. Están en el [plan de pruebas](08-plan-de-pruebas-spark.md).
