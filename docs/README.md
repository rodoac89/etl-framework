# Documentación del framework ETL

Estas páginas describen cómo construir un ETL en HiveQL o PySpark para que lea poco, no duplique filas al unir y deje una tabla final particionada en formato columnar. El contenido sale de las guías de diseño del proyecto y está escrito sin depender de un proveedor de plataforma ni de un cliente concreto.

El validador de este repositorio revisa en el código la parte que se puede ver sin ejecutar el job. El plan de pruebas cubre conteos, rendimiento y calidad, que solo aparecen al correr el proceso.

## Por dónde empezar

| Documento | Para qué sirve |
| --- | --- |
| [Diseño de flujo](01-diseno-de-flujo.md) | Las cuatro etapas: extracción, filtros, tabla maestra y agregaciones. |
| [Buenas prácticas HiveQL](02-buenas-practicas-hiveql.md) | Particiones, CTE frente a tabla temporal, un JOIN por paso y lo que no hay que hacer. |
| [Guía HiveQL](03-guia-hiveql.md) | Los nueve pasos para escribir el script. |
| [Template HiveQL](04-template-hiveql.md) | Plantilla con tablas temporales, con CTE y mixta. |
| [Buenas prácticas Spark](05-buenas-practicas-spark.md) | DataFrames, cache, como máximo dos joins por etapa, broadcast y AQE. |
| [Guía Spark](06-guia-spark.md) | Los pasos equivalentes en PySpark. |
| [Template Spark](07-template-spark.md) | Plantilla PySpark alineada con el mismo flujo. |
| [Plan de pruebas Spark](08-plan-de-pruebas-spark.md) | Pruebas unitarias, integración, rendimiento, calidad y regresión. |
| [Reglas del validador](09-reglas-del-validador.md) | Qué regla del código corresponde a cada práctica, y con qué severidad. |

## El flujo que comparten las dos guías

1. Extraer cada fuente a un paso `IN_` (Hive) o `in_` (Spark), con filtro de partición y columnas explícitas.
2. Armar la maestra en pasos `TMP_` / `tmp_`. En Hive, un solo JOIN por `SELECT`. En Spark, como máximo dos joins por etapa.
3. Si la relación es de uno a muchos, resumir antes en un paso `AGG_` / `agg_` y recién ahí unir. El conteo de la maestra no debe crecer.
4. Escribir la tabla final en Parquet u ORC, particionada, y calcular estadísticas sobre esa tabla, no sobre las temporales.

Los prefijos admiten un número opcional (`TMP1_`, `Agg1_`). En Spark también vale `df_` para el resultado de un join.

## Qué no está en estas páginas

No hay metastore, así que nadie puede afirmar desde el texto si una dimensión está particionada o si un lado del join cabe en memoria. Por eso el validador trata el filtro de partición ausente y la falta de `broadcast()` como advertencia o nota, no como error. Las dimensiones de los propios templates (`clientes`, `productos`) se leen a veces sin partición.

Tampoco se describe aquí un producto, una cola con nombre de proyecto ni un volumen de un cliente. Donde las guías hablaban de volúmenes muy grandes, estas páginas dicen lo mismo en términos de datos: filtrar antes de mover, y no unir detalle de uno a muchos sin agregar.
