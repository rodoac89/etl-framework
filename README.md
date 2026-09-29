# Framework ETL — detección de buenas prácticas

Este framework revisa un script **HiveQL** o **PySpark** y dice si cumple reglas de buenas prácticas, sin conectarse a ningún clúster y sin ejecutar el ETL.

## Qué revisa

El flujo que exigen el diseño y los templates es el mismo en los dos motores:

1. Extraer cada fuente a un paso `IN_` / `in_`, con filtro de partición y columnas explícitas.
2. Armar la maestra en pasos `TMP_` / `tmp_` (o `df_`), con un solo JOIN por SELECT en Hive y como máximo dos joins por sentencia en Spark.
3. Si la relación es de uno a muchos, agregar antes en un paso `AGG_` / `agg_` y recién ahí unir.
4. Escribir la tabla final en Parquet u ORC, particionada, sin `SELECT *` ni producto cartesiano.

| Estado | Significado |
| --- | --- |
| APROBADO | Sin errores. Puede haber notas informativas. |
| CON OBSERVACIONES | Solo advertencias. El script no se rechaza. |
| RECHAZADO | Hay al menos un error. |
| OMITIDO | El archivo no es Hive ni un script PySpark. |

`python -m etl_framework rules` lista cada regla, el motor y la severidad por defecto. Un JSON de configuración puede apagar una regla o subirla de advertencia a error. Ver `config.example.json`.

Las columnas de partición que reconoce por defecto son las de las guías (`anio`, `mes`, `dia`) y las de la reingeniería (`year`, `ingestion_year`, `ingestion_month`, `ingestion_day`).

## Uso

Desde la raíz del repositorio:

```powershell
python -m etl_framework check examples\hive\etl_ok.sql
python -m etl_framework check examples\spark\etl_ok.py
python -m etl_framework check mi_etl.py --format json --output reporte.json
python -m etl_framework check mi_etl.sql --config config.example.json --strict
```

`--strict` hace que las advertencias también devuelvan código de salida 1. Sin ese flag, solo los errores fallan el proceso (útil para engancharlo en un pipeline).

Código de salida: `0` aprobado o con observaciones, `1` rechazado, `2` ruta inexistente o sin scripts.

No hace falta instalar PySpark. El análisis de Python usa el AST de la biblioteca estándar.

## Ejemplos

- `examples/hive/etl_ok.sql` y `examples/spark/etl_ok.py` siguen el flujo y quedan aprobados.
- `examples/hive/etl_malo.sql` y `examples/spark/etl_malo.py` concentran los incumplimientos que las guías marcan como "no hacer".

## Documentación

El diseño del flujo, las buenas prácticas, las guías paso a paso, los templates y el plan de pruebas están en [`docs/`](docs/README.md). Esa documentación es genérica: conserva las reglas de los entregables y no nombra un proveedor de plataforma ni un cliente.

## Qué no puede ver

No hay metastore, así que no sabe qué tablas están realmente particionadas ni el tamaño de cada lado de un join. Por eso el filtro de partición ausente y la falta de `broadcast()` son advertencia o nota, no error: en los propios templates hay dimensiones (`clientes`, `productos`) que se leen sin partición. Si en un proyecto todas las fuentes están particionadas, suba `HIVE-PARTITION-FILTER` y `SPARK-PARTITION-FILTER` a `error` en el JSON.

Tampoco mira el plan físico, el Spark UI ni la cuadratura de conteos. Eso corresponde a las fases de ejecución del plan de pruebas, no a esta detección estática.

## Pruebas

```powershell
python -m unittest discover -s tests -v
```
