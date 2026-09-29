# Template Spark

Plantilla PySpark del mismo flujo que el [template HiveQL](04-template-hiveql.md). Un join por paso (por debajo del máximo de dos por etapa), columnas explícitas, agregado antes del detalle de uno a muchos, y escritura Parquet particionada.

Las dimensiones del ejemplo se leen sin filtro. Si están particionadas, el filtro va en esa lectura; si no lo están, el validador emite `SPARK-PARTITION-FILTER` como advertencia. `col("a.*")` no se usa: cada paso declara las columnas que entrega.

El script de referencia aprobado está en `examples/spark/etl_ok.py`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, count, sum as sum_

spark = (
    SparkSession.builder.appName("etl_transacciones")
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

anio_proceso = 2025
mes_proceso = 6

# 1. Extracción. Filtro de partición y columnas en la misma cadena.
in_tabla1 = (
    spark.table("datos.transacciones")
    .filter(
        (col("estado") == "ACTIVO")
        & (col("anio") == anio_proceso)
        & (col("mes") == mes_proceso)
    )
    .select(
        "id_transaccion",
        "fecha_transaccion",
        "monto",
        "estado",
        "anio",
        "mes",
        "dia",
    )
)

# 2. Maestra. Un join. broadcast solo si el lado derecho es chico.
in_clientes = spark.table("datos.clientes").select(
    "id_transaccion", "nombre_cliente", "segmento"
)
tmp_maestra = in_tabla1.alias("a").join(
    broadcast(in_clientes.alias("b")),
    col("a.id_transaccion") == col("b.id_transaccion"),
    "left",
).select(
    col("a.id_transaccion"),
    col("a.fecha_transaccion"),
    col("a.monto"),
    col("a.anio"),
    col("a.mes"),
    col("b.nombre_cliente"),
    col("b.segmento"),
)

# 3. Una fuente por paso.
in_productos = spark.table("datos.productos").select(
    "id_transaccion", "tipo_producto"
)
tmp_maestra2 = tmp_maestra.alias("a").join(
    in_productos.alias("c"),
    col("a.id_transaccion") == col("c.id_transaccion"),
    "left",
).select(
    col("a.id_transaccion"),
    col("a.fecha_transaccion"),
    col("a.monto"),
    col("a.anio"),
    col("a.mes"),
    col("a.nombre_cliente"),
    col("a.segmento"),
    col("c.tipo_producto"),
)

in_regiones = spark.table("datos.regiones").select(
    "id_transaccion", "nombre_region"
)
tmp_maestra3 = tmp_maestra2.alias("a").join(
    in_regiones.alias("d"),
    col("a.id_transaccion") == col("d.id_transaccion"),
    "left",
).select(
    col("a.id_transaccion"),
    col("a.fecha_transaccion"),
    col("a.monto"),
    col("a.anio"),
    col("a.mes"),
    col("a.nombre_cliente"),
    col("a.segmento"),
    col("a.tipo_producto"),
    col("d.nombre_region"),
)

# 4. Uno a muchos: agregar y después unir.
tabla4 = (
    spark.table("datos.tabla4")
    .filter((col("anio") == anio_proceso) & (col("mes") == mes_proceso))
    .select("id_transaccion")
)
tabla5 = (
    spark.table("datos.tabla5")
    .filter((col("anio") == anio_proceso) & (col("mes") == mes_proceso))
    .select("id_transaccion", "id_pago", "monto_pago")
)
agg_tabla45 = (
    tabla4.alias("t4")
    .join(
        tabla5.alias("t5"),
        col("t4.id_transaccion") == col("t5.id_transaccion"),
        "inner",
    )
    .groupBy(col("t4.id_transaccion"))
    .agg(
        count(col("t5.id_pago")).alias("total_pagos"),
        sum_(col("t5.monto_pago")).alias("suma_pagos"),
    )
)

tmp_maestra4 = tmp_maestra3.alias("a").join(
    agg_tabla45.alias("agg"),
    col("a.id_transaccion") == col("agg.id_transaccion"),
    "left",
).select(
    col("a.id_transaccion"),
    col("a.fecha_transaccion"),
    col("a.monto"),
    col("a.anio"),
    col("a.mes"),
    col("a.nombre_cliente"),
    col("a.segmento"),
    col("a.tipo_producto"),
    col("a.nombre_region"),
    col("agg.total_pagos"),
    col("agg.suma_pagos"),
)

# 5. Escritura. coalesce para no dejar un archivo por tarea.
(
    tmp_maestra4.coalesce(50)
    .write.format("parquet")
    .mode("overwrite")
    .partitionBy("anio", "mes")
    .saveAsTable("resultado.transacciones_finales")
)

spark.sql(
    "ANALYZE TABLE resultado.transacciones_finales COMPUTE STATISTICS"
)
spark.stop()
```

Cachear `in_tabla1` o `tmp_maestra` solo si otro paso los vuelve a usar. `distinct()` al final no corrige un join que ya multiplicó filas: esa corrección es el `groupBy` del paso 4.

`saveAsTable` crea o reemplaza la tabla en el metastore. Si el destino es un directorio y no una tabla, el mismo DataFrame se escribe con `.parquet(ruta)` y el mismo `partitionBy`.
