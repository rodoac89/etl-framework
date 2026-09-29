"""ETL de referencia PySpark. Debe quedar APROBADO, sin advertencias."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = (
    SparkSession.builder.appName("etl_ok")
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

anio_proceso = 2025
mes_proceso = 6

in_transacciones = (
    spark.table("datos.transacciones")
    .filter(
        (col("anio") == anio_proceso)
        & (col("mes") == mes_proceso)
        & (col("estado") == "ACTIVO")
    )
    .select("id_transaccion", "monto", "anio", "mes")
    .cache()
)

in_clientes = (
    spark.table("datos.clientes")
    .filter(col("anio") == anio_proceso)
    .select("id_transaccion", "nombre_cliente")
)

tmp_maestra = in_transacciones.join(
    broadcast(in_clientes),
    "id_transaccion",
    "left",
).select(
    col("id_transaccion"),
    col("monto"),
    col("nombre_cliente"),
    col("anio"),
    col("mes"),
)

(
    tmp_maestra.coalesce(10)
    .write.format("parquet")
    .mode("overwrite")
    .partitionBy("anio", "mes")
    .saveAsTable("resultado.transacciones_finales")
)
