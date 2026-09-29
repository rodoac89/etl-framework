"""ETL PySpark que incumple las reglas de la guía."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("etl_malo").getOrCreate()

df = spark.read.parquet("hdfs://datos/ventas")
otro = spark.table("clientes")
zonas = spark.table("zonas")
tipos = spark.table("tipos")

resultado = (
    df.join(otro, "id")
    .join(zonas, "zona_id")
    .join(tipos, how="cross")
    .select("*")
    .groupBy("region")
    .agg({"monto": "sum"})
)

salida = df.rdd.map(lambda row: row.monto)
resultado.write.csv("hdfs://salida/ventas")
_ = salida
