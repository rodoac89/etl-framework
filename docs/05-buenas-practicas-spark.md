# Buenas prácticas Spark

Las etapas son las mismas que en Hive: extraer poco, armar una maestra, agregar el uno a muchos antes de unirlo y escribir columnar y particionado. Lo que cambia es el mecanismo. Spark combina memoria y disco, optimiza con Catalyst y puede ajustar el plan en ejecución con Adaptive Query Execution (AQE). Esta página cubre solo Spark. Las reglas SQL, cuando el ETL se escribe con `spark.sql()`, son las de [HiveQL](02-buenas-practicas-hiveql.md), con un límite distinto de joins: dos por etapa, no uno.

PySpark es el estilo de referencia. La misma lógica se puede expresar en la API de otro lenguaje.

## DataFrames, no RDD

El DataFrame es la estructura por defecto. Catalyst empuja filtros y proyecciones. El RDD queda para datos no estructurados o una transformación que la API no expresa. Pasar a `.rdd` en un ETL tabular pierde esas optimizaciones. El validador lo advierte (`SPARK-RDD`).

```python
df = (
    spark.read.parquet("path/to/data")
    .filter("salario > 50000")
    .select("nombre", "salario")
)
```

## Vistas temporales

`createOrReplaceTempView` publica un DataFrame para reutilizarlo desde SQL dentro de la misma sesión. No reemplaza al cache: evita repetir el texto, no el cómputo, salvo que el DataFrame esté persistido.

```python
df.createOrReplaceTempView("temp_salarios")
result = spark.sql("""
    SELECT nombre, salario
    FROM temp_salarios
    WHERE salario > (SELECT AVG(salario) FROM temp_salarios)
""")
```

## Reutilizar con cache o persist

Si un DataFrame entra en más de una acción o en más de una rama, hay que `cache()` o `persist()`. Sin eso Spark recomputa el linaje. `persist` permite memoria y disco cuando el conjunto no cabe en memoria. El validador advierte `SPARK-CACHE-REUSE` cuando detecta esa reutilización sin cache.

```python
from pyspark import StorageLevel

df.cache()
df_grande = df.persist(StorageLevel.MEMORY_AND_DISK)
```

No cachear por reflejo un DataFrame que se usa una sola vez.

## Particiones de lectura y de cómputo

En la lectura, filtrar la clave de partición en la misma expresión que abre la tabla (`spark.table(...).filter(...)` o `spark.read.parquet(...).where(...)`). Un filtro en una sentencia posterior ya no poda esa lectura.

`repartition(n)` redistribuye y corrige un sesgo, a cambio de un shuffle. `coalesce(n)` baja la cantidad de particiones sin ese shuffle y es lo que se usa antes de escribir, para no dejar miles de archivos chicos.

```python
df = spark.read.parquet("path/to/partitioned/data").where("anio = 2025")
df.repartition(200, "region").write.partitionBy("anio").parquet("output")
```

El número de particiones de shuffle sale de `spark.sql.shuffle.partitions`. Dejar el default en un ETL grande desbalancea las tareas. AQE (`spark.sql.adaptive.enabled=true`) puede partir o fusionar particiones según el tamaño real y mitiga el sesgo de claves. El validador deja nota si hay joins o agregaciones y esas dos configuraciones no aparecen.

## Modularizar

Cada etapa es una función o un bloque con nombre (`in_`, `tmp_`, `agg_`, `df_`), testeable con un DataFrame de entrada conocido. Ver la [guía Spark](06-guia-spark.md).

```python
def extract_data(spark, path):
    return spark.read.parquet(path)

def transform_data(df):
    return df.filter("estado = 'activo'").groupBy("id").agg({"monto": "sum"})
```

## Joins

Como máximo dos `join` encadenados en la misma sentencia. El resto va a un DataFrame intermedio. Un join de más es error (`SPARK-MAX-JOINS`). `how="cross"` o `crossJoin()` también.

Si un lado es chico y cabe en memoria, `broadcast()` evita el shuffle. El validador no conoce el tamaño, así que la ausencia de broadcast es una nota (`SPARK-BROADCAST`), no un error. No broadcastear una tabla grande: la guía usa como referencia práctica un lado por debajo de unos 10 MB, y el umbral real lo marca la configuración del clúster.

```python
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), "id", "left")
```

## Qué no hacer

### Shuffle de más

Un join de dos tablas grandes seguido de un `groupBy` sin haber filtrado mueve el detalle dos veces. Filtrar y proyectar antes, broadcastear el lado chico y fijar `spark.sql.shuffle.partitions`.

### Sesgo

Una clave con casi todas las filas deja una tarea lenta y al resto ociosas. Reparticionar, salar la clave si hace falta, y dejar AQE activo.

### Archivos pequeños

`write` sin `coalesce` o `repartition` puede crear un archivo por partición de cómputo. Antes de la escritura final, bajar ese número. El validador lo advierte (`SPARK-SMALL-FILES`) si no ve `coalesce` o `repartition` en la cadena de escritura o en la asignación previa de ese DataFrame.

### Agregación sin filtro

`groupBy` sobre la lectura cruda hace shuffle del volumen entero. Filtrar primero (`SPARK-AGG-WITHOUT-FILTER`). Para un conteo aproximado de distintos, `approx_count_distinct` evita el distinto exacto cuando el negocio lo acepta.

### Formato y contrato de salida

La salida del ETL es Parquet u ORC, no CSV, JSON ni texto. La tabla final lleva `partitionBy` con la clave de partición. `select("*")` es error. `col("a.*")` es advertencia: lista las columnas que el paso entrega.

## Cómo mirar el costo

`df.explain()` muestra el plan. La interfaz de Spark muestra tiempo por stage, lectura y escritura de shuffle, y tareas sesgadas. Eso ya es ejecución: lo cubre el [plan de pruebas](08-plan-de-pruebas-spark.md), no el validador estático.
