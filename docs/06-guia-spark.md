# Guía paso a paso para un ETL en Spark

Misma secuencia que la [guía HiveQL](03-guia-hiveql.md), en PySpark. El detalle de cada práctica está en [Buenas prácticas Spark](05-buenas-practicas-spark.md). La plantilla completa está en [Template Spark](07-template-spark.md).

En volúmenes grandes, el costo está en el shuffle y en el sesgo. Por eso cada paso filtra, parte el join y deja un conteo para comparar.

## Paso 1. Abrir la sesión

Fijar al inicio AQE, las particiones de shuffle y, si el clúster lo permite, la asignación dinámica de ejecutores. Memoria y núcleos son del tamaño del clúster, no un valor universal: el ejemplo de abajo es un punto de partida para joins pesados, no una configuración obligatoria.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("etl_transacciones")
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "4")
    .getOrCreate()
)
```

Comprobar `spark.version` y que la sesión levantó sin error. Si el gestor de recursos usa colas, la cola se elige al lanzar la aplicación, fuera de la lógica de transformación.

## Paso 2. Extraer

Leer Parquet, ORC o la tabla del metastore. El filtro de partición y el `select` van en la misma cadena, para que el predicado baje a la lectura.

```python
df_avisos = (
    spark.read.parquet("warehouse/usuarios/avisos_origen")
    .filter("anio = year(current_date()) and estado = '1'")
    .select("identificador", "tipo_documento", "numero_documento", "nombre", "anio")
)
print("Registros extraídos:", df_avisos.count())
```

Formatos columnares permiten leer solo las columnas pedidas. Un `SELECT` equivalente dentro de `spark.sql()` sigue las reglas Hive de proyección y de partición.

## Paso 3. Normalizar los DataFrames de entrada

Una variable `in_` por fuente: filtros que faltaban, casts, texto normalizado y, solo si la clave no debe repetirse, una deduplicación. `persist()` si ese DataFrame se usa en más de un paso.

```python
from pyspark.sql.functions import col, upper

df_in_pagos = (
    spark.read.parquet("warehouse/raw/pagos")
    .filter("anio = year(current_date()) and mes = 7")
    .withColumn("moneda_normalizada", upper(col("moneda")))
    .dropDuplicates(["id_transaccion"])
)
df_in_pagos.persist()
```

Comparar el conteo antes y después del filtro. En un volumen muy grande, una muestra (`sample`) alcanza para inspeccionar filas; el conteo exacto se reserva para la cuadratura.

## Paso 4. Definir la maestra

El DataFrame maestro es el universo de filas. Reparticionar por la clave de join si esa clave está sesgada, y persistir porque los pasos siguientes lo releen. Contar e imprimir el esquema. Ese conteo es el que los joins posteriores tienen que conservar.

```python
df_maestra = (
    df_avisos.select("identificador", "nombre", "anio")
    .repartition(200, "identificador")
    .persist()
)
print("Registros en maestra:", df_maestra.count())
df_maestra.printSchema()
```

`explain()` en este punto muestra si el filtro de la extracción quedó abajo del todo.

## Paso 5. Unir por etapas

Como máximo dos joins por etapa. En la práctica de los templates se hace uno por paso, que es más fácil de contar. `broadcast` solo del lado chico. Comparar el conteo con el de la maestra: si creció, la clave del lado derecho no era única.

```python
from pyspark.sql.functions import broadcast

df_temp1 = df_maestra.join(broadcast(df_in_pagos), "identificador", "left")
df_temp2 = df_temp1.join(df_transacciones, "identificador", "left").persist()
print("Registros después de joins:", df_temp2.count())
```

El segundo join es otra etapa. No encadenar un tercero sobre la misma expresión.

## Paso 6. Agregar y derivar columnas

`groupBy` y `agg` después del filtro. Las columnas calculadas a partir de varias fuentes van aquí, cuando ya están en el mismo DataFrame. Evitar agrupar por una clave de cardinalidad enorme si el resultado que se necesita es un resumen por la clave de la maestra.

```python
from pyspark.sql.functions import count, sum as sum_

df_agg = df_temp2.groupBy("identificador").agg(
    sum_("monto").alias("total_gastado"),
    count("*").alias("num_transacciones"),
)
df_agg = df_agg.withColumn(
    "promedio_gasto", col("total_gastado") / col("num_transacciones")
)
```

## Paso 7. Agregados intermedios, si hace falta

Cuando el detalle es de uno a muchos, el resumen se calcula aparte y se une ya colapsado. Persistir el agregado si entra en más de un destino.

```python
df_agg_pagos = (
    df_in_pagos.groupBy("id_transaccion")
    .agg(sum_("monto").alias("total_pagado"))
    .persist()
)
df_maestra_agg = df_maestra.join(df_agg_pagos, "id_transaccion", "left")
```

El nombre del resultado lleva prefijo `agg_` o `tmp_`.

## Paso 8. Escribir

`coalesce` (o `repartition`) antes del write, formato Parquet u ORC, `partitionBy` y modo acorde a la recarga (`overwrite` de la partición que se procesa, no de la tabla entera, salvo que esa sea la corrida). Leer de vuelta y comparar conteo y esquema.

```python
df_final = df_maestra_agg.coalesce(50)
(
    df_final.write.partitionBy("anio", "mes")
    .mode("overwrite")
    .parquet("warehouse/consolidacion/avisos_final")
)
df_check = spark.read.parquet("warehouse/consolidacion/avisos_final")
print("Registros cargados:", df_check.count())
```

Para una tabla del metastore, el equivalente es `format("parquet").mode("overwrite").partitionBy(...).saveAsTable(...)`, y después `ANALYZE TABLE` sobre esa tabla.

## Paso 9. Cerrar

- [ ] La sesión tiene AQE y `spark.sql.shuffle.partitions` explícitos.
- [ ] Cada lectura filtra la partición y proyecta columnas.
- [ ] Ninguna etapa encadena más de dos joins, y el conteo de la maestra se mantuvo.
- [ ] El uno a muchos pasó por un agregado.
- [ ] La escritura usa Parquet u ORC, `partitionBy` y `coalesce` o `repartition`.
- [ ] CPU, memoria y shuffle se miraron en la interfaz de Spark.

Al terminar, `spark.stop()` libera la sesión. En producción el script lo dispara el orquestador (por ejemplo Airflow); el orquestador no cambia estas reglas.

Un script que el validador aprueba sin hallazgos está en `examples/spark/etl_ok.py`.
