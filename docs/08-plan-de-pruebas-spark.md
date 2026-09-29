# Plan de pruebas para ETL en Spark

Este plan valida que un ETL Spark sea correcto, eficiente y repetible sobre volúmenes grandes. No sustituye al validador estático: ese mira el script antes de ejecutarlo. Aquí se corre el proceso, se cuadran cifras y se mira el costo real. Los tiempos son una orientación para un equipo de dos o tres personas, no un compromiso de calendario.

El mismo esquema sirve para un ETL Hive en lo que es cuadratura, particiones y regresión. Las herramientas de abajo son las de Spark.

## 1. Preparación

Dejar un conjunto de muestra (del orden del 1 % de la corrida real) y el conjunto completo. En el clúster de prueba, activar AQE (`spark.sql.adaptive.enabled=true`) y fijar `spark.sql.shuffle.partitions` al valor que usará el ETL. El lanzamiento puede ser la consola de PySpark o el orquestador (Airflow u otro).

Listo cuando el entorno levanta y los datos cargan sin error. Orientación: uno o dos días.

## 2. Pruebas unitarias

Cada función de extracción, join o agregación se prueba sola, con nulos, duplicados y tamaños distintos. PyTest o unittest, con una sesión compartida.

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("test_etl").getOrCreate()

def test_filtra_salario(spark):
    df = spark.createDataFrame([(1, "Ana", 50000)], ["id", "nombre", "salario"])
    assert df.filter("salario > 40000").count() == 1
```

El criterio de la guía original es cobertura de las funciones del ETL y cero fallos. Orientación: dos o tres días.

## 3. Integración

Correr el ETL de punta a punta con la muestra y, cuando esa pase, con el volumen real.

- Cuadrar conteos de entrada y salida contra la maestra. Un left join de clave única no debe cambiar el número de filas.
- Cuadrar sumas (`SUM(monto)`) y, si aplica, un checksum por clave.
- Comprobar que las particiones escritas son las de la corrida y que no se reescribieron otras.

```python
df = spark.read.parquet("origen")
df_transformed = df.groupBy("region").agg({"monto": "sum"})
df_transformed.write.partitionBy("anio", "mes").mode("overwrite").parquet("destino")
assert spark.read.parquet("destino").count() == expected_count
```

Pasa si el job termina y la salida es la entrada transformada, no un conteo inflado. Orientación: tres o cuatro días.

## 4. Rendimiento

Repetir con volúmenes crecientes, desde una muestra hasta el orden de la corrida de producción. Anotar tiempo, CPU, memoria y bytes de shuffle. Comparar el mismo ETL con y sin `broadcast` en el lado chico, y con y sin el filtro de partición.

La interfaz de Spark es la fuente: stages, tareas lentas y shuffle read/write. El monitor de recursos del clúster complementa CPU y memoria. El criterio de la guía es un tiempo acotado a la ventana de la corrida y un uso de recursos que deje margen (por debajo de un 80 % sostenido como referencia). Orientación: dos o tres días.

## 5. Calidad de datos

Reglas sobre el resultado, no solo sobre el código:

- columnas obligatorias sin nulos;
- rangos (un monto negativo cuenta como fallo si el negocio no lo admite);
- unicidad de la clave de la maestra;
- diferencia entre origen y destino por conteo o por hash de la clave.

```python
assert df.filter("monto < 0").count() == 0
```

Sirven librerías de calidad (Deequ, Great Expectations) o aserciones en el propio job. Criterio: todas las reglas definidas para esa tabla, sin anomalías abiertas. Orientación: uno o dos días.

## 6. Regresión

El conjunto anterior se dispara solo en la integración continua (GitHub Actions, Jenkins u otra) y, si el ETL está orquestado, como una tarea previa a la publicación. Cada cambio de script vuelve a correr las pruebas de la muestra. El validador estático de este repositorio entra en ese mismo punto: `python -m etl_framework check` sobre el script, con `--strict` si las advertencias también deben frenar el pipeline.

Pasa cuando esa corrida automática queda en verde. El armado inicial lleva cerca de un día; después es parte de cada cambio.

## 7. Cierre

Dejar un informe con tiempos, errores, conteos cuadrados y lo que se cambió en configuración (particiones de shuffle, broadcast, coalesce). El informe es el que habilita el paso a producción. Orientación: un día.

## Resumen

| Fase | Éxito | Orientación |
| --- | --- | --- |
| Preparación | Muestra y volumen completo cargados, AQE activo | 1–2 días |
| Unitarias | Funciones del ETL en verde | 2–3 días |
| Integración | Conteos y sumas cuadran | 3–4 días |
| Rendimiento | Cabe en la ventana, con margen de recursos | 2–3 días |
| Calidad | Reglas de nulos, rangos y unicidad | 1–2 días |
| Regresión | La automatización, incluido el validador estático, pasa | 1 día y luego continuo |
| Cierre | Informe y decisión de salida | 1 día |

En total, del orden de diez a quince días la primera vez. Hacen falta un clúster de prueba, la muestra y el equipo que mantiene el ETL.

Riesgos a tratar antes de usar datos reales: anonimizar lo que sea sensible, y tener alerta si el clúster de prueba se cae a mitad de una corrida de volumen. El plan se ajusta al proceso; las fases no se saltan.
