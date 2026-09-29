# Buenas prácticas HiveQL

Estándares para integrar varias fuentes en Hive sin releer de más, sin un plan de joins imposible de leer y sin duplicar filas. El diseño de las etapas está en [Diseño de flujo](01-diseno-de-flujo.md). La secuencia de codificación está en la [guía paso a paso](03-guia-hiveql.md).

## Tablas temporales y CTE

Una tabla temporal vive en disco durante la sesión y Hive la descarta al cerrar ese contexto. No hace falta `DROP TABLE` para eso. Sirve para un resultado intermedio que se va a reutilizar.

```sql
CREATE TEMPORARY TABLE TEMP_SALARIO_PROMEDIO AS
SELECT AVG(salario) AS promedio_salario
FROM empleados;

SELECT e.nombre, e.salario
FROM empleados e
JOIN TEMP_SALARIO_PROMEDIO ps
  ON e.salario > ps.promedio_salario;
```

Una CTE (`WITH ... AS`) es un resultado con nombre, válido solo dentro de la sentencia donde se declara. Conviene cuando el intermedio es chico y el motor puede resolverlo sin materializarlo: por ejemplo, filtrar antes de un único join.

```sql
CREATE TEMPORARY TABLE TEMP_EMPLEADOS AS
SELECT id, nombre, salario
FROM empleados
WHERE departamento = 'Ventas';

WITH CTE_AUMENTOS AS (
    SELECT id, nombre, salario * 1.10 AS nuevo_salario
    FROM TEMP_EMPLEADOS
    WHERE salario > 50000
)
SELECT id, nombre, nuevo_salario
FROM CTE_AUMENTOS;
```

Se pueden mezclar en el mismo script. La CTE no sobrevive al punto y coma: el `INSERT` que la usa tiene que estar en la misma sentencia. Un `;` dentro del `WITH` deja el nombre fuera de alcance. El validador marca eso como `HIVE-BROKEN-CTE`.

Elegir así:

| Situación | Usar |
| --- | --- |
| El intermedio se lee varias veces, o es grande y hay que contar filas a mitad de camino | `CREATE TEMPORARY TABLE` |
| El paso es una cadena corta que termina en un solo `INSERT` | CTE |
| Hace falta depurar o reprocesar un tramo | Tabla temporal de ese tramo |

## Reutilizar lo ya calculado

Repetir la misma lectura con otro filtro, o volver a calcular un join contra la maestra, cuesta más que leer el temporal. Reutilizar también deja a todos los pasos mirando el mismo conjunto.

Casos típicos: varias consultas derivadas de una fuente, joins recurrentes contra la maestra, un histórico que junta muchas particiones, filtros parecidos en más de un proceso, y una validación de conteo antes de transformar.

## Particiones

El filtro por la clave de partición (`anio`, `mes`, `dia`, o los nombres equivalentes `year` / `month` / `day` / `ingestion_*`) permite la poda: el motor descarta los directorios que no cumplen el `WHERE` y lee solo esos archivos.

```sql
SELECT v.producto_id, v.nombre_producto, v.dia, v.mes, v.anio
FROM ventas v
WHERE v.anio = 2025
  AND v.mes = 3
  AND v.dia = 25
  AND v.producto_id = 'PD30089';
```

Sin ese predicado, la consulta recorre todas las particiones. Estos son los casos en los que el descuido se paga:

| Caso | Qué pasa | Qué hacer |
| --- | --- | --- |
| Varias particiones (un mes en una tabla diaria) | Se puede terminar leyendo la tabla completa. | Listar el rango de particiones en el `WHERE`. |
| Join entre particionados distintos (día contra mes) | Se mueve más dato del necesario. | Filtrar cada lado a su grano antes de unir. |
| `GROUP BY` o filtro sobre una columna que no es la partición | No hay poda. | Filtrar antes por la partición y agrupar después. |
| Histórico de muchas particiones | Una sola consulta directa es cara. | Extraer por partición y consolidar en un temporal. |
| ETL de volumen alto | La lectura domina el tiempo. | Procesar el recorte de particiones que corresponde a la corrida. |
| Consulta sin filtro de partición | Escaneo completo. | No publicar ese patrón. El validador lo advierte (`HIVE-PARTITION-FILTER`). |

El aviso es una advertencia, no un error, porque una dimensión puede no estar particionada. Si en un proyecto todas las fuentes lo están, se sube la regla a error en el JSON de configuración.

## Modularizar

Un script único que extrae, une y carga es frágil: un fallo al final obliga a releer todo, y dos personas no pueden tocar el mismo bloque sin pisarse. Cada etapa (extracción, limpieza, maestra, agregado, carga) debe poder correr y probarse sola.

```sql
CREATE TEMPORARY TABLE TMP_RESULTADO AS
SELECT a.id AS identificador, a.nombre, b.ventas
FROM TEMP_DATOS a
JOIN ventas b
  ON a.id = b.id_cliente;

INSERT INTO tabla_final
SELECT identificador, nombre, ventas
FROM TMP_RESULTADO;
```

La modularización también permite reutilizar una limpieza en más de un flujo, en lugar de copiar el mismo `SELECT`.

## Un JOIN por SELECT

Encadenar tres o más joins en una sentencia le pide al optimizador que elija entre muchos órdenes. Un orden malo infla el resultado intermedio, mueve datos de más entre las etapas y hace imposible ver qué unión duplicó las filas.

La regla es un máximo de un `JOIN` por `SELECT` o CTE. Si hay que incorporar otra tabla, el resultado filtrado se guarda en un temporal y el siguiente paso une solo esa tabla. Dos tablas en el `FROM` son un join; una lista separada por comas también cuenta como join.

No usar `CROSS JOIN`. Un producto cartesiano no es un enriquecimiento.

## Estadísticas

Después de cargar la tabla final:

```sql
ANALYZE TABLE tabla_final COMPUTE STATISTICS;
ANALYZE TABLE tabla_final COMPUTE STATISTICS FOR COLUMNS id, nombre, ventas;
```

El optimizador por costo usa filas, distintos y tamaños para elegir el plan. Sin estadísticas cae en estimaciones genéricas y puede escanear de más o quedarse sin memoria. No calcular estadísticas sobre tablas temporales ni sobre CTE: solo sobre la tabla permanente, al cerrar el ETL o después de una carga grande.

## Qué no hacer

### Escaneo completo

`SELECT *` sin poda lee todas las filas y todas las columnas. Proyectar la lista y filtrar por partición.

### Joins grandes en una sola sentencia

Cinco joins en el mismo `SELECT`, o un join de dos tablas grandes antes de filtrar, generan un shuffle enorme y pueden terminar en falta de memoria. Modularizar y filtrar cada lado antes.

### DISTINCT sobre un dataset grande

`DISTINCT` de muchas columnas obliga a reordenar el conjunto completo. Para dejar una fila por clave, usar `GROUP BY` o `ROW_NUMBER()`. El validador advierte `HIVE-DISTINCT`; no lo rechaza, porque a veces el negocio pide un distinto acotado.

### Agregación sin filtro

`SUM` o `GROUP BY` sobre la tabla fuente, sin `WHERE`, mueve el detalle entero. Filtrar la partición primero, agregar después (`HIVE-AGG-WITHOUT-FILTER`).

### Archivos pequeños

Cada archivo, aunque pese poco, es una entrada de metadatos. Un `INSERT` que deja miles de archivos chicos vuelve lentas las lecturas siguientes. Antes de insertar, activar la compactación de archivos del motor, por ejemplo:

```sql
SET hive.merge.mapfiles=true;
SET hive.merge.smallfiles.avgsize=128000000;
```

Si el script inserta y no configura ese merge, el validador deja una nota (`HIVE-SMALL-FILES`), no un error.

## Nombres

Las tablas temporales del flujo usan prefijo `IN_`, `TMP_`, `TEMP_` o `AGG_`, con un número opcional antes del guion bajo (`Tmp1_`, `Agg1_`). La tabla final permanente va en Parquet u ORC y, si el proceso es periódico, particionada. El `INSERT` de una tabla creada con `PARTITIONED BY` en el mismo script debe declarar `PARTITION`.
