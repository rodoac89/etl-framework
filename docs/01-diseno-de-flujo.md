# Diseño de flujo

HiveQL es el motor principal de este diseño. Las mismas reglas valen para Spark SQL o DataFrames sobre las tablas registradas en el metastore: los dos motores pueden leer el mismo esquema. El objetivo es un flujo eficiente, sin duplicar filas en los joins y apoyado en las particiones.

El proceso se parte en cuatro etapas. Las extracciones de fuentes distintas, y las agregaciones que no dependen entre sí, pueden correr en paralelo.

## 1. Extracción

Se lee cada tabla fuente y se deja una versión reducida, llamada tabla `IN`.

- Filtrar por la partición (fecha u otra clave) para que el motor pode directorios y no lea la tabla entera. Si hace falta una semana de una tabla particionada por día, pedir esos siete días, no el histórico.
- Proyectar solo las columnas que las etapas siguientes usan. Una fuente de 50 columnas de la que el ETL necesita 5 se extrae con esas 5.
- Aplicar en el mismo `WHERE` los filtros de negocio (estado, tipo de transacción) para no arrastrar filas que se van a descartar.
- Materializar el resultado como `CREATE TEMPORARY TABLE ... AS SELECT` o como CTE, según el volumen. Ver [tablas temporales y CTE](02-buenas-practicas-hiveql.md#tablas-temporales-y-cte).

En tablas transaccionales, planificar la lectura para que no coincida con una escritura que bloquee la fuente. Este diseño asume lecturas sin transacción.

## 2. Filtros y depuración

Sobre cada tabla `IN`, antes de cualquier unión:

- Confirmar que solo quedan las columnas necesarias.
- Calcular aquí los campos que salen de la propia fuente: normalizar texto, formatear una fecha, armar un identificador.
- Quitar duplicados de la clave de join solo cuando la fuente no debería repetirla. Preferir `GROUP BY` o `ROW_NUMBER()` antes que `DISTINCT` sobre muchas columnas.
- Anotar el conteo de cada `IN`. Ese número es la referencia para saber si el filtro hizo lo esperado y para dimensionar los joins.

## 3. Tabla maestra

La maestra define el universo de filas del resultado.

- Elegir como base la fuente que representa la entidad del proceso, en general la de más filas que se quieren conservar. Si el proceso debe quedarse con todas las transacciones enriquecidas, la maestra nace de esa tabla `IN`, no de la dimensión chica.
- Usar `INNER JOIN` solo cuando el negocio exige intersección (por ejemplo, quedarse únicamente con las transacciones que tienen cliente). Si hay que conservar las filas sin correspondencia, la maestra es la tabla principal y el resto entra con `LEFT JOIN`.
- La clave de la maestra tiene que identificar cada fila. Si la maestra sale de un join, el lado que se agrega no puede repetir esa clave: una relación de uno a muchos multiplica filas y altera conteos y totales.
- Materializarla (`TMP_` / `Tmp1_`) si se reutiliza o si hay que depurar el conteo. En una sola sentencia puede ser una CTE.

Ejemplo de criterio: un millón de transacciones y doscientos mil clientes. Un `INNER JOIN` se queda solo con las transacciones que encuentran cliente. Un `LEFT JOIN` conserva el millón y deja nulos donde no hubo cliente. Esa segunda tabla, con una fila por transacción, es la base de los pasos siguientes.

## 4. Agregaciones e incorporación

El resto de las fuentes se suma de a una. "Agregación" aquí significa dos cosas: añadir columnas a la maestra, y resumir (`SUM`, `COUNT`, `MAX`) el detalle antes de unirlo.

- Cada fuente nueva entra con `LEFT JOIN` sobre la clave. Después de un left join contra una clave única, el número de filas debe ser el mismo que el de la maestra.
- Si la fuente nueva tiene varias filas por clave, no se une directo. O se deja una sola fila por clave (la más reciente, o la consolidada), o se resume en una tabla `AGG_` y esa tabla, ya de uno a uno, se une a la maestra.
- Varias fuentes de detalle pueden armar una cadena `Agg1_`, `Agg2_`, `Agg3_`. Cada eslabón sigue la misma regla: una base, claves únicas en lo que se añade, y el conteo de la base sin cambios. Esas cadenas son un subproceso y pueden prepararse aparte.
- Los cálculos que mezclan columnas de distintas fuentes van al final, en el `SELECT` de cierre o en un último temporal, según lo larga que sea la regla.
- Al terminar, contrastar conteos, totales y nulos contra lo que se esperaba de la maestra. Si un monto se duplicó, la causa suele ser una clave repetida en un `AGG_`.

| Paso | Operación | Resultado | Filas |
| --- | --- | --- | --- |
| 1 | `INNER` o `LEFT JOIN` de las tablas base. Define la maestra. | `TMP_MAESTRA` | Las de la tabla que se decidió conservar. |
| 2 | `LEFT JOIN` con la siguiente fuente, de clave única. | `TMP_MAESTRA2` | Igual que el paso anterior. |
| 3 | Agregar el detalle (`AGG_`) y hacer `LEFT JOIN` de ese resumen. | `TMP_MAESTRA3` | Igual. |
| 4 | `LEFT JOIN` de las fuentes que ya son uno a uno. | Tabla final | Igual, salvo un filtro explícito. |

## Cierre

La tabla final se publica particionada, en ORC o Parquet, para HiveQL, Spark u otra herramienta de consulta. Puede salir de un único `CREATE TABLE ... AS SELECT` con subconsultas, o de varios pasos con resultados intermedios. Los pasos intermedios facilitan reprocesar desde el punto que falló.

Conviene tener estadísticas al día y el optimizador por costo activo, para que el plan elija el orden de los joins. El formato columnar acelera las lecturas posteriores.

Spark aplica la misma lógica: `filter` y `select` tempranos, joins de a uno o de a dos, `dropDuplicates` o una agregación previa donde habría uno a muchos, y escritura particionada. El detalle está en la [guía Spark](06-guia-spark.md).

Mantener las cuatro etapas separadas no es solo orden de lectura. Cada una se puede probar sola, y un fallo en la carga final no obliga a releer las fuentes.
