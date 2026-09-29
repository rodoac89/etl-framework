# Template HiveQL

Tres formas de escribir el mismo flujo: tablas temporales, una sola sentencia con CTE, o una mezcla. Las reglas no cambian entre las tres.

- Filtro de partición y columnas explícitas en la extracción. Sin `SELECT *` ni `alias.*`.
- Un `JOIN` por `SELECT` o por CTE.
- Prefijos `IN_`, `TMP_` y `AGG_`.
- La relación de uno a muchos se agrega antes de unirse a la maestra.
- Tabla final en Parquet, particionada.
- `ANALYZE` solo de la tabla final. `DISTINCT` no es el mecanismo por defecto para controlar duplicados: si hay que colapsar una clave, se hace con `GROUP BY` o `ROW_NUMBER()` en el paso que corresponde.

Sustituir nombres de tablas, columnas y filtros. Un script de referencia ya revisado por el validador está en `examples/hive/etl_ok.sql`.

## Tablas temporales

Cada paso queda en disco de sesión y se puede contar por separado.

```sql
SET hive.merge.mapfiles=true;
SET hive.merge.smallfiles.avgsize=128000000;

-- 1. Extracción
CREATE TEMPORARY TABLE IN_TABLA1 AS
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    estado,
    anio,
    mes,
    dia
FROM datos.transacciones
WHERE estado = 'ACTIVO'
  AND anio = 2025
  AND mes = 6;

-- 2. Maestra: un solo join, clave única en el lado que se añade
CREATE TEMPORARY TABLE TMP_MAESTRA AS
SELECT
    a.id_transaccion,
    a.fecha_transaccion,
    a.monto,
    a.anio,
    a.mes,
    b.nombre_cliente,
    b.segmento
FROM IN_TABLA1 a
LEFT JOIN clientes b
  ON a.id_transaccion = b.id_transaccion;

-- 3. Una fuente más por paso
CREATE TEMPORARY TABLE TMP_MAESTRA2 AS
SELECT
    a.id_transaccion,
    a.fecha_transaccion,
    a.monto,
    a.anio,
    a.mes,
    a.nombre_cliente,
    a.segmento,
    c.tipo_producto
FROM TMP_MAESTRA a
LEFT JOIN productos c
  ON a.id_transaccion = c.id_transaccion;

CREATE TEMPORARY TABLE TMP_MAESTRA3 AS
SELECT
    a.id_transaccion,
    a.fecha_transaccion,
    a.monto,
    a.anio,
    a.mes,
    a.nombre_cliente,
    a.segmento,
    a.tipo_producto,
    d.nombre_region
FROM TMP_MAESTRA2 a
LEFT JOIN regiones d
  ON a.id_transaccion = d.id_transaccion;

-- 4. Detalle de uno a muchos, resumido antes del join
CREATE TEMPORARY TABLE AGG_TABLA45 AS
SELECT
    t4.id_transaccion,
    count(t5.id_pago) AS total_pagos,
    sum(t5.monto_pago) AS suma_pagos
FROM tabla4 t4
JOIN tabla5 t5
  ON t4.id_transaccion = t5.id_transaccion
WHERE t4.anio = 2025
  AND t4.mes = 6
GROUP BY t4.id_transaccion;

CREATE TEMPORARY TABLE TMP_MAESTRA4 AS
SELECT
    a.id_transaccion,
    a.fecha_transaccion,
    a.monto,
    a.anio,
    a.mes,
    a.nombre_cliente,
    a.segmento,
    a.tipo_producto,
    a.nombre_region,
    agg.total_pagos,
    agg.suma_pagos
FROM TMP_MAESTRA3 a
LEFT JOIN AGG_TABLA45 agg
  ON a.id_transaccion = agg.id_transaccion;

-- 5. Tabla final
CREATE TABLE IF NOT EXISTS resultado.transacciones_finales (
    id_transaccion STRING,
    fecha_transaccion DATE,
    monto DOUBLE,
    nombre_cliente STRING,
    segmento STRING,
    tipo_producto STRING,
    nombre_region STRING,
    total_pagos INT,
    suma_pagos DOUBLE
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET;

INSERT OVERWRITE TABLE resultado.transacciones_finales
PARTITION (anio = 2025, mes = 6)
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    nombre_cliente,
    segmento,
    tipo_producto,
    nombre_region,
    total_pagos,
    suma_pagos
FROM TMP_MAESTRA4;

ANALYZE TABLE resultado.transacciones_finales COMPUTE STATISTICS;
```

`clientes`, `productos` y `regiones` están en el ejemplo como dimensiones. Si esas tablas están particionadas, hay que filtrarlas en su propia `IN_` antes del join. Si no lo están, el validador advierte la lectura sin poda y se puede dejar así, o subir `HIVE-PARTITION-FILTER` a error cuando el proyecto no tenga ese caso.

## CTE

Una CTE muere con el `;`. Por eso el `CREATE TABLE` va en una sentencia anterior, y el `WITH` abre la sentencia del `INSERT`. No se puede declarar el `WITH`, cerrarlo y después crear la tabla esperando que los nombres sigan existiendo.

```sql
CREATE TABLE IF NOT EXISTS resultado.transacciones_finales (
    id_transaccion STRING,
    fecha_transaccion DATE,
    monto DOUBLE,
    nombre_cliente STRING,
    segmento STRING,
    tipo_producto STRING,
    nombre_region STRING,
    total_pagos INT,
    suma_pagos DOUBLE
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET;

WITH IN_TABLA1 AS (
    SELECT
        id_transaccion,
        fecha_transaccion,
        monto,
        estado,
        anio,
        mes
    FROM datos.transacciones
    WHERE estado = 'ACTIVO'
      AND anio = 2025
      AND mes = 6
),
TMP_MAESTRA AS (
    SELECT
        a.id_transaccion,
        a.fecha_transaccion,
        a.monto,
        a.anio,
        a.mes,
        b.nombre_cliente,
        b.segmento
    FROM IN_TABLA1 a
    LEFT JOIN clientes b
      ON a.id_transaccion = b.id_transaccion
),
TMP_MAESTRA2 AS (
    SELECT
        a.id_transaccion,
        a.fecha_transaccion,
        a.monto,
        a.anio,
        a.mes,
        a.nombre_cliente,
        a.segmento,
        c.tipo_producto
    FROM TMP_MAESTRA a
    LEFT JOIN productos c
      ON a.id_transaccion = c.id_transaccion
),
TMP_MAESTRA3 AS (
    SELECT
        a.id_transaccion,
        a.fecha_transaccion,
        a.monto,
        a.anio,
        a.mes,
        a.nombre_cliente,
        a.segmento,
        a.tipo_producto,
        d.nombre_region
    FROM TMP_MAESTRA2 a
    LEFT JOIN regiones d
      ON a.id_transaccion = d.id_transaccion
),
AGG_TABLA45 AS (
    SELECT
        t4.id_transaccion,
        count(t5.id_pago) AS total_pagos,
        sum(t5.monto_pago) AS suma_pagos
    FROM tabla4 t4
    JOIN tabla5 t5
      ON t4.id_transaccion = t5.id_transaccion
    WHERE t4.anio = 2025
      AND t4.mes = 6
    GROUP BY t4.id_transaccion
),
TMP_MAESTRA4 AS (
    SELECT
        a.id_transaccion,
        a.fecha_transaccion,
        a.monto,
        a.anio,
        a.mes,
        a.nombre_cliente,
        a.segmento,
        a.tipo_producto,
        a.nombre_region,
        agg.total_pagos,
        agg.suma_pagos
    FROM TMP_MAESTRA3 a
    LEFT JOIN AGG_TABLA45 agg
      ON a.id_transaccion = agg.id_transaccion
)
INSERT OVERWRITE TABLE resultado.transacciones_finales
PARTITION (anio = 2025, mes = 6)
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    nombre_cliente,
    segmento,
    tipo_producto,
    nombre_region,
    total_pagos,
    suma_pagos
FROM TMP_MAESTRA4;

ANALYZE TABLE resultado.transacciones_finales COMPUTE STATISTICS;
```

Cada CTE sigue teniendo un solo join. Arrastrar `a.*` esconde columnas y el validador lo advierte (`HIVE-ALIAS-STAR`); la plantilla lista los campos.

## Enfoque mixto

Materializar lo que se reutiliza o lo que agrega mucho dato (`IN_` y `AGG_`). Encadenar con CTE los joins ya reducidos, que el optimizador puede reordenar dentro de una sentencia. Sigue habiendo un join por CTE.

```sql
SET hive.merge.mapfiles=true;

CREATE TEMPORARY TABLE IN_TABLA1 AS
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    anio,
    mes
FROM datos.transacciones
WHERE estado = 'ACTIVO'
  AND anio = 2025
  AND mes = 6;

CREATE TEMPORARY TABLE AGG_TABLA45 AS
SELECT
    t4.id_transaccion,
    count(t5.id_pago) AS total_pagos,
    sum(t5.monto_pago) AS suma_pagos
FROM tabla4 t4
JOIN tabla5 t5
  ON t4.id_transaccion = t5.id_transaccion
WHERE t4.anio = 2025
  AND t4.mes = 6
GROUP BY t4.id_transaccion;

CREATE TEMPORARY TABLE TMP_MAESTRA AS
WITH CTE_BASE AS (
    SELECT
        a.id_transaccion,
        a.fecha_transaccion,
        a.monto,
        a.anio,
        a.mes,
        b.nombre_cliente,
        b.segmento
    FROM IN_TABLA1 a
    LEFT JOIN clientes b
      ON a.id_transaccion = b.id_transaccion
),
CTE_PRODUCTOS AS (
    SELECT
        m.id_transaccion,
        m.fecha_transaccion,
        m.monto,
        m.anio,
        m.mes,
        m.nombre_cliente,
        m.segmento,
        c.tipo_producto
    FROM CTE_BASE m
    LEFT JOIN productos c
      ON m.id_transaccion = c.id_transaccion
),
CTE_REGIONES AS (
    SELECT
        p.id_transaccion,
        p.fecha_transaccion,
        p.monto,
        p.anio,
        p.mes,
        p.nombre_cliente,
        p.segmento,
        p.tipo_producto,
        d.nombre_region
    FROM CTE_PRODUCTOS p
    LEFT JOIN regiones d
      ON p.id_transaccion = d.id_transaccion
)
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    anio,
    mes,
    nombre_cliente,
    segmento,
    tipo_producto,
    nombre_region
FROM CTE_REGIONES;

CREATE TABLE IF NOT EXISTS resultado.transacciones_finales (
    id_transaccion STRING,
    fecha_transaccion DATE,
    monto DOUBLE,
    nombre_cliente STRING,
    segmento STRING,
    tipo_producto STRING,
    nombre_region STRING,
    total_pagos INT,
    suma_pagos DOUBLE
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET;

WITH CTE_FINAL AS (
    SELECT
        a.id_transaccion,
        a.fecha_transaccion,
        a.monto,
        a.nombre_cliente,
        a.segmento,
        a.tipo_producto,
        a.nombre_region,
        agg.total_pagos,
        agg.suma_pagos,
        a.anio,
        a.mes
    FROM TMP_MAESTRA a
    LEFT JOIN AGG_TABLA45 agg
      ON a.id_transaccion = agg.id_transaccion
)
INSERT OVERWRITE TABLE resultado.transacciones_finales
PARTITION (anio, mes)
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    nombre_cliente,
    segmento,
    tipo_producto,
    nombre_region,
    total_pagos,
    suma_pagos,
    anio,
    mes
FROM CTE_FINAL;

ANALYZE TABLE resultado.transacciones_finales COMPUTE STATISTICS;
```

Si el lado chico de un join cabe en memoria, el motor puede convertirlo en map join (`hive.auto.convert.join`). Eso no autoriza a juntar varios joins en el mismo `SELECT`. Para ver el plan de un caso concreto, `EXPLAIN` sobre esa sentencia, no un cambio de la estructura.
