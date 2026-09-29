-- ETL de referencia. Debe quedar APROBADO, sin advertencias.
-- Sigue el flujo IN_ -> TMP_ -> AGG_ -> tabla final de las guías.

SET hive.merge.mapfiles=true;
SET hive.merge.smallfiles.avgsize=128000000;

CREATE TEMPORARY TABLE IN_TRANSACCIONES AS
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
  AND mes = 6;

CREATE TEMPORARY TABLE IN_CLIENTES AS
SELECT
    id_transaccion,
    nombre_cliente,
    segmento
FROM datos.clientes
WHERE anio = 2025;

CREATE TEMPORARY TABLE TMP_MAESTRA AS
SELECT
    a.id_transaccion,
    a.fecha_transaccion,
    a.monto,
    a.anio,
    a.mes,
    b.nombre_cliente,
    b.segmento
FROM IN_TRANSACCIONES a
LEFT JOIN IN_CLIENTES b
    ON a.id_transaccion = b.id_transaccion;

CREATE TEMPORARY TABLE AGG_PAGOS AS
SELECT
    id_transaccion,
    COUNT(id_pago) AS total_pagos,
    SUM(monto_pago) AS suma_pagos
FROM datos.pagos
WHERE anio = 2025
  AND mes = 6
GROUP BY id_transaccion;

CREATE TEMPORARY TABLE TMP_MAESTRA_FINAL AS
SELECT
    a.id_transaccion,
    a.fecha_transaccion,
    a.monto,
    a.nombre_cliente,
    a.segmento,
    a.anio,
    a.mes,
    agg.total_pagos,
    agg.suma_pagos
FROM TMP_MAESTRA a
LEFT JOIN AGG_PAGOS agg
    ON a.id_transaccion = agg.id_transaccion;

CREATE TABLE IF NOT EXISTS resultado.transacciones_finales (
    id_transaccion STRING,
    fecha_transaccion DATE,
    monto DOUBLE,
    nombre_cliente STRING,
    segmento STRING,
    total_pagos BIGINT,
    suma_pagos DOUBLE
)
PARTITIONED BY (anio INT, mes INT)
STORED AS PARQUET;

INSERT OVERWRITE TABLE resultado.transacciones_finales
PARTITION (anio, mes)
SELECT
    id_transaccion,
    fecha_transaccion,
    monto,
    nombre_cliente,
    segmento,
    total_pagos,
    suma_pagos,
    anio,
    mes
FROM TMP_MAESTRA_FINAL;
