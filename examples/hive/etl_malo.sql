-- ETL que incumple las reglas de la guía Hive.
-- Sirve para ver un reporte RECHAZADO.

CREATE TABLE resultado.ventas_txt (
    id_venta STRING,
    producto STRING,
    monto DOUBLE
)
STORED AS TEXTFILE;

CREATE TEMPORARY TABLE SalarioPromedio AS
SELECT DISTINCT producto, monto
FROM datos.ventas;

INSERT INTO resultado.ventas_txt
SELECT *
FROM datos.ventas v
JOIN clientes c ON v.cliente_id = c.id
JOIN zonas z ON c.zona_id = z.id
CROSS JOIN tipos t;
