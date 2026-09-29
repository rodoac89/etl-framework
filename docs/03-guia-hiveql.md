# Guía paso a paso para un ETL en HiveQL

Secuencia para codificar el flujo de [diseño](01-diseno-de-flujo.md) con las [prácticas HiveQL](02-buenas-practicas-hiveql.md). Cada paso reduce datos, deja un punto donde contar filas o hace explícita la cardinalidad. Los ejemplos usan tablas temporales y, debajo, la misma idea con CTE. La plantilla completa está en [Template HiveQL](04-template-hiveql.md).

## Paso 1. Extraer

Leer la fuente, filtrar por partición y por la condición de negocio, y proyectar solo las columnas que siguen.

Sirve para procesar menos, soltar la tabla fuente y depurar sobre un subconjunto.

```sql
CREATE TEMPORARY TABLE TEMP_AVISOS AS
SELECT
    identificador,
    tipo_documento,
    numero_documento,
    nombre,
    estado,
    anio,
    mes,
    dia
FROM usuarios.avisos_origen
WHERE estado = '1'
  AND anio = year(current_date);
```

Con CTE, el filtro es igual, pero el nombre solo existe en esa sentencia:

```sql
WITH AVISOS_FILTRADOS AS (
    SELECT
        identificador,
        tipo_documento,
        numero_documento,
        nombre,
        estado,
        anio,
        mes,
        dia
    FROM usuarios.avisos_origen
    WHERE estado = '1'
      AND anio = year(current_date)
)
SELECT identificador, tipo_documento, numero_documento, nombre, estado, anio, mes
FROM AVISOS_FILTRADOS;
```

## Paso 2. Normalizar las tablas IN

Una tabla `IN_` por fuente. Aquí se limpian formatos y se crean columnas derivadas. El temporal se reutiliza; no se vuelve a escanear el origen.

```sql
CREATE TEMPORARY TABLE IN_TABLA3 AS
SELECT
    id_transaccion,
    metodo_pago,
    upper(moneda) AS moneda_normalizada
FROM raw.pagos
WHERE anio = 2024
  AND mes = 7;
```

## Paso 3. Definir la maestra

Crear `TMP_` (o `Tmp1_`) con la base del proceso y contar las filas. Ese conteo es la referencia: si más adelante cambia sin un filtro explícito, un join duplicó o perdió registros.

```sql
CREATE TEMPORARY TABLE TMP1_TABLAMAESTRA (
    identificador INT,
    tipo_documento STRING,
    numero_documento STRING,
    nombre STRING,
    estado STRING
)
PARTITIONED BY (anio INT, mes INT, dia INT)
STORED AS PARQUET;

INSERT OVERWRITE TABLE TMP1_TABLAMAESTRA
PARTITION (anio, mes, dia)
SELECT identificador, tipo_documento, numero_documento, nombre, estado, anio, mes, dia
FROM TEMP_AVISOS;

SELECT count(*) FROM TMP1_TABLAMAESTRA;
```

La maestra puede ser una sola fuente o el primer join (nunca más de uno en ese `SELECT`). La clave tiene que ser única.

## Paso 4. Unir por etapas

Como máximo dos tablas por consulta, es decir un `JOIN`. Filtrar cada lado antes de unir. Guardar el resultado para el paso siguiente.

```sql
CREATE TEMPORARY TABLE TEMP_DELEGADOS AS
SELECT
    s.identificador,
    s.identificador_principal,
    s.tipo_documento,
    s.numero_documento,
    s.descripcion,
    s.estado
FROM usuarios.avisos_delegados_origen s
JOIN TMP1_TABLAMAESTRA p
  ON s.identificador_principal = p.identificador
WHERE s.anio = year(current_date);
```

## Paso 5. Enriquecer sin cambiar la cardinalidad

Sumar columnas de otra tabla `IN_` con `LEFT JOIN`. La tabla que se añade debe tener una fila por clave. El resultado es la maestra siguiente (`TMP2_`).

```sql
CREATE TEMPORARY TABLE TMP2_TABLAMAESTRA AS
SELECT
    m.identificador,
    m.tipo_documento,
    m.numero_documento,
    m.nombre,
    m.estado,
    m.anio,
    m.mes,
    m.dia,
    p.metodo_pago,
    p.moneda_normalizada
FROM TMP1_TABLAMAESTRA m
LEFT JOIN IN_TABLA3 p
  ON m.id_transaccion = p.id_transaccion;
```

## Paso 6. Armar agregados, si la relación es de uno a muchos

Resumir el detalle por clave (`SUM`, `COUNT`, `MAX`) en `AGG_` / `Agg1_` y unir ese resumen a la última maestra. El conteo no debe subir.

```sql
CREATE TEMPORARY TABLE AGG1_PAGOS AS
SELECT
    id_transaccion,
    sum(monto) AS total_pagado
FROM raw.pagos
WHERE anio = 2024
  AND mes = 7
GROUP BY id_transaccion;

CREATE TEMPORARY TABLE TMP3_TABLAMAESTRA AS
SELECT
    m.identificador,
    m.tipo_documento,
    m.numero_documento,
    m.nombre,
    m.estado,
    m.anio,
    m.mes,
    m.dia,
    m.metodo_pago,
    m.moneda_normalizada,
    a.total_pagado
FROM TMP2_TABLAMAESTRA m
LEFT JOIN AGG1_PAGOS a
  ON m.id_transaccion = a.id_transaccion;
```

## Paso 7. Cargar la tabla final

Crear la tabla permanente en Parquet (u ORC), particionada, e insertar declarando la partición. Si el clúster usa colas del gestor de recursos, la cola se asigna solo cuando hace falta aislar el trabajo; no es parte de la lógica del ETL.

Las columnas de `PARTITIONED BY` no se repiten en el cuerpo de la tabla. El `SELECT` del `INSERT` debe entregar las columnas de datos y, si la partición es dinámica, las de partición al final.

```sql
SET hive.merge.mapfiles=true;

CREATE TABLE IF NOT EXISTS consolidacion.avisos_delegados (
    identificador INT,
    tipo_documento STRING,
    numero_documento STRING,
    nombre STRING,
    estado STRING,
    metodo_pago STRING,
    moneda_normalizada STRING,
    total_pagado DOUBLE
)
PARTITIONED BY (anio INT, mes INT, dia INT)
STORED AS PARQUET;

INSERT OVERWRITE TABLE consolidacion.avisos_delegados
PARTITION (anio, mes, dia)
SELECT
    identificador,
    tipo_documento,
    numero_documento,
    nombre,
    estado,
    metodo_pago,
    moneda_normalizada,
    total_pagado,
    anio,
    mes,
    dia
FROM TMP3_TABLAMAESTRA;
```

## Paso 8. Calcular estadísticas

Solo sobre la tabla final.

```sql
ANALYZE TABLE consolidacion.avisos_delegados COMPUTE STATISTICS;
```

Para columnas concretas: `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS col1, col2`. Eso afina joins, agregaciones y la poda. No aplicarlo a temporales ni a CTE.

## Paso 9. Cerrar contra la guía

- [ ] Cada fuente se extrajo con filtro de partición y sin `SELECT *`.
- [ ] Hay como máximo un join por `SELECT`.
- [ ] La maestra está definida y se contaron sus filas.
- [ ] Toda relación de uno a muchos pasó por un `AGG_` antes del join.
- [ ] La tabla final está en Parquet u ORC, el `INSERT` declara `PARTITION` y se calcularon estadísticas.
- [ ] El script activa el merge de archivos pequeños antes de insertar.

Un ejemplo que cumple esta lista y el validador aprueba sin hallazgos está en `examples/hive/etl_ok.sql`.
