-- =====================================================================
-- Consultas de evidencia (informe, revisión de datos y demo en vivo)
-- Ejecutar desde la carpeta proyecto_etl/:
--   docker compose exec -T postgres psql -U etl -d dw_gestion < sql/consultas/consultas_evidencia.sql
-- O copiar cada bloque en DBeaver / pgAdmin (host localhost, puerto 5433).
-- =====================================================================

-- 1) Registros por tabla (Información de carga)
SELECT 'raw.mp_licitaciones_dia' AS tabla, count(*) AS registros FROM raw.mp_licitaciones_dia
UNION ALL SELECT 'raw.mp_licitacion_detalle', count(*) FROM raw.mp_licitacion_detalle
UNION ALL SELECT 'raw.bcch_series', count(*) FROM raw.bcch_series
UNION ALL SELECT 'staging.mp_licitacion', count(*) FROM staging.mp_licitacion
UNION ALL SELECT 'staging.bcch_observacion', count(*) FROM staging.bcch_observacion
UNION ALL SELECT 'dw.dim_tiempo', count(*) FROM dw.dim_tiempo
UNION ALL SELECT 'dw.dim_organismo', count(*) FROM dw.dim_organismo
UNION ALL SELECT 'dw.fact_licitacion', count(*) FROM dw.fact_licitacion
UNION ALL SELECT 'dw.fact_indicador_economico', count(*) FROM dw.fact_indicador_economico
UNION ALL SELECT 'mart.kpi_mensual', count(*) FROM mart.kpi_mensual
UNION ALL SELECT 'mart.top_organismos_mes', count(*) FROM mart.top_organismos_mes;

-- 2) Prueba de idempotencia: anota este resultado, haz "Clear" de un run exitoso,
--    espera que termine y vuelve a ejecutar. staging y dw NO deben cambiar
--    (raw sí crece: guarda cada extracción con su fecha y hora, a propósito).
SELECT (SELECT count(*) FROM staging.mp_licitacion)          AS stg_licitaciones,
       (SELECT count(*) FROM dw.fact_licitacion)             AS dw_licitaciones,
       (SELECT count(*) FROM staging.bcch_observacion)       AS stg_bcch,
       (SELECT count(*) FROM dw.fact_indicador_economico)    AS dw_bcch,
       (SELECT count(*) FROM raw.mp_licitaciones_dia)        AS raw_listados_mp;

-- 3) Auditoría: últimas 15 tareas ejecutadas
SELECT audit_id, dag_id, task_id, estado, intento, filas_leidas, filas_cargadas, filas_rechazadas,
       pct_nulos, duracion_seg,
       to_char(inicio AT TIME ZONE 'America/Santiago', 'DD-MM HH24:MI:SS') AS inicio_chile,
       left(mensaje, 80) AS mensaje
FROM audit.etl_audit ORDER BY audit_id DESC LIMIT 15;

-- 4) Fallas y reintentos registrados por los callbacks (demo de falla)
SELECT to_char(inicio AT TIME ZONE 'America/Santiago', 'DD-MM HH24:MI:SS') AS hora,
       dag_id, task_id, estado, intento, left(mensaje, 100) AS mensaje
FROM audit.etl_audit WHERE estado <> 'SUCCESS' ORDER BY audit_id DESC LIMIT 10;

-- 5) Cantidad de registros de auditoría por DAG y estado
SELECT dag_id, estado, count(*) AS registros FROM audit.etl_audit GROUP BY 1, 2 ORDER BY 1, 2;

-- 6) Tiempo de procesamiento por DAG (suma de duraciones de sus tareas en cada run)
SELECT dag_id, count(*) AS runs, round(avg(dur), 2) AS prom_seg, round(max(dur), 2) AS max_seg
FROM (SELECT dag_id, run_id, sum(duracion_seg) AS dur
      FROM audit.etl_audit WHERE estado = 'SUCCESS' GROUP BY 1, 2) x
GROUP BY 1 ORDER BY 1;

-- 7) Calidad: % de nulos en campos clave (licitaciones con detalle)
SELECT count(*) AS con_detalle,
       round(100.0 * avg((monto_estimado IS NULL)::int), 2)          AS pct_monto_nulo,
       round(100.0 * avg((region IS NULL)::int), 2)                  AS pct_region_nula,
       round(100.0 * avg((dias_publicacion_cierre IS NULL)::int), 2) AS pct_plazo_nulo
FROM staging.mp_licitacion WHERE tiene_detalle;

-- 8) Cobertura del detalle (limitada por la cuota diaria del ticket)
SELECT count(*) AS licitaciones,
       count(*) FILTER (WHERE tiene_detalle) AS con_detalle,
       round(100.0 * avg((NOT tiene_detalle)::int), 1) AS pct_sin_detalle
FROM staging.mp_licitacion;

-- 9) Cruce con el Banco Central: montos convertidos por moneda
SELECT COALESCE(moneda_codigo, '(sin detalle)') AS moneda, count(*) AS licitaciones,
       count(monto_estimado_original) AS con_monto,
       count(monto_estimado_clp)      AS convertidas_clp,
       count(monto_estimado_uf)       AS convertidas_uf
FROM dw.fact_licitacion GROUP BY 1 ORDER BY 2 DESC;

-- 10) Series del Banco Central: rango cargado y nulos (feriados / sin publicación)
SELECT serie_id, min(fecha) AS desde, max(fecha) AS hasta, count(*) AS observaciones,
       count(*) FILTER (WHERE es_nulo) AS nulas
FROM staging.bcch_observacion GROUP BY 1 ORDER BY 1;

-- 11) Ejemplo del cruce: las 5 licitaciones de mayor monto en UF
SELECT codigo_externo, moneda_codigo, monto_estimado_original, monto_estimado_clp, monto_estimado_uf
FROM dw.fact_licitacion WHERE monto_estimado_uf IS NOT NULL
ORDER BY monto_estimado_uf DESC LIMIT 5;

-- 12) KPIs del último mes calculado
SELECT anio_mes, kpi_codigo, kpi_nombre, round(valor, 2) AS valor, meta,
       CASE cumple_meta WHEN true THEN 'Sí' WHEN false THEN 'No' ELSE 'N/A' END AS cumple
FROM mart.kpi_mensual
WHERE anio_mes = (SELECT max(anio_mes) FROM mart.kpi_mensual)
ORDER BY kpi_codigo;

-- 13) Frescura: última carga de cada fuente
SELECT 'bcch' AS fuente, max(cargado_en) AS ultima_carga FROM dw.fact_indicador_economico
UNION ALL
SELECT 'mercadopublico', max(cargado_en) FROM dw.fact_licitacion;
