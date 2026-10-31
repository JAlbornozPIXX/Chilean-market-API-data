-- =====================================================================
-- Indicadores de gestión - Compras públicas (Mercado Público + BCCh)
-- Recalculo completo e idempotente (DELETE + INSERT en una transacción).
-- Pregunta de gestión: ¿Qué tan eficiente, competitivo y transparente es el
-- proceso de licitaciones del Estado y cuánto se licita en términos reales?
-- =====================================================================
DELETE FROM mart.kpi_mensual;
DELETE FROM mart.top_organismos_mes;

WITH lic AS (
    SELECT t.anio_mes, f.*, e.es_final, e.es_exitoso
    FROM dw.fact_licitacion f
    JOIN dw.dim_tiempo t USING (tiempo_key)
    JOIN dw.dim_estado_licitacion e USING (codigo_estado)
),
k AS (
    -- KPI1 Tasa de adjudicación (sobre licitaciones con estado final)
    SELECT anio_mes, 'KPI1' cod, 'Tasa de adjudicación (%)' nom,
           100.0 * count(*) FILTER (WHERE es_exitoso) / NULLIF(count(*) FILTER (WHERE es_final),0) val,
           80.0 meta, 'mayor' sentido
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI2 Tasa de licitaciones desiertas
    SELECT anio_mes, 'KPI2', 'Tasa de licitaciones desiertas (%)',
           100.0 * count(*) FILTER (WHERE codigo_estado = 7) / NULLIF(count(*) FILTER (WHERE es_final),0),
           15.0, 'menor'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI3 Plazo promedio de publicación (días entre publicación y cierre)
    SELECT anio_mes, 'KPI3', 'Plazo promedio publicación-cierre (días)',
           avg(dias_publicacion_cierre), 10.0, 'mayor'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI4 Monto licitado en términos reales (miles de UF) - cruce con BCCh
    SELECT anio_mes, 'KPI4', 'Monto estimado licitado (miles de UF)',
           sum(monto_estimado_uf) / 1000.0, NULL, 'info'
    FROM lic GROUP BY anio_mes
    UNION ALL
    -- KPI5 Transparencia: % de licitaciones con monto estimado publicado
    SELECT anio_mes, 'KPI5', 'Licitaciones con monto estimado visible (%)',
           100.0 * count(monto_estimado_original) / NULLIF(count(*) FILTER (WHERE cantidad_items IS NOT NULL),0),
           50.0, 'mayor'
    FROM lic GROUP BY anio_mes
)
INSERT INTO mart.kpi_mensual (anio_mes, kpi_codigo, kpi_nombre, valor, meta, cumple_meta)
SELECT anio_mes, cod, nom, round(val::numeric, 4), meta,
       CASE WHEN val IS NULL OR meta IS NULL THEN NULL
            WHEN sentido = 'mayor' THEN val >= meta ELSE val <= meta END
FROM k;

-- KPI6 Concentración: participación de los 10 organismos con mayor monto (CLP)
WITH m AS (
    SELECT t.anio_mes, o.nombre_organismo, sum(f.monto_estimado_clp) monto
    FROM dw.fact_licitacion f
    JOIN dw.dim_tiempo t USING (tiempo_key)
    JOIN dw.dim_organismo o USING (organismo_key)
    WHERE f.monto_estimado_clp IS NOT NULL
    GROUP BY 1, 2
), r AS (
    SELECT *, row_number() OVER (PARTITION BY anio_mes ORDER BY monto DESC) rk,
           100.0 * monto / sum(monto) OVER (PARTITION BY anio_mes) pct
    FROM m
)
INSERT INTO mart.top_organismos_mes
SELECT anio_mes, rk, nombre_organismo, monto, round(pct::numeric, 4) FROM r WHERE rk <= 10;

INSERT INTO mart.kpi_mensual (anio_mes, kpi_codigo, kpi_nombre, valor, meta, cumple_meta)
SELECT anio_mes, 'KPI6', 'Concentración top-10 organismos en monto (%)',
       round(sum(participacion_pct), 4), 60.0, sum(participacion_pct) <= 60.0
FROM mart.top_organismos_mes GROUP BY anio_mes;

-- Vista de apoyo para el informe: KPI4 con referencia del dólar promedio del mes
CREATE OR REPLACE VIEW mart.v_contexto_economico_mes AS
SELECT t.anio_mes, s.nombre serie, round(avg(f.valor), 2) valor_promedio,
       round(stddev(f.variacion_diaria_pct), 4) volatilidad_diaria_pct
FROM dw.fact_indicador_economico f
JOIN dw.dim_tiempo t USING (tiempo_key)
JOIN dw.dim_serie s USING (serie_key)
GROUP BY 1, 2;
