-- ---------------------------------------------------------------------
-- DICCIONARIO DE DATOS (COMMENT ON es idempotente: reemplaza el comentario)
-- ---------------------------------------------------------------------
COMMENT ON SCHEMA raw     IS 'Respuestas de las APIs tal como llegan (JSONB) + fecha/hora de extracción. Solo se agregan filas.';
COMMENT ON SCHEMA staging IS 'Datos limpios, tipados y deduplicados por clave natural.';
COMMENT ON SCHEMA dw      IS 'Modelo dimensional: constelación de 2 hechos con dimensión de tiempo conformada.';
COMMENT ON SCHEMA mart    IS 'Indicadores de gestión, recalculados por el DAG kpis_gestion_compras_publicas.';
COMMENT ON SCHEMA audit   IS 'Bitácora de cada tarea ejecutada (éxito, reintento, falla).';
COMMENT ON TABLE dw.fact_licitacion IS 'Grano: una fila por licitación (codigo_externo); se actualiza (upsert) cuando cambia de estado.';
COMMENT ON TABLE dw.fact_indicador_economico IS 'Grano: una fila por serie del BCCh y día (tiempo_key, serie_key).';
COMMENT ON TABLE dw.dim_tiempo IS 'Dimensión de tiempo conformada (la usan ambos hechos). Clave inteligente yyyymmdd.';
COMMENT ON TABLE dw.dim_organismo IS 'Organismos compradores. Clave subrogada organismo_key; clave natural codigo_organismo.';
COMMENT ON TABLE dw.dim_estado_licitacion IS 'Estados de Mercado Público con banderas es_final / es_exitoso que usan los KPIs.';
COMMENT ON TABLE dw.dim_moneda IS 'Monedas de las licitaciones y serie del BCCh que permite convertirlas a CLP (campo de cruce).';
COMMENT ON TABLE dw.dim_serie IS 'Series del Banco Central: dólar, euro, UF y UTM.';
COMMENT ON COLUMN dw.fact_licitacion.tiempo_key IS 'Fecha de movimiento: primer día en que la licitación aparece en el listado diario.';
COMMENT ON COLUMN dw.fact_licitacion.monto_estimado_uf IS 'Monto convertido a UF con el valor vigente a la fecha de publicación (cruce con BCCh).';
COMMENT ON TABLE audit.etl_audit IS 'Una fila por tarea: filas leídas/cargadas/rechazadas, % nulos, duración, estado e intento.';

-- ---------------------------------------------------------------------
-- SEGURIDAD (opcional): rol de solo lectura para analistas.
-- raw guarda el JSON completo (incluye datos personales del detalle),
-- por eso el analista solo puede leer dw y mart.
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analista_kpi') THEN
    CREATE ROLE analista_kpi NOLOGIN;
  END IF;
END $$;
GRANT USAGE ON SCHEMA dw, mart TO analista_kpi;
GRANT SELECT ON ALL TABLES IN SCHEMA dw, mart TO analista_kpi;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO analista_kpi;
