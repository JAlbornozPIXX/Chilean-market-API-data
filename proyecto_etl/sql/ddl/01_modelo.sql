-- =====================================================================
-- inf-522 evaluacion 2 - ddl postgresql
-- fuentes: api banco central de chile (bde sieterestws) y api mercado publico
-- capas: raw -> staging -> dw (modelo dimensional) -> mart (kpis) + audit
-- script idempotente: puede ejecutarse varias veces.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------------
-- raw: respuesta tal cual llega desde la api + fecha/hora de extraccion
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.bcch_series (
    raw_id          BIGSERIAL PRIMARY KEY,
    serie_id        VARCHAR(60)  NOT NULL,
    fecha_desde     DATE         NOT NULL,
    fecha_hasta     DATE         NOT NULL,
    http_status     INTEGER,
    payload         JSONB        NOT NULL,
    run_id          VARCHAR(250),
    extraido_en     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_bcch_serie ON raw.bcch_series (serie_id, fecha_desde);

CREATE TABLE IF NOT EXISTS raw.mp_licitaciones_dia (
    raw_id          BIGSERIAL PRIMARY KEY,
    fecha_consulta  DATE         NOT NULL,
    http_status     INTEGER,
    payload         JSONB        NOT NULL,
    run_id          VARCHAR(250),
    extraido_en     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_mp_dia ON raw.mp_licitaciones_dia (fecha_consulta);

CREATE TABLE IF NOT EXISTS raw.mp_licitacion_detalle (
    raw_id          BIGSERIAL PRIMARY KEY,
    codigo_externo  VARCHAR(40)  NOT NULL,
    fecha_consulta  DATE         NOT NULL,
    http_status     INTEGER,
    payload         JSONB        NOT NULL,
    run_id          VARCHAR(250),
    extraido_en     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_mp_det ON raw.mp_licitacion_detalle (codigo_externo);

-- ---------------------------------------------------------------------
-- staging: datos limpios, tipados y deduplicados
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.bcch_observacion (
    serie_id        VARCHAR(60)   NOT NULL,
    fecha           DATE          NOT NULL,
    valor           NUMERIC(18,4),
    es_nulo         BOOLEAN       NOT NULL DEFAULT FALSE,
    cargado_en      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (serie_id, fecha)
);

CREATE TABLE IF NOT EXISTS staging.mp_licitacion (
    codigo_externo        VARCHAR(40) PRIMARY KEY,
    nombre                TEXT,
    codigo_estado         INTEGER,
    estado                VARCHAR(40),
    tipo                  VARCHAR(10),
    moneda                VARCHAR(5),
    monto_estimado        NUMERIC(20,2),
    codigo_organismo      VARCHAR(20),
    nombre_organismo      TEXT,
    region                VARCHAR(80),
    comuna                VARCHAR(80),
    fecha_publicacion     DATE,
    fecha_cierre          TIMESTAMP,
    fecha_adjudicacion    TIMESTAMP,
    dias_publicacion_cierre INTEGER,
    cantidad_items        INTEGER,
    tiene_detalle         BOOLEAN NOT NULL DEFAULT FALSE,
    fecha_consulta        DATE NOT NULL,
    cargado_en            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- dw: modelo dimensional (constelacion de hechos con dimensiones conformadas)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
    tiempo_key      INTEGER PRIMARY KEY,          -- yyyymmdd
    fecha           DATE        NOT NULL UNIQUE,
    anio            SMALLINT    NOT NULL,
    trimestre       SMALLINT    NOT NULL,
    mes             SMALLINT    NOT NULL,
    nombre_mes      VARCHAR(15) NOT NULL,
    dia             SMALLINT    NOT NULL,
    dia_semana      SMALLINT    NOT NULL,         -- 1=lunes ... 7=domingo
    es_fin_semana   BOOLEAN     NOT NULL,
    anio_mes        CHAR(7)     NOT NULL          -- 'yyyy-mm'
);

CREATE TABLE IF NOT EXISTS dw.dim_serie (
    serie_key       SERIAL PRIMARY KEY,
    serie_id        VARCHAR(60) NOT NULL UNIQUE,
    nombre          VARCHAR(120) NOT NULL,
    unidad          VARCHAR(20)  NOT NULL,
    moneda_codigo   VARCHAR(5),                   -- cruce con moneda de licitacion
    frecuencia      VARCHAR(10)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_organismo (
    organismo_key   SERIAL PRIMARY KEY,
    codigo_organismo VARCHAR(20) NOT NULL UNIQUE,
    nombre_organismo TEXT        NOT NULL,
    region          VARCHAR(80),
    actualizado_en  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_estado_licitacion (
    codigo_estado   INTEGER PRIMARY KEY,
    estado          VARCHAR(40) NOT NULL,
    es_final        BOOLEAN     NOT NULL,
    es_exitoso      BOOLEAN     NOT NULL
);
INSERT INTO dw.dim_estado_licitacion VALUES
  (5,'Publicada',FALSE,FALSE),(6,'Cerrada',FALSE,FALSE),(7,'Desierta',TRUE,FALSE),
  (8,'Adjudicada',TRUE,TRUE),(15,'Revocada',TRUE,FALSE),(16,'Suspendida',FALSE,FALSE),
  (18,'Revocada',TRUE,FALSE),(19,'Suspendida',FALSE,FALSE),
  (-1,'Desconocido',FALSE,FALSE)
ON CONFLICT (codigo_estado) DO NOTHING;

CREATE TABLE IF NOT EXISTS dw.dim_moneda (
    moneda_codigo   VARCHAR(5) PRIMARY KEY,
    descripcion     VARCHAR(40) NOT NULL,
    serie_id_bcch   VARCHAR(60)               -- serie del bcch para convertir a clp
);
INSERT INTO dw.dim_moneda VALUES
  ('CLP','Peso chileno',NULL),
  ('USD','Dólar estadounidense','F073.TCO.PRE.Z.D'),
  ('EUR','Euro','F072.CLP.EUR.N.O.D'),
  ('CLF','Unidad de Fomento','F073.UFF.PRE.Z.D'),
  ('UTM','Unidad Tributaria Mensual','F073.UTR.PRE.Z.M')
ON CONFLICT (moneda_codigo) DO NOTHING;

-- hecho 1: valor diario de indicadores economicos (bcch)
CREATE TABLE IF NOT EXISTS dw.fact_indicador_economico (
    tiempo_key      INTEGER NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
    serie_key       INTEGER NOT NULL REFERENCES dw.dim_serie(serie_key),
    valor           NUMERIC(18,4) NOT NULL,
    variacion_diaria_pct NUMERIC(10,4),
    cargado_en      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tiempo_key, serie_key)
);

-- hecho 2: licitaciones (mercado publico), monto convertido a clp y uf con bcch
CREATE TABLE IF NOT EXISTS dw.fact_licitacion (
    codigo_externo  VARCHAR(40) PRIMARY KEY,
    tiempo_key      INTEGER NOT NULL REFERENCES dw.dim_tiempo(tiempo_key),
    organismo_key   INTEGER REFERENCES dw.dim_organismo(organismo_key),
    codigo_estado   INTEGER NOT NULL REFERENCES dw.dim_estado_licitacion(codigo_estado),
    moneda_codigo   VARCHAR(5) REFERENCES dw.dim_moneda(moneda_codigo),
    tipo            VARCHAR(10),
    monto_estimado_original NUMERIC(20,2),
    monto_estimado_clp      NUMERIC(22,2),
    monto_estimado_uf       NUMERIC(18,4),
    dias_publicacion_cierre INTEGER,
    cantidad_items  INTEGER,
    cargado_en      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_lic_tiempo ON dw.fact_licitacion (tiempo_key);

-- ---------------------------------------------------------------------
-- mart: indicadores de gestion (los recalcula el dag pipeline_kpis_gestion)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.kpi_mensual (
    anio_mes        CHAR(7)      NOT NULL,
    kpi_codigo      VARCHAR(20)  NOT NULL,
    kpi_nombre      VARCHAR(120) NOT NULL,
    valor           NUMERIC(22,4),
    meta            NUMERIC(22,4),
    cumple_meta     BOOLEAN,
    calculado_en    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (anio_mes, kpi_codigo)
);

CREATE TABLE IF NOT EXISTS mart.top_organismos_mes (
    anio_mes        CHAR(7) NOT NULL,
    ranking         SMALLINT NOT NULL,
    nombre_organismo TEXT NOT NULL,
    monto_clp       NUMERIC(22,2),
    participacion_pct NUMERIC(8,4),
    PRIMARY KEY (anio_mes, ranking)
);

-- ---------------------------------------------------------------------
-- audit: registro de cada ejecucion y de fallas
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.etl_audit (
    audit_id        BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(250) NOT NULL,
    task_id         VARCHAR(250) NOT NULL,
    run_id          VARCHAR(250) NOT NULL,
    tabla_destino   VARCHAR(120),
    filas_leidas    INTEGER,
    filas_cargadas  INTEGER,
    filas_rechazadas INTEGER,
    pct_nulos       NUMERIC(7,3),
    inicio          TIMESTAMPTZ NOT NULL,
    fin             TIMESTAMPTZ,
    duracion_seg    NUMERIC(10,2),
    estado          VARCHAR(20) NOT NULL,       -- success / failed / retry
    intento         INTEGER,
    mensaje         TEXT
);
CREATE INDEX IF NOT EXISTS ix_audit_dag ON audit.etl_audit (dag_id, inicio);
-- ---------------------------------------------------------------------
-- diccionario de datos (comment on es idempotente: reemplaza el comentario)
-- ---------------------------------------------------------------------
COMMENT ON SCHEMA raw     IS 'respuestas de las apis tal como llegan (jsonb) + fecha/hora de extraccion. solo se agregan filas.';
COMMENT ON SCHEMA staging IS 'datos limpios, tipados y deduplicados por clave natural.';
COMMENT ON SCHEMA dw      IS 'modelo dimensional: constelacion de 2 hechos con dimension de tiempo conformada.';
COMMENT ON SCHEMA mart    IS 'indicadores de gestion, recalculados por el dag pipeline_kpis_gestion.';
COMMENT ON SCHEMA audit   IS 'bitacora de cada tarea ejecutada (exito, reintento, falla).';
COMMENT ON TABLE dw.fact_licitacion IS 'grano: una fila por licitacion (codigo_externo); se actualiza (upsert) cuando cambia de estado.';
COMMENT ON TABLE dw.fact_indicador_economico IS 'grano: una fila por serie del bcch y dia (tiempo_key, serie_key).';
COMMENT ON TABLE dw.dim_tiempo IS 'dimension de tiempo conformada (la usan ambos hechos). clave inteligente yyyymmdd.';
COMMENT ON TABLE dw.dim_organismo IS 'organismos compradores. clave subrogada organismo_key; clave natural codigo_organismo.';
COMMENT ON TABLE dw.dim_estado_licitacion IS 'estados de mercado publico con banderas es_final / es_exitoso que usan los kpis.';
COMMENT ON TABLE dw.dim_moneda IS 'monedas de las licitaciones y serie del bcch que permite convertirlas a clp (campo de cruce).';
COMMENT ON TABLE dw.dim_serie IS 'series del banco central: dolar, euro, uf y utm.';
COMMENT ON COLUMN dw.fact_licitacion.tiempo_key IS 'fecha de movimiento: primer dia en que la licitacion aparece en el listado diario.';
COMMENT ON COLUMN dw.fact_licitacion.monto_estimado_uf IS 'monto convertido a uf con el valor vigente a la fecha de publicacion (cruce con bcch).';
COMMENT ON TABLE audit.etl_audit IS 'una fila por tarea: filas leidas/cargadas/rechazadas, % nulos, duracion, estado e intento.';

-- ---------------------------------------------------------------------
-- seguridad (opcional): rol de solo lectura para analistas.
-- raw guarda el json completo (incluye datos personales del detalle),
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
