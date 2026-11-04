# INF-522 - Evaluación 2 - ETL de compras públicas (Mercado Público + Banco Central)

ETL en Python orquestado con **Apache Airflow 3** (3.3.2, CeleryExecutor) que consume dos APIs públicas
chilenas, carga PostgreSQL 16 por capas (`raw -> staging -> dw -> mart`, más `audit`) y calcula 6 KPIs.

| Fuente | Endpoint | Credencial |
|---|---|---|
| Mercado Público (ChileCompra) | `https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json` | Ticket (se pide con Clave Única en https://www.chilecompra.cl/api/) |
| Banco Central de Chile (BDE) | `https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx` | Usuario (correo) y clave de la API (registro en https://si3.bcentral.cl/Siete/es/Siete/API) |

## Estructura
```
dags/
  dag_pipeline_bcch.py            DAG pipeline_bcch_etl            (lun-vie 10:00, ventana de 7 días)
  dag_pipeline_mercadopublico.py  DAG pipeline_mercadopublico_etl  (diario 06:00, catchup desde 01-10-2026)
  dag_pipeline_kpis.py            DAG pipeline_kpis_gestion        (Assets: ASSET_BCCH | ASSET_MP)
  pipeline_bcch.py                Etapas del ETL del Banco Central (Python puro, ejecutable sin Airflow)
  pipeline_mercadopublico.py      Etapas del ETL de Mercado Público (Python puro)
  pipeline_kpis.py                Cálculo de los 6 KPIs en SQL (copiado de sql/kpi/kpis.sql)
  comun_airflow.py                Conexión, credenciales enmascaradas, auditoría, callbacks y Assets
sql/ddl/00_init_meta.sh           Crea la base de metadatos de Airflow (airflow_meta)
sql/ddl/01_modelo.sql             DDL completo (esquemas, dimensiones, hechos, mart, audit,
                                  diccionario de datos con COMMENT ON y rol analista_kpi)
sql/kpi/kpis.sql                  Cálculo de indicadores (mart)
sql/consultas/consultas_evidencia.sql  Consultas de evidencia para el informe (carga, auditoría, KPIs)
docs/figuras/modelo_dimensional.png    Diagrama del modelo dimensional
docker-compose.yml, .env.example, requirements.txt
```

Cada ETL tiene seis tareas: `crear_modelo -> validar_api -> extract -> transform -> calidad -> load`.
`calidad` es una compuerta: si falla, `load` no se ejecuta, no se carga nada y no se publica el Asset.
Al terminar bien, `load` publica su Asset y eso dispara `pipeline_kpis_gestion`
(`crear_modelo -> verificar_frescura -> calcular_kpis`).

## Requisitos
- Docker Desktop con al menos 4 GB de RAM (en Windows, con WSL2) y los puertos 8080 y 5433 libres.
  Es lo único que hay que instalar: Airflow, PostgreSQL, Redis y todas las librerías vienen en las imágenes.
  En Windows: `winget install Docker.DockerDesktop` y `wsl --install --no-distribution`, y reiniciar.
- Credenciales propias: ticket de Mercado Público y usuario/clave **activados** de la API del Banco Central.
- Opcional, solo para ejecutar un `pipeline_*.py` fuera de Airflow o inspeccionar la base: Python 3.11 o superior
  y `pip install -r requirements.txt` (ver "Ejecutar un ETL sin Airflow").

## Cómo ejecutar
1. `cp .env.example .env` y completar claves (`MP_TICKET`, `BCCH_USER`, `BCCH_PASS`, contraseñas). **`.env` no se entrega.**
   Si la clave del BCCh contiene `$`, escribirla entre comillas simples (`BCCH_PASS='...'`).
2. Generar las dos claves de Airflow y ponerlas en `.env` (una sola vez; no cambiarlas después).
   Ambas son 32 bytes aleatorios; no hace falta instalar nada, basta Python:
   ```bash
   python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"   # AIRFLOW_FERNET_KEY
   python -c "import secrets; print(secrets.token_urlsafe(32))"                              # AIRFLOW_JWT_SECRET
   ```
3. `docker compose up -d`. Al crear la base se aplica el DDL (`sql/ddl`) y `airflow-init` migra la base
   de metadatos, crea el usuario admin y registra la Connection `dw_postgres` y las Variables
   `mp_ticket`, `bcch_user` y `bcch_password` (visibles en Admin -> Connections / Variables, cifradas con la Fernet key).
4. Airflow en http://localhost:8080 (usuario/clave de `.env`). Activar (unpause) los 3 DAGs.
5. Opcional (tope de detalles por run para cuidar la cuota de 10.000 consultas diarias del ticket):
   `docker compose exec airflow-scheduler airflow variables set mp_max_detalle 150`

Variables opcionales: `mp_base_url`, `bcch_base_url` (URL de cada API) y `mp_max_detalle` (150 por defecto).
No hay claves en el código: se leen de la Connection y las Variables al ejecutar cada tarea y se
enmascaran (`***`) en los logs.

## Compatibilidad con Airflow 2.x
Los DAGs están escritos para Airflow 3 (el `docker-compose.yml` usa `apache/airflow:3.3.2-python3.11`) y también corren en
**Airflow 2.10** (probado con 2.10.5, Python 3.11, PostgreSQL 16). Todo lo que cambia entre versiones está en
`dags/comun_airflow.py`: los DAGs importan `dag`, `task`, `Param`, `Asset`, etc. desde ese módulo y no desde `airflow`.

| | Airflow 3.x | Airflow 2.10 |
|---|---|---|
| Imports | `airflow.sdk` | `airflow.decorators`, `airflow.models`, `airflow.datasets` |
| Disparo del DAG de KPIs | `Asset` con condición OR (`ASSET_BCCH \| ASSET_MP`) | `Dataset` con la misma condición OR |
| Fecha de un run programado | el cron no tiene intervalo: `logical_date` es la hora programada | `logical_date` es el inicio del intervalo; la hora programada es `data_interval_end` |
| Recarga de un rango | `airflow backfill create ...` | `airflow dags backfill -s INICIO -e FIN <dag_id>` |

En ambas versiones el run programado de Mercado Público procesa **el día anterior a la hora en que corre** y el del Banco
Central una ventana de 7 días hasta esa hora; `momento_programado()` en `comun_airflow.py` unifica las dos semánticas.

Para usar el proyecto en un Docker con Airflow 2.10 (por ejemplo `apache/airflow:2.10.5-python3.11`):
1. Montar `dags/` y `sql/`, y tener una base PostgreSQL con el DDL de `sql/ddl/01_modelo.sql`.
2. Python 3.10 o superior, con `apache-airflow-providers-postgres`, `pandas`, `requests` y `psycopg2-binary`
   (si la imagen no los trae: `pip install pandas requests psycopg2-binary apache-airflow-providers-postgres`).
3. Registrar la Connection y las Variables:
   `airflow connections add dw_postgres --conn-type postgres --conn-host <host> --conn-port 5432 --conn-schema etl_bcc --conn-login <usuario> --conn-password <clave>`
   y `airflow variables set mp_ticket <ticket>` / `airflow variables set bcch_user <correo>` / `airflow variables set bcch_password <clave>`.
4. Activar (unpause) los 3 DAGs. La recarga de un rango en 2.10 es, por ejemplo:
   `airflow dags backfill -s 2026-10-01 -e 2026-10-12 pipeline_mercadopublico_etl` (un run por día; cada run procesa
   el día de su fecha lógica).

## Indicadores de gestión (`sql/kpi/kpis.sql` -> `mart.kpi_mensual`)
Pregunta de gestión: ¿qué tan eficiente, competitivo y transparente es el proceso de licitaciones del
Estado y cuánto se licita en términos reales? Todos se calculan **por mes** (`anio_mes` de la fecha de
movimiento) y se recalculan solos cuando cualquiera de los dos DAGs ETL publica su Asset.

| KPI | Objetivo / decisión que apoya | Fórmula | Fuente | Meta |
|---|---|---|---|---|
| KPI1 Tasa de adjudicación (%) | Eficiencia: cuántos procesos terminan en compra | adjudicadas / licitaciones con estado final x 100 | Mercado Público | >= 80 % |
| KPI2 Tasa de licitaciones desiertas (%) | Detectar procesos fallidos que obligan a relicitar | desiertas (estado 7) / con estado final x 100 | Mercado Público | <= 15 % |
| KPI3 Plazo promedio publicación-cierre (días) | Competencia: tiempo que tienen los proveedores para ofertar | promedio de `dias_publicacion_cierre` | Mercado Público | >= 10 días |
| KPI4 Monto estimado licitado (miles de UF) | Volumen de compra en términos reales, comparable entre meses | suma de `monto_estimado_uf` / 1.000 | Mercado Público + BCCh (UF) | Umbral de alerta: +-20 % respecto del mes anterior |
| KPI5 Licitaciones con monto estimado visible (%) | Transparencia del monto publicado | con monto informado / con detalle x 100 | Mercado Público | >= 50 % |
| KPI6 Concentración top-10 organismos (%) | Concentración del gasto en pocos compradores | participación de los 10 organismos con mayor monto en CLP | Mercado Público + BCCh (tipo de cambio) | <= 60 % |

`mart.top_organismos_mes` guarda el ranking que usa el KPI6 y `mart.v_contexto_economico_mes` el valor
promedio y la volatilidad de cada serie del BCCh por mes.

## Verificar que todo quedó bien
```bash
docker compose ps -a     # postgres y redis "healthy", airflow-init "Exited (0)", el resto "Up (healthy)"
docker compose exec airflow-scheduler airflow dags list-import-errors   # "No data found" = sin errores
docker compose exec airflow-scheduler airflow dags list                 # los 3 DAGs pipeline_*
```

## Conectarse a la base con DBeaver o pgAdmin
Host `localhost` - Puerto `5433` - Base `etl_bcc` - Usuario y clave: `POSTGRES_USER` / `POSTGRES_PASSWORD` del `.env`.
Para consultar los KPIs sin acceso a `raw` (datos personales) usar el rol de solo lectura `analista_kpi`.

## Reiniciar desde cero
`docker compose down -v` borra los contenedores **y la base de datos** de este proyecto. Al volver a
ejecutar `docker compose up -d`, el DDL se aplica de nuevo automáticamente.

## Respaldo de la base
```bash
docker compose exec postgres pg_dump -U etl -Fc -f /tmp/etl_bcc.dump etl_bcc
docker compose cp postgres:/tmp/etl_bcc.dump ../respaldo_etl_bcc.dump
```

## Recarga de un rango pasado
(Comandos de Airflow 3. Para Airflow 2.10 ver «Compatibilidad con Airflow 2.x».)
- UI: **Trigger** (Run Parameters) -> `{"fecha_desde": "2026-10-01", "fecha_hasta": "2026-10-13"}` (ambos ETL).
- UI: **Trigger -> Backfill**, o por CLI (un run por día del rango):
  `docker compose exec airflow-scheduler airflow backfill create --dag-id pipeline_mercadopublico_etl --from-date 2026-10-01 --to-date 2026-10-13 --reprocess-behavior completed --max-active-runs 1`
  (`--to-date` no se incluye: este ejemplo crea los runs del 01-10 al 12-10; cada run procesa el día anterior a su fecha).
  `--max-active-runs 1` es obligatorio: el backfill no respeta el `max_active_runs` del DAG y con runs en paralelo
  el ticket recibe peticiones simultáneas (HTTP 429) y los runs comparten los archivos intermedios.
- Recarga histórica del BCCh (desde 2024, por las licitaciones publicadas ese año):
  `docker compose exec airflow-scheduler airflow dags trigger pipeline_bcch_etl -c '{"fecha_desde": "2024-01-01"}'`

Todas las cargas son idempotentes (`INSERT ... ON CONFLICT`), re-ejecutar no duplica.

## Demostración de falla controlada
```bash
docker compose exec airflow-scheduler airflow variables set mp_base_url https://api.mercadopublico.cl/url-incorrecta.json
docker compose exec airflow-scheduler airflow dags trigger pipeline_mercadopublico_etl
# validar_api responde HTTP 404 -> up_for_retry y falla tras 3 intentos (~10 min por el backoff)
# filas RETRY/FAILED en audit.etl_audit; el DAG del BCCh y el de KPIs siguen ejecutándose
docker compose exec airflow-scheduler airflow variables delete mp_base_url   # restaura la URL por defecto
# en la UI: Clear sobre la tarea validar_api (con sus tareas siguientes) -> el run se recupera
# (un run manual no tiene fecha lógica, por eso `airflow tasks clear` por CLI no lo encuentra)
```

## Consultas útiles
```sql
SELECT * FROM audit.etl_audit ORDER BY audit_id DESC LIMIT 20;
SELECT * FROM mart.kpi_mensual ORDER BY anio_mes, kpi_codigo;
SELECT * FROM mart.top_organismos_mes WHERE anio_mes = '2026-10';
```
Todas las consultas de evidencia del informe:
`docker compose exec -T postgres psql -U etl -d etl_bcc < sql/consultas/consultas_evidencia.sql`

## Ejecutar un ETL sin Airflow
Cada `pipeline_*.py` tiene un `main()` que ejecuta todas sus etapas en orden (útil para depurar). Necesita el
stack levantado (`docker compose up -d`) porque escribe en la base del puerto 5433.

PowerShell:
```powershell
python -m venv .venv; .venv\Scripts\python -m pip install -r requirements.txt
$env:PYTHONUTF8 = "1"        # evita que la consola de Windows muestre mal las tildes
$env:PGHOST = "localhost"; $env:PGPORT = "5433"; $env:PGDATABASE = "etl_bcc"
$env:PGUSER = "etl"; $env:PGPASSWORD = "<clave>"; $env:MP_TICKET = "<ticket>"
.venv\Scripts\python dags\pipeline_kpis.py                                   # no gasta cuota de ninguna API
.venv\Scripts\python dags\pipeline_mercadopublico.py 2026-10-05 2026-10-06   # desde, hasta (por defecto, ayer)
```

Bash:
```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
PGHOST=localhost PGPORT=5433 PGDATABASE=etl_bcc PGUSER=etl PGPASSWORD=<clave> MP_TICKET=<ticket> \
  .venv/bin/python dags/pipeline_mercadopublico.py 2026-10-05 2026-10-06
```
