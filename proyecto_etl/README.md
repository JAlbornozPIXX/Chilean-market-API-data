# INF-522 · Evaluación 2 — ETL de compras públicas (Mercado Público + Banco Central)

ETL en Python orquestado con Apache Airflow 2.10 que consume dos APIs públicas chilenas,
carga PostgreSQL 16 por capas (`raw → staging → dw → mart`, más `audit`) y calcula 6 KPIs.

| Fuente | Endpoint | Credencial |
|---|---|---|
| Mercado Público (ChileCompra) | `https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json` | Ticket (se pide con Clave Única en api.mercadopublico.cl) |
| Banco Central de Chile (BDE) | `https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx` | Usuario/clave (registro en si3.bcentral.cl) |

## Estructura
```
dags/
  dag_etl_bcch.py              DAG 1  etl_bcch_indicadores           (lun-vie 10:00)
  dag_etl_mercadopublico.py    DAG 2  etl_mercadopublico_licitaciones (diario 06:00, catchup)
  dag_kpis.py                  DAG 3  kpis_gestion_compras_publicas   (Dataset BCCH | MP)
  etl_lib/common.py            HTTP con reintentos, conexión BD, auditoría, callbacks
  etl_lib/bcch.py              extract / transform / load BCCh
  etl_lib/mercadopublico.py    extract / transform / load Mercado Público
sql/ddl/01_modelo.sql          DDL completo (esquemas, dimensiones, hechos, mart, audit)
sql/kpi/kpis.sql               Cálculo de indicadores (mart)
docs/generar_entregables.py    Genera informe PDF y PPTX desde la BD
docker-compose.yml, .env.example, requirements.txt
```

## Requisitos
- Docker Desktop con al menos 4 GB de RAM (en Windows, con WSL2) y los puertos 8080 y 5433 libres.
- Credenciales propias: ticket de Mercado Público y token **activado** de la API del Banco Central
  (registro en https://si3.bcentral.cl/Siete/es/Siete/API).

## Cómo ejecutar
1. Requisitos: Docker + Docker Compose (ver sección anterior).
2. `cp .env.example .env` y completar claves (`MP_TICKET`, `BCCH_TOKEN`, contraseñas). **`.env` no se entrega.**
   El token del BCCh contiene `$`: escribirlo entre comillas simples (`BCCH_TOKEN='...'`).
3. `docker compose up -d` → el DDL se aplica automáticamente al crear la BD (`sql/ddl`).
   Si la BD ya existía: `psql -h localhost -p 5433 -U etl dw_gestion -f sql/ddl/01_modelo.sql`.
4. Airflow en http://localhost:8080 (usuario/clave de `.env`). Activar (unpause) los 3 DAGs.
5. Opcional (tope de detalles por día para la cuota):
   `docker compose exec airflow-scheduler airflow variables set mp_max_detalle 150`

La Connection `dw_postgres` y las Variables `mp_ticket`, `bcch_token` se crean
desde el entorno (`AIRFLOW_CONN_DW_POSTGRES`, `AIRFLOW_VAR_*`); no hay claves en el código.

## Verificar que todo quedó bien
```bash
docker compose ps -a     # postgres "healthy", airflow-init "Exited (0)", webserver y scheduler "Up"
docker compose exec airflow-scheduler airflow dags list-import-errors   # "No data found" = sin errores
```

## Conectarse a la base con DBeaver o pgAdmin
Host `localhost` · Puerto `5433` · Base `dw_gestion` · Usuario y clave: `POSTGRES_USER` / `POSTGRES_PASSWORD` del `.env`.

## Cifrado de Variables (opcional, recomendado)
Generar una clave Fernet y ponerla en `AIRFLOW_FERNET_KEY` del `.env`:
`docker compose exec airflow-scheduler python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
y luego `docker compose up -d`. No cambiarla después: lo cifrado con una clave no se puede leer con otra.

## Reiniciar desde cero
`docker compose down -v` borra los contenedores **y la base de datos** de este proyecto. Al volver a
ejecutar `docker compose up -d`, el DDL se aplica de nuevo automáticamente.

## Respaldo de la base
```bash
docker compose exec postgres pg_dump -U etl -Fc -f /tmp/dw_gestion.dump dw_gestion
docker compose cp postgres:/tmp/dw_gestion.dump ../respaldo_dw_gestion.dump
```

## Recarga de un rango pasado
- UI: *Trigger DAG w/ config* → `{"fecha_desde": "2026-10-01", "fecha_hasta": "2026-10-13"}`
- CLI: `docker compose exec airflow-scheduler airflow dags backfill -s 2026-10-01 -e 2026-10-13 etl_mercadopublico_licitaciones`

Todas las cargas son idempotentes (`INSERT … ON CONFLICT`), re-ejecutar no duplica.

## Demostración de falla controlada
```bash
docker compose exec airflow-scheduler airflow variables set mp_base_url https://api.mercadopublico.cl/url-incorrecta.json
docker compose exec airflow-scheduler airflow dags trigger etl_mercadopublico_licitaciones
# validar_api falla -> reintentos (grilla en amarillo/rojo) y filas RETRY/FAILED en audit.etl_audit
# el DAG del BCCh y el de KPIs siguen ejecutándose
docker compose exec airflow-scheduler airflow variables delete mp_base_url   # restaura la URL por defecto
# en la UI: Clear sobre la tarea fallida -> el run se recupera
```

## Consultas útiles
```sql
SELECT * FROM audit.etl_audit ORDER BY audit_id DESC LIMIT 20;
SELECT * FROM mart.kpi_mensual ORDER BY anio_mes, kpi_codigo;
SELECT * FROM mart.top_organismos_mes WHERE anio_mes = '2026-10';
```

## Generar informe y presentación
```bash
pip install python-docx python-pptx reportlab matplotlib psycopg2-binary
PGPASSWORD=<clave> python docs/generar_entregables.py   # capturas de Airflow en docs/capturas/
```

En Windows (PowerShell): `$env:PGPASSWORD="<clave>"; python docs\generar_entregables.py`
