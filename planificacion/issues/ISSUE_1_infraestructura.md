# Issue 1 · Infraestructura, configuración segura y librería común

> **Responsable:** Ivan Sanchez (@ivvan-asv) · **Etiqueta:** `infraestructura` · **Hito:** Base lista
> **Rama:** `feature/1-infraestructura` · **Revisa tu PR:** Gustavo (@Rhezm)
> **Depende de:** Issue 2, parte A: `01_modelo.sql` debe estar en `main` antes de tu prueba completa (Paso 7).
> **Bloquea a:** Issues 3, 4 y 5, porque todas usan Docker Compose y `common.py`.
> **Fechas:** PR listo el **sáb 24-10** (en la tarde) · máquina de demo el **mié 28-10** · capturas el **jue 29-10** · .zip final el **sáb 31-10**

Documentos de apoyo: [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md) · [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md) · [Manual de GitHub](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/02_MANUAL_GITHUB.md) · [Guía de presentación y defensa](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md)

---

## 1. Tu misión en una frase

Preparar el **terreno** donde corre todo el proyecto:

- Con **un solo comando** (`docker compose up -d`) se levantan PostgreSQL y Airflow.
- **Ninguna clave queda en el código.**
- Existe la **librería común** (`common.py`) que usan los ETL para conectarse a la base, llamar a las APIs con reintentos y auditar cada tarea.

Al final también te encargas de la **máquina de demo**, las **capturas de evidencia** y el **.zip** que se entrega.

## 2. ¿Por qué es importante? (lo que pide la pauta)

| Criterio de la pauta | Qué depende de ti |
|---|---|
| **Informe y carpeta comprimida** (5 pts): *"carpeta comprimida que se puede reproducir con el README"* | `docker-compose.yml`, `.env.example`, `README.md` y el .zip final |
| **Diseño de DAGs** (5 pts): *"configuración (Connections y Variables) correctos y justificados"* | La Connection `dw_postgres` y las Variables se crean desde `.env`, fuera del código |
| **Tolerancia a fallas** (5 pts): *"reintentos con espera, timeouts, registro de fallas y tabla de auditoría"* | `http_session`, `get_json`, la clase `Auditoria` y los callbacks de `common.py` |
| **Extracción** (5 pts): *"límites de uso y errores HTTP"* | Reintentos ante HTTP 429 y 5xx, respeto de `Retry-After` y timeouts |
| **Restricción**: *"No se deben incluir contraseñas ni claves de API en el código ni en la carpeta entregada"* | `.env` nunca entra a git ni al .zip |

Si esta parte falla, **nadie** puede probar su trabajo. Por eso tu PR va primero, junto con el DDL de la Issue 2.

## 3. Conceptos que debes dominar

La explicación larga de cada uno está en el [Glosario](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/01_GLOSARIO.md).

- **Docker:** empaqueta un programa con todo lo que necesita (sistema, librerías, versión exacta) para que funcione igual en cualquier computador.
- **Imagen y contenedor:** la *imagen* es la plantilla (por ejemplo `postgres:16`). El *contenedor* es una copia de esa plantilla en ejecución.
- **Docker Compose:** levanta varios contenedores que trabajan juntos, descritos en `docker-compose.yml`. Cada contenedor es un *servicio*.
- **Volumen:** disco que sobrevive aunque borres el contenedor. Ahí vive la base de datos (`pgdata`).
- **Bind mount:** carpeta de tu PC "vista" dentro del contenedor (`./dags` → `/opt/airflow/dags`). Editas en VS Code y Airflow ve el cambio.
- **Puerto:** "puerta" de red. `5433:5432` significa que el puerto 5433 de tu PC lleva al 5432 del contenedor.
- **Variables de entorno y `.env`:** valores (como claves) que el programa lee del sistema y no del código. `.env` es el archivo donde los escribes. **Nunca se sube.**
- **Airflow:** orquestador. Decide *qué* tarea corre, *cuándo*, *en qué orden* y *qué hacer si falla*.
  - Sus piezas: **webserver** (la página web), **scheduler** (el cerebro), **executor** (quien ejecuta; usamos `LocalExecutor`) y **base de metadatos** (`airflow_meta`).
- **Connection y Variable de Airflow:**
  - Una *Connection* guarda cómo conectarse a un sistema, como nuestra base `dw_postgres`.
  - Una *Variable* guarda un valor de configuración, como `mp_ticket`.
  - Las creamos con variables de entorno `AIRFLOW_CONN_*` y `AIRFLOW_VAR_*`.
- **Hook:** objeto de Airflow que usa una Connection para conectarse. Nosotros usamos `PostgresHook`.
- **Context manager (`with`):** bloque de Python que garantiza abrir y cerrar recursos. Nuestro `pg_cursor()` hace *commit* si todo sale bien y *rollback* si algo falla.
- **HTTP, códigos de estado y timeout:**
  - Los códigos más importantes: 200 = OK, 404 = no existe, 429 = demasiadas peticiones, 5xx = error del servidor.
  - El *timeout* es el máximo de espera antes de rendirse.
- **Reintento con backoff exponencial:** volver a intentar esperando cada vez más (por ejemplo 2, 4, 8 s) para no saturar a la API.
- **Auditoría y callbacks:**
  - Guardamos una fila por tarea en `audit.etl_audit`.
  - Un *callback* es una función que Airflow llama sola cuando una tarea falla (`on_failure_callback`) o se reintenta (`on_retry_callback`).

## 4. Archivos que te pertenecen

Solo tú los modificas. Si otro integrante necesita un cambio, lo pide en esta issue.

```
proyecto_etl/docker-compose.yml
proyecto_etl/.env.example
proyecto_etl/.gitignore
proyecto_etl/requirements.txt
proyecto_etl/README.md
proyecto_etl/sql/ddl/00_init_meta.sh
proyecto_etl/dags/.airflowignore
proyecto_etl/dags/etl_lib/__init__.py
proyecto_etl/dags/etl_lib/common.py
proyecto_etl/docs/capturas/            (Fase 3: capturas finales y log_falla.txt)
entregables/Codigo_E2_INF522.zip       (Fase 4)
```

**No toques:**

- `01_modelo.sql` (Issue 2)
- `mercadopublico.py` y su DAG (Issue 3)
- `bcch.py` y su DAG (Issue 4)
- `kpis.sql`, `dag_kpis.py` y `generar_entregables.py` (Issue 5)

## 5. Paso a paso

### Paso 0 — Prepara tu computador y tus credenciales (Día 0: hoy, vie 23-10)

Tu laptop será la **máquina de demo**: ahí se hará la carga final y se presentará. Por eso necesitas un entorno estable.

1. Instala **Docker Desktop** desde https://www.docker.com/products/docker-desktop/.
   - **En Windows** instala primero **WSL2 con Ubuntu** (en PowerShell como administrador: `wsl --install`) y **trabaja dentro de Ubuntu**: clona el repositorio en tu carpeta de Linux (`/home/<tu_usuario>`), no en `C:\`. Así evitas problemas de rendimiento, de permisos y de fines de línea.
   - En Docker Desktop → *Settings* → *Resources* asigna **al menos 4 GB de RAM**.
2. Verifica la instalación:
   ```bash
   docker --version
   docker compose version
   docker run --rm hello-world      # debe imprimir "Hello from Docker!"
   ```
3. Instala **Git**, **VS Code** y **GitHub CLI**, clona el repositorio y crea la carpeta `referencia/`. Todo eso está en la Parte B del [Manual de GitHub](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/02_MANUAL_GITHUB.md).
4. **Pide tus propias credenciales**, porque las usará la máquina de demo:
   - **Ticket de Mercado Público:** en https://www.chilecompra.cl/api/, opción "Pide tu ticket" (con Clave Única). Llega por correo. Es **uno por persona** y permite **10.000 consultas diarias**.
   - **Usuario de la API del Banco Central:** regístrate en https://si3.bcentral.cl/Siete/es/Siete/API y luego **inicia sesión para activar las credenciales** (la página dice: *"Para activar las credenciales debe iniciar sesión"*). El proyecto de referencia falló justamente por no tenerlas activas. Los detalles están en el Paso 0 de la Issue 4.

### Paso 1 — Crea tu rama

```bash
cd Chilean-market-API-data
git switch main
git pull
git switch -c feature/1-infraestructura
git push -u origin feature/1-infraestructura
```

### Paso 2 — Copia los archivos base desde la referencia

La carpeta `referencia/` (zip descomprimido, ver el manual) **no se sube nunca**: está en `.gitignore`.

```bash
mkdir -p proyecto_etl/dags/etl_lib proyecto_etl/sql/ddl
R=referencia/proyecto_etl
cp $R/docker-compose.yml $R/.env.example $R/.gitignore $R/requirements.txt $R/README.md proyecto_etl/
cp $R/sql/ddl/00_init_meta.sh proyecto_etl/sql/ddl/
cp $R/dags/.airflowignore proyecto_etl/dags/
cp $R/dags/etl_lib/__init__.py $R/dags/etl_lib/common.py proyecto_etl/dags/etl_lib/
git status        # deben aparecer SOLO estos archivos nuevos
```

### Paso 3 — Entiende `docker-compose.yml` bloque por bloque

Abre `proyecto_etl/docker-compose.yml` al lado de esta tabla. Los números de línea corresponden al archivo de `referencia/`.

| Bloque (líneas) | Qué hace | Por qué está así |
|---|---|---|
| `x-airflow: &airflow` (3–24) | Ancla YAML: configuración común que reutilizan los 3 servicios de Airflow con `<<: *airflow` | No repetir lo mismo 3 veces. Si cambias la imagen, cambias un solo lugar |
| `image: apache/airflow:2.10.5-python3.11` (4) | Imagen oficial de Airflow con **versión fija** | Reproducibilidad: todos usan exactamente la misma versión |
| `env_file: .env` (5) | Carga las variables de `.env` dentro del contenedor | Las claves viven fuera del código |
| `AIRFLOW__CORE__EXECUTOR: LocalExecutor` (7) | Ejecuta las tareas como procesos en la misma máquina | Suficiente para un proyecto local; no requiere Redis ni Celery |
| `AIRFLOW__CORE__LOAD_EXAMPLES: "false"` (8) | No carga los DAGs de ejemplo | La UI muestra solo nuestros 3 DAGs |
| `...DEFAULT_TIMEZONE` y `...DEFAULT_UI_TIMEZONE` (9–10) | Zona horaria `America/Santiago` | Los schedules (06:00, 10:00) se interpretan en hora de Chile, con cambio de horario incluido |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (11) | Base donde Airflow guarda su propia información (runs, estados, Variables): `airflow_meta` | Separa los metadatos del orquestador de los datos analíticos (`dw_gestion`) |
| `AIRFLOW_CONN_DW_POSTGRES` (13) | Crea la **Connection** `dw_postgres` a partir de una URI | Los ETL piden la conexión **por nombre**. El usuario y la clave nunca aparecen en el código |
| `AIRFLOW_VAR_BCCH_USER`, `..._BCCH_PASS`, `..._MP_TICKET` (14–16) | Crean las **Variables** `bcch_user`, `bcch_pass` y `mp_ticket` | Credenciales fuera del código. Las Variables de entorno **no se ven en la UI** y **tienen prioridad** sobre las creadas en la UI |
| `volumes:` `./dags`, `./sql`, `./logs` (17–20) | Bind mounts: carpetas de tu PC dentro del contenedor | Editas un DAG y Airflow lo ve sin reconstruir nada. Los logs quedan en tu PC |
| `user: "${AIRFLOW_UID:-50000}:0"` (21) | Usuario con que corre Airflow en el contenedor | Evita problemas de permisos con `logs/` (sobre todo en Linux) |
| `depends_on: postgres: condition: service_healthy` (22–24) | Airflow espera a que Postgres esté listo | Evita errores de conexión al arrancar |
| `postgres` → `image: postgres:16` (28) | El servidor PostgreSQL 16 | La pauta exige PostgreSQL como destino |
| `ports: ["5433:5432"]` (34) | Puerto 5433 de tu PC → 5432 del contenedor | Usamos 5433 para no chocar con un PostgreSQL instalado en tu PC, que suele usar el 5432 |
| `pgdata:/var/lib/postgresql/data` (36) | Volumen con nombre: los datos sobreviven a `docker compose down` | Solo `docker compose down -v` los borra |
| `00_init_meta.sh` y `01_modelo.sql` → `/docker-entrypoint-initdb.d/` (37–38) | Scripts que Postgres ejecuta **solo la primera vez**, cuando el volumen está vacío | Crean `airflow_meta` y todo el modelo de datos automáticamente |
| `healthcheck` con `pg_isready` (39–42) | Cada 5 s pregunta si Postgres acepta conexiones | Es lo que usa `service_healthy` |
| `airflow-init` (44–48) | Prepara la base de Airflow (`db migrate`), crea el usuario admin y termina | `\|\| true`: no falla si el usuario ya existe. `$${...}`: la variable la expande el shell del contenedor, no Compose |
| `airflow-webserver` (50–56) | La interfaz web en http://localhost:8080 | Para ver DAGs, runs y logs |
| `airflow-scheduler` (58–63) | El "cerebro": decide qué tarea corre y cuándo | Sin scheduler no se ejecuta nada |
| `volumes: pgdata:` (65–66) | Declara el volumen con nombre | Docker lo crea la primera vez |

### Paso 4 — Entiende `.env.example` y crea tu `.env`

| Variable | Para qué sirve |
|---|---|
| `AIRFLOW_UID` | Usuario de Airflow dentro del contenedor. En Linux pon tu `id -u`; en Mac/Windows deja `50000` |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Usuario, clave y base que crea Postgres al arrancar por primera vez (`etl` / tu clave / `dw_gestion`) |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` | Tu usuario para entrar a http://localhost:8080 |
| `BCCH_USER`, `BCCH_PASS` | Correo y clave **activados** de la API del Banco Central → Variables `bcch_user` y `bcch_pass` |
| `MP_TICKET` | Tu ticket de Mercado Público → Variable `mp_ticket` |
| `AIRFLOW_FERNET_KEY` | (Mejora del Paso 5) Clave para cifrar lo que Airflow guarda en su base |

```bash
cd proyecto_etl
cp .env.example .env          # .env es TU copia con tus claves; nunca se sube
git check-ignore -v .env      # debe responder con la regla de .gitignore que lo ignora
```

> Usa claves **sin** `$`, `#`, comillas, `\` ni espacios. Docker Compose puede interpretar esos caracteres.

### Paso 5 — Aplica las 3 mejoras a la configuración

**Mejora 1 — Nombre fijo del proyecto Compose.**

- **Problema:** el proyecto de referencia también vive en una carpeta llamada `proyecto_etl`. Docker Compose nombra contenedores y volúmenes según la carpeta, así que ambos proyectos **compartirían el volumen de la base**. Además, los scripts del DDL no se ejecutarían, porque el volumen ya no estaría vacío.
- **Solución:** agrega `name:` justo después de los dos comentarios iniciales de `docker-compose.yml`:

```yaml
# Postgres (BD destino + metadatos Airflow) y Airflow 2.10 con LocalExecutor.
# Las credenciales se leen desde .env (no incluido en la entrega).
# Nombre fijo del proyecto: evita mezclar contenedores/volúmenes con otra carpeta llamada proyecto_etl
name: inf522_e2
```

**Mejora 2 — Cifrado de Variables (Fernet key).**

- **Problema:** el log del proyecto de referencia muestra `WARNING - empty cryptography key - values will not be stored encrypted`. Ese log se entrega en el informe, así que el profesor puede preguntarlo. Significa que lo que se guarda en `airflow_meta` (por ejemplo `mp_base_url` y `mp_max_detalle`) queda sin cifrar.
- **Solución:**
  1. En `docker-compose.yml`, dentro de `environment:` del ancla `x-airflow`, agrega debajo de `AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE`:
     ```yaml
         # Cifra Variables/Connections guardadas en la BD de Airflow (vacío = sin cifrado)
         AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:-}
     ```
  2. En `.env.example` agrega al final:
     ```bash
     # Clave para cifrar Variables y Connections guardadas en la BD de Airflow.
     # Generarla UNA sola vez (ver README) y no cambiarla después.
     AIRFLOW_FERNET_KEY=
     ```
  3. Para generarla, con el entorno levantado (Paso 7):
     ```bash
     docker compose exec airflow-scheduler python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
     ```
  4. Pega el resultado en tu `.env` (`AIRFLOW_FERNET_KEY=...`) y ejecuta `docker compose up -d`, que recrea los contenedores con la clave. **No la cambies después**: lo cifrado con una clave no se puede leer con otra.

**Mejora 3 — README reproducible.** El criterio "carpeta reproducible con el README" depende de este archivo. En `proyecto_etl/README.md`:

1. **Corrige** el paso 5 de "Cómo ejecutar". El comando debe ejecutarse dentro del contenedor:
   ```bash
   docker compose exec airflow-scheduler airflow variables set mp_max_detalle 150
   ```
2. **Agrega** estas secciones (puedes copiarlas y ajustarlas):

````markdown
## Requisitos
- Docker Desktop con al menos 4 GB de RAM (en Windows, con WSL2) y los puertos 8080 y 5433 libres.
- Credenciales propias: ticket de Mercado Público y usuario **activado** de la API del Banco Central.

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
y luego `docker compose up -d`.

## Reiniciar desde cero
`docker compose down -v` borra los contenedores **y la base de datos** de este proyecto. Al volver a
ejecutar `docker compose up -d`, el DDL se aplica de nuevo automáticamente.

## Respaldo de la base
```bash
docker compose exec postgres pg_dump -U etl -Fc -f /tmp/dw_gestion.dump dw_gestion
docker compose cp postgres:/tmp/dw_gestion.dump ../respaldo_dw_gestion.dump
```

## Generar el informe en Windows (PowerShell)
`$env:PGPASSWORD="<clave>"; python docs\generar_entregables.py`
````

### Paso 6 — Entiende `common.py`, función por función

Es la caja de herramientas que comparten los dos ETL y el DAG de KPIs. Debes poder explicar **cada** función.

| Función (líneas en la referencia) | Qué hace | Detalle que te pueden preguntar |
|---|---|---|
| Docstring (1–7) | Explica que no hay credenciales en el código | La Connection `dw_postgres` y las Variables llegan desde `.env` |
| `PG_CONN_ID`, `HTTP_TIMEOUT` (19–20) | Constantes: nombre de la Connection y timeout `(10, 60)` | 10 s para **conectar** y 60 s para **leer** la respuesta. Sin timeout, una llamada colgada bloquea la tarea hasta su `execution_timeout` |
| `get_pg_conn()` (24–26) | Pide a Airflow una conexión a la BD usando la Connection `dw_postgres` | El `import` va dentro de la función: el módulo se puede importar aunque Airflow no esté cargado |
| `pg_cursor()` (29–40) | *Context manager*: entrega un cursor, hace `commit()` si todo salió bien, `rollback()` si hubo excepción, y siempre cierra | **Atomicidad**: una tarea que falla a la mitad no deja datos a medias |
| `asegurar_dim_tiempo(cur, desde, hasta)` (43–58) | Llena `dw.dim_tiempo` con un día por fila (`generate_series`), con año, trimestre, mes, día de semana y fin de semana | `ON CONFLICT (tiempo_key) DO NOTHING`: es **idempotente**, así que llamarla mil veces no duplica |
| `ApiError` (62–63) | Excepción propia para errores **de negocio** | Ambas APIs pueden responder HTTP **200** con un error dentro del JSON. `requests` no lo detecta; nosotros sí |
| `http_session()` (66–80) | Sesión HTTP con `Retry` de urllib3: hasta 4 reintentos ante **429, 500, 502, 503 y 504**, espera creciente (`backoff_factor=2`), respeta **`Retry-After`** y solo reintenta **GET** | `raise_on_status=False`: al agotar los reintentos devuelve la última respuesta y `get_json` decide qué error lanzar. `User-Agent` identifica al cliente |
| `get_json()` (83–96) | GET con timeout y pausa opcional (`min_interval`) para no exceder cuotas. HTTP ≥ 400 lanza `HTTPError` con un extracto del mensaje; si la respuesta no es JSON, lanza `ApiError` | El mensaje de error incluye la URL **sin** los parámetros, así **el ticket no queda en los logs** |
| `Auditoria` (100–130) | Al crearla anota dag, tarea, run, intento y hora de inicio. La tarea va sumando filas leídas, cargadas y rechazadas y el % de nulos. `cerrar()` inserta la fila en `audit.etl_audit` con la duración | Es la tabla que pide la pauta: *"DAG, run_id, filas leídas y cargadas, % de nulos, duración y estado"* |
| `_registrar_estado()` (133–148) | Inserta una fila FAILED o RETRY con el mensaje del error y escribe `[ALERTA] ...` en el log | Tiene `try/except`: **la auditoría nunca debe romper el callback** ni ocultar el error original |
| `on_failure_callback` / `on_retry_callback` (151–156) | Airflow las llama solo cuando una tarea falla definitivamente o queda en reintento | Se configuran en `default_args` de cada DAG |
| `pct_nulos(filas, campos)` (159–165) | % de celdas nulas en los campos indicados | Alimenta la columna `pct_nulos` de la auditoría |

**Los 3 niveles de reintento del proyecto.** Es una pregunta muy probable en la defensa:

1. **HTTP**, en segundos y dentro de la misma llamada: `Retry` de urllib3 (`http_session`).
2. **Negocio**, cuando la API responde 200 con un código de error: `_consultar` de cada ETL. Por ejemplo, el 10500 de Mercado Público espera 5·i segundos.
3. **Tarea**, en minutos: Airflow vuelve a ejecutar la tarea completa (`retries`, `retry_exponential_backoff`). Si se agota, la tarea queda FAILED, `on_failure_callback` lo registra y los otros DAGs siguen funcionando.

### Paso 7 — Levanta el entorno y pruébalo

Primero actualiza tu rama con `main`, que ya debe tener `01_modelo.sql` de la Issue 2:

```bash
git fetch origin
git merge origin/main
ls proyecto_etl/sql/ddl/      # deben estar 00_init_meta.sh y 01_modelo.sql
```

> Si la Issue 2 se atrasa, puedes copiar temporalmente `referencia/proyecto_etl/sql/ddl/01_modelo.sql` para probar, **sin agregarlo a tu commit**. Si falta el archivo, Docker crea una *carpeta* con ese nombre y Postgres no arranca.

Luego, si en tu PC corre el proyecto de referencia, detenlo, porque usa los mismos puertos:

```bash
cd <ruta>/trabajo_de_gestion/proyecto_etl && docker compose down     # sin -v: no borra sus datos
```

Ahora levanta y verifica:

```bash
cd <ruta>/Chilean-market-API-data/proyecto_etl
docker compose config --quiet && echo "docker-compose.yml válido"
docker compose up -d
docker compose ps -a
```

| Contenedor | Estado esperado |
|---|---|
| `inf522_e2-postgres-1` | `Up ... (healthy)` |
| `inf522_e2-airflow-init-1` | `Exited (0)`: terminó bien y se apagó, como corresponde |
| `inf522_e2-airflow-webserver-1` y `...-airflow-scheduler-1` | `Up` |

```bash
# 1) La base y sus esquemas (raw, staging, dw, mart, audit)
docker compose exec postgres psql -U etl -d dw_gestion -c "\dn"

# 2) La Connection existe (se imprime SIN la clave)
docker compose exec airflow-scheduler python -c "from airflow.hooks.base import BaseHook; c = BaseHook.get_connection('dw_postgres'); print(c.conn_type, c.host, c.port, c.schema)"
#    esperado: postgres postgres 5432 dw_gestion

# 3) Las Variables existen (se imprime True/False, nunca el valor)
docker compose exec airflow-scheduler python -c "from airflow.models import Variable; print({k: bool(Variable.get(k)) for k in ['mp_ticket', 'bcch_user', 'bcch_pass']})"
#    esperado: {'mp_ticket': True, 'bcch_user': True, 'bcch_pass': True}

# 4) common.py: HTTP 200 con error de negocio (credenciales falsas a propósito)
docker compose exec -w /opt/airflow/dags airflow-scheduler python -c "from etl_lib.common import http_session, get_json; print(get_json(http_session(), 'https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx', {'user': 'x', 'pass': 'y', 'function': 'GetSeries', 'timeseries': 'F073.UFF.PRE.Z.D', 'firstdate': '2026-10-01', 'lastdate': '2026-10-02'}))"
#    esperado: (200, {...'Codigo': <distinto de 0>...}). Con credenciales inválidas, el proyecto de
#    referencia recibió Codigo -5 "Invalid username or password". Por eso _consultar revisa "Codigo".

# 5) pg_cursor y la tabla de auditoría (el script se envía por la entrada estándar con -T)
docker compose exec -T -w /opt/airflow/dags airflow-scheduler python - <<'PY'
from etl_lib.common import pg_cursor
with pg_cursor() as cur:
    cur.execute("SELECT count(*) FROM audit.etl_audit")
    print("filas en audit:", cur.fetchone()[0])
PY
#    esperado: filas en audit: 0
```

Finalmente entra a http://localhost:8080 con `AIRFLOW_ADMIN_USER` y `AIRFLOW_ADMIN_PASSWORD`. Por ahora la lista de DAGs está **vacía**, y es correcto: los DAGs llegan con las Issues 3, 4 y 5.

### Paso 8 — Laboratorio: prueba la auditoría y los callbacks con un DAG de juguete

Este DAG **no se sube**: el patrón `proyecto_etl/dags/lab_*.py` está en `.gitignore`.

```bash
cp ../planificacion/recursos/lab_infra.py dags/lab_infra.py
```

1. En la UI aparece `lab_infra` (puede tardar ~30 s). Actívalo con el interruptor y pulsa ▶ *Trigger DAG*.
2. Observa en **Grid**:
   - `tarea_ok` queda verde.
   - `tarea_falla` pasa a naranjo (*up_for_retry*) y, unos 10 s después, a rojo (*failed*).
3. Comprueba lo que quedó registrado:
   ```bash
   docker compose exec postgres psql -U etl -d dw_gestion -c "SELECT task_id, estado, intento, left(mensaje, 60) FROM audit.etl_audit WHERE dag_id = 'lab_infra' ORDER BY audit_id"
   ```
   Esperado: `tarea_ok SUCCESS 1`, `tarea_falla RETRY 1` y `tarea_falla FAILED 2`. En el log de `tarea_falla` (clic en la tarea → *Logs*) busca la línea `[ALERTA]`.
4. Limpia todo:
   ```bash
   rm dags/lab_infra.py
   docker compose exec postgres psql -U etl -d dw_gestion -c "DELETE FROM audit.etl_audit WHERE dag_id = 'lab_infra'"
   ```

Con esto podrás explicar con propiedad *qué pasa cuando una tarea falla*.

### Paso 9 — Commit, push y Pull Request

```bash
cd ..                                   # raíz del repositorio
git status                              # revisa que NO aparezcan .env, logs/ ni referencia/
git add proyecto_etl/docker-compose.yml proyecto_etl/.env.example proyecto_etl/.gitignore \
        proyecto_etl/requirements.txt proyecto_etl/README.md proyecto_etl/sql/ddl/00_init_meta.sh \
        proyecto_etl/dags/.airflowignore proyecto_etl/dags/etl_lib/__init__.py proyecto_etl/dags/etl_lib/common.py
git commit -m "feat(infra): docker compose, configuración por .env y librería común (#1)"
git push
```

Abre el PR en GitHub con `Closes #1` en la descripción y pide revisión a Gustavo (@Rhezm). El paso a paso está en el manual, Parte E.

### Paso 10 — Fase 3 (mié 28-10 y jue 29-10): máquina de demo y capturas

Sigue el **runbook de integración** del [Plan](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/00_PLAN_DEL_PROYECTO.md), sección 9, junto a Jairo, Benja y Sofia. Tú operas el teclado.

1. Toma las capturas en tu laptop, con los datos finales. Pantalla completa del navegador y zoom al 100 %.

   | Archivo (en `proyecto_etl/docs/capturas/`) | Qué debe verse | Dónde |
   |---|---|---|
   | `01_lista_dags.png` | Los 3 DAGs activos, con schedule y última ejecución | Página principal *DAGs* |
   | `02_grid_mercadopublico.png` | Muchos runs verdes por catchup y, si los hubo, reintentos | DAG MP → *Grid* |
   | `03_grid_bcch.png` | Runs exitosos del BCCh, incluida la recarga histórica | DAG BCCh → *Grid* |
   | `04_grid_kpis.png` | Runs del DAG de KPIs disparados por Dataset | DAG KPIs → *Grid* |
   | `05_datasets.png` | Los 2 ETL alimentan al DAG de KPIs | Menú *Datasets* |
   | `06_graph_mercadopublico.png` | Las 5 tareas encadenadas | DAG MP → *Graph* |
   | `07_grid_recuperado.png` | La falla controlada: intentos fallidos y luego verde tras *Clear* | DAG MP → *Grid*, run de la demo |

2. Genera el extracto del log de la falla. Copia el `run_id` exacto desde la UI:
   ```bash
   RUN='manual__2026-10-29T...'
   for f in logs/dag_id=etl_mercadopublico_licitaciones/run_id=$RUN/task_id=validar_api/attempt=*.log; do
     echo "===== $f"; grep -E "ERROR|ALERTA|UP_FOR_RETRY|Marking task" "$f" | cut -c1-300
   done > docs/capturas/log_falla.txt
   grep -n -i -E "ticket=|pass=|user=" docs/capturas/log_falla.txt || echo "OK: sin credenciales en el log"
   ```
3. Respalda la base, por si la laptop falla el día de la presentación:
   ```bash
   docker compose exec postgres pg_dump -U etl -Fc -f /tmp/dw_gestion.dump dw_gestion
   docker compose cp postgres:/tmp/dw_gestion.dump ../respaldo_dw_gestion.dump
   docker compose exec postgres pg_dump -U etl -Fc -f /tmp/airflow_meta.dump airflow_meta
   docker compose cp postgres:/tmp/airflow_meta.dump ../respaldo_airflow_meta.dump
   ```
   Los `.dump` están en `.gitignore`. Guárdalos también en un Drive privado del grupo.
4. Sube las capturas en una rama nueva, `feature/1-evidencia`, con su PR. **Solo tú** subes archivos a `docs/capturas/`.

### Paso 11 — Fase 4 (sáb 31-10): .zip final y prueba de reproducibilidad

1. **Prueba de reproducibilidad**, antes del zip:
   - Otro integrante (sugerido: Gustavo) descomprime el código en una carpeta vacía de **su** PC, crea su `.env` y sigue **solo** el README.
   - Anoten cada duda o error y corrijan el README.
   - Así se asegura el puntaje de *"carpeta que se puede reproducir con el README"*.
2. **Genera el .zip** desde `main` actualizado. `git archive` incluye **solo archivos versionados**, así que `.env` y `logs/` quedan fuera automáticamente:
   ```bash
   git switch main && git pull
   git switch -c feature/1-zip-final
   git archive --format=zip --output=entregables/Codigo_E2_INF522.zip HEAD proyecto_etl
   tar -tf entregables/Codigo_E2_INF522.zip | grep -E "(^|/)\.env$|/logs/|__pycache__" && echo "REVISAR: hay archivos prohibidos" || echo "OK: sin .env, logs ni caché"
   tar -tf entregables/Codigo_E2_INF522.zip | head -40      # revisa que estén DAGs, scripts y DDL
   git add entregables/Codigo_E2_INF522.zip
   git commit -m "chore(entrega): código final en zip (#1)"
   git push -u origin feature/1-zip-final
   ```

## 6. Criterios de aceptación (Definition of Done)

- [ ] `docker compose config --quiet` no muestra errores y el proyecto se llama `inf522_e2`
- [ ] `docker compose up -d` deja Postgres *healthy*, `airflow-init` en *Exited (0)*, y el webserver y el scheduler *Up*
- [ ] Puedo entrar a http://localhost:8080
- [ ] La Connection `dw_postgres` y las Variables `mp_ticket`, `bcch_user` y `bcch_pass` existen; lo comprobé con los comandos del Paso 7, sin mostrar valores
- [ ] La prueba de `get_json` devuelve HTTP 200 con `Codigo` ≠ 0 y entiendo por qué
- [ ] El laboratorio del Paso 8 dejó filas SUCCESS, RETRY y FAILED, y vi la línea `[ALERTA]`
- [ ] El README tiene el comando corregido y las secciones nuevas
- [ ] `.env` no aparece en `git status` ni en el PR
- [ ] Puedo explicar los 3 niveles de reintento y cada fila de la tabla del Paso 6
- [ ] (Fase 3) Las 7 capturas y `log_falla.txt` están subidas, sin credenciales
- [ ] (Fase 4) El .zip está generado con `git archive`, verificado con `tar -tf` y probado por otro integrante

## 7. Tu parte en el informe y en la presentación

- **Informe:**
  - Revisa en el PDF la sección *3. Paso a paso* (arquitectura) y la configuración de la *sección 4* (Connections y Variables).
  - Envía tus correcciones como comentario en la **Issue 5**.
- **Presentación:**
  - **Lámina 3 "Arquitectura"** (~50 s).
  - **Lámina 10 "Lecciones aprendidas"** (~30 s).
  - En la demostración de falla **operas el teclado** mientras Jairo explica.
  - El guion está en la [guía de presentación](https://github.com/JAlbornozPIXX/Chilean-market-API-data/blob/main/planificacion/03_PRESENTACION_Y_DEFENSA.md).

## 8. Preguntas probables en la defensa (con respuesta guía)

1. **¿Por qué usaron Docker y no instalaron Airflow directamente?** Por reproducibilidad: cualquier persona levanta exactamente las mismas versiones (Airflow 2.10.5 y PostgreSQL 16) con un comando. Además, Airflow no corre de forma nativa en Windows y Docker lo aísla del resto del PC.
2. **¿Qué diferencia hay entre una imagen y un contenedor?** La imagen es la plantilla inmutable (`postgres:16`). El contenedor es una instancia en ejecución de esa imagen; se puede borrar y volver a crear.
3. **Si apago Docker o hago `docker compose down`, ¿se pierden los datos?** No. Están en el volumen `pgdata`. Solo `docker compose down -v` borra el volumen.
4. **¿Cómo llegan las credenciales a los DAGs si no están en el código?**
   1. Se escriben en `.env`.
   2. Compose las pasa como variables de entorno: `AIRFLOW_CONN_DW_POSTGRES` y `AIRFLOW_VAR_*`.
   3. Airflow las expone como la Connection `dw_postgres` y las Variables `mp_ticket`, `bcch_user` y `bcch_pass`.
   4. El código solo usa **nombres**: `PostgresHook(postgres_conn_id="dw_postgres")` y `Variable.get("mp_ticket")`.
   5. `.env` está en `.gitignore` y no va en el .zip.
5. **¿Por qué no se ven esas Variables en la UI de Airflow?** Las Variables definidas por variables de entorno no aparecen en la UI y tienen prioridad sobre las creadas desde la UI. Así lo dice la documentación de Airflow 2.10.
6. **¿Por qué hay dos bases de datos?** `airflow_meta` guarda el estado del orquestador (runs, tareas, Variables). `dw_gestion` guarda nuestros datos analíticos. Separarlas permite respaldar o consultar los datos sin tocar Airflow y viceversa.
7. **¿Qué hace `http_session`?**
   - Crea una sesión `requests` con `Retry`: hasta 4 reintentos con espera creciente ante 429 y 5xx.
   - Respeta la cabecera `Retry-After` y solo reintenta GET, que es seguro de repetir.
   - Es la respuesta a *"límites de uso y errores HTTP"* de la pauta.
8. **¿Para qué sirve el timeout `(10, 60)`?** 10 s para establecer la conexión y 60 s para recibir la respuesta. Sin timeout, una API colgada dejaría la tarea esperando indefinidamente.
9. **¿Qué pasa si una consulta falla a la mitad de una carga?** `pg_cursor` hace `rollback`: se deshace todo lo de esa transacción y no quedan datos a medias. Airflow reintenta la tarea y, como las cargas son idempotentes, reintentar no duplica nada.
10. **¿Qué guarda la auditoría y quién la escribe?**
    - Una fila por tarea en `audit.etl_audit`: DAG, tarea, run, intento, filas leídas, cargadas y rechazadas, % de nulos, inicio, fin, duración, estado y mensaje.
    - Las tareas exitosas la escriben con `Auditoria.cerrar()`. Los reintentos y las fallas, los callbacks.
11. **¿Por qué el callback tiene `try/except`?** Si la base estuviera caída, un error al auditar ocultaría el error original de la tarea. Lo registramos en el log y seguimos.
12. **¿Qué es la Fernet key?** Una clave simétrica con que Airflow cifra los valores sensibles que guarda en su base, como Variables y Connections creadas por la UI o la CLI. Sin ella se guardan en texto plano; por eso aparecía el *warning* en el log.
13. **¿Cómo aseguran que el .zip no lleve claves?** Lo generamos con `git archive`, que solo empaqueta archivos versionados, y `.env` nunca se versiona. Además lo revisamos con `tar -tf`.
14. **¿Por qué el puerto 5433?** Para no chocar con un PostgreSQL local que use el 5432. Dentro de Docker, Airflow se conecta a `postgres:5432` por la red interna de Compose.
15. **¿Por qué `.airflowignore` ignora `etl_lib/`?** Airflow revisa todos los `.py` de `dags/` buscando DAGs, y `etl_lib/` solo tiene funciones. Ignorarla ahorra trabajo al procesador de DAGs. Los DAGs igual pueden **importarla**, porque Airflow agrega la carpeta `dags/` al `sys.path`.

## 9. Si te atascas

| Síntoma | Causa probable | Solución |
|---|---|---|
| `Bind for 0.0.0.0:8080 failed: port is already allocated` (o el 5433) | Otro stack, por ejemplo el de referencia, usa el puerto | `docker ps` para ver quién lo usa y `docker compose down` en esa carpeta |
| Postgres no queda *healthy* o `airflow-init` termina con error | Error de configuración o de inicialización | `docker compose logs postgres` y `docker compose logs airflow-init`, y lee el primer error |
| En los logs de Postgres aparece `$'\r': command not found` | `00_init_meta.sh` con fines de línea de Windows (CRLF) | Este repo fuerza LF con `.gitattributes`. Vuelve a clonar o, en VS Code, cambia "CRLF" por "LF" (barra inferior) y guarda |
| `\dn` no muestra `raw`, `staging`, etc. | El volumen ya existía, y los scripts de inicio solo corren con el volumen vacío | `docker compose down -v && docker compose up -d` (⚠ borra la BD de este proyecto), o aplica el DDL a mano: `docker compose exec -T postgres psql -U etl -d dw_gestion < sql/ddl/01_modelo.sql` |
| `PermissionError` en `logs/` (Linux) | El contenedor no puede escribir en la carpeta | `mkdir -p logs` y en `.env`: `AIRFLOW_UID=` seguido del resultado de `id -u` |
| No puedo entrar a la UI | La clave de admin cambió después del primer arranque, y el usuario solo se crea la primera vez | `docker compose exec airflow-webserver airflow users create -r Admin -u admin2 -p <clave> -f Admin -l INF522 -e admin2@example.com` |
| Los contenedores se reinician solos | Poca RAM para Docker | Docker Desktop → *Resources* → 4 GB o más |
| `Cannot connect to the Docker daemon` | Docker Desktop no está abierto | Ábrelo y espera a que diga *Running* |
| `Variable mp_ticket does not exist` | `.env` sin `MP_TICKET`, o contenedores creados antes de editarlo | Completa `.env` y ejecuta `docker compose up -d`, que recrea con las variables nuevas |

Si después de 30 minutos sigues bloqueado, comenta en esta issue con el error exacto (**sin credenciales**) y etiqueta `bloqueante`.
