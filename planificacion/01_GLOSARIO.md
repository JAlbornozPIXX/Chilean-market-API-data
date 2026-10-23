# Glosario técnico del proyecto (explicado desde cero)

> Cada término tiene **qué es** y **dónde aparece en nuestro proyecto**. Los términos están agrupados por tema y en el orden en que conviene aprenderlos.
> Si en una issue ves una palabra que no entiendes, búscala aquí con Ctrl+F.

## Índice

A. Datos e inteligencia de negocios · B. APIs y HTTP · C. Python · D. PostgreSQL y SQL · E. Docker · F. Apache Airflow · G. Git y GitHub · H. Seguridad

---

## A. Datos e inteligencia de negocios

**Dato abierto.** Información pública que el Estado publica para que cualquiera la use, por lo general mediante APIs.
- *En el proyecto:* las licitaciones de ChileCompra y los indicadores del Banco Central.

**Indicador de gestión / KPI (*Key Performance Indicator*).** Número que mide qué tan bien se cumple un objetivo y apoya una decisión. Se define con nombre, objetivo, fórmula, fuente, periodicidad y meta o umbral.
- *En el proyecto:* 6 KPIs en `mart.kpi_mensual` (Issue 5).

**Meta, umbral y sentido.**
- La *meta* es el valor esperado (KPI1 ≥ 80 %).
- El *umbral* es el valor que gatilla una alerta (KPI4: variación > ±20 %).
- El *sentido* indica si "mayor es mejor" o "menor es mejor" (KPI2: menor).
- *En el proyecto:* columnas `meta` y `cumple_meta` y la lógica `sentido` de `kpis.sql`.

**Pregunta de gestión.** La pregunta de negocio que los KPIs ayudan a responder.
- *En el proyecto:* "¿Qué tan eficiente, competitivo y transparente es el proceso de licitaciones y cuánto se licita en términos reales?".

**ETL (*Extract, Transform, Load*).** Proceso en tres pasos:
1. **Extraer** los datos de la fuente.
2. **Transformarlos**: limpiar, tipar y validar.
3. **Cargarlos** en el destino.

En el **ELT** se carga primero y se transforma dentro de la base.
- *En el proyecto:* las tareas `extraer_raw` → `transformar_staging` → `cargar_dw` de cada DAG.

**OLTP (*Online Transaction Processing*).** Sistema que **opera** un negocio: muchas escrituras pequeñas y rápidas, datos normalizados y el estado actual.
- *En el proyecto:* la plataforma Mercado Público, desde donde leemos con la API.

**OLAP (*Online Analytical Processing*).** Sistema para **analizar**: lecturas grandes, agregaciones, historia y modelos dimensionales.
- *En el proyecto:* nuestro `dw` y nuestro `mart` (Issue 2, Paso 3).

**Data Warehouse (DW).** Base de datos diseñada para el análisis, que **integra varias fuentes** y conserva su historia.
- *En el proyecto:* el esquema `dw`.

**Data Mart.** Subconjunto del DW orientado a un tema o a un tipo de usuario.
- *En el proyecto:* el esquema `mart`, con los KPIs.

**Capas raw / staging / dw / mart / audit.** Etapas por las que pasa el dato:
- **raw:** el dato tal como llega.
- **staging:** limpio y tipado.
- **dw:** el modelo dimensional.
- **mart:** los indicadores.
- **audit:** la bitácora del proceso.
- *En el proyecto:* son los 5 esquemas de `01_modelo.sql`.

**Modelo dimensional.** Forma de organizar un DW en **hechos**, que son lo que se mide, y **dimensiones**, que son el contexto (quién, cuándo, qué).
- *En el proyecto:* 2 hechos y 5 dimensiones.

**Tabla de hechos y medida.** La tabla de hechos registra eventos medibles. Las medidas son sus números: montos, días, valores.
- *En el proyecto:* `fact_licitacion` (montos, plazos) y `fact_indicador_economico` (valor y variación).

**Dimensión y atributo.** La dimensión es la tabla descriptiva por la que se filtra o agrupa. Un atributo es una de sus columnas.
- *En el proyecto:* `dim_tiempo` (año, mes, …), `dim_organismo`, `dim_estado_licitacion`, `dim_moneda` y `dim_serie`.

**Grano (granularidad).** Qué representa **una fila** de un hecho. Es la decisión más importante del modelo.
- *En el proyecto:* en `fact_licitacion`, una licitación; en `fact_indicador_economico`, una serie en un día.

**Esquema estrella / copo de nieve / constelación.**
- *Estrella:* un hecho rodeado de dimensiones.
- *Copo de nieve:* las dimensiones están normalizadas en subtablas.
- *Constelación:* varios hechos comparten dimensiones.
- *En el proyecto:* una **constelación**.

**Dimensión conformada.** Una misma dimensión, con el mismo significado, compartida por varios hechos. Es lo que permite cruzar fuentes.
- *En el proyecto:* `dim_tiempo`.

**Clave natural, subrogada e inteligente.**
- *Natural:* viene del negocio (`codigo_organismo`).
- *Subrogada:* número interno generado por la base (`organismo_key SERIAL`).
- *Inteligente:* se construye a partir del dato (`tiempo_key = 20261015`).
- *En el proyecto:* las tres se usan en `dw`.

**SCD tipo 1 (*Slowly Changing Dimension*).** Cuando cambia un atributo de una dimensión, se **sobrescribe** sin guardar la historia.
- *En el proyecto:* `dim_organismo` (el nombre y la región se actualizan).

**Idempotencia.** Ejecutar algo 1 o 10 veces deja **el mismo resultado**. Es clave para reintentar y recargar sin duplicar.
- *En el proyecto:* `ON CONFLICT`, `IF NOT EXISTS` y `DELETE + INSERT` transaccional (Issue 2, tabla de idempotencia).

**Upsert.** "*Update or insert*": inserta la fila o, si ya existe por su clave, la actualiza.
- *En el proyecto:* `INSERT ... ON CONFLICT ... DO UPDATE`.

**Carga incremental vs. carga completa.** La incremental procesa solo lo nuevo o cambiado; la completa (*full refresh*) recalcula todo.
- *En el proyecto:* los ETL son incrementales por fecha; el mart es una carga completa.

**Backfill / recarga histórica.** Cargar períodos pasados.
- *En el proyecto:* el catchup de MP, *Trigger DAG w/ config* con `fecha_desde` y `fecha_hasta`, y `airflow dags backfill`.

**Linaje del dato (*data lineage*).** El camino que recorre un dato desde su origen hasta el indicador.
- *En el proyecto:* API → raw → staging → dw → mart (Informe, 6.3).

**Calidad de datos.** Qué tan confiables son los datos. Sus dimensiones típicas:
- **nulos:** faltan valores;
- **duplicados:** el mismo registro varias veces;
- **atípicos** (*outliers*): valores anormales;
- **consistencia:** que las fuentes coincidan entre sí;
- **frescura:** qué tan actualizados están.
- *En el proyecto:* `pct_nulos`, `es_nulo`, deduplicación, el filtro de variación > 15 % y `control_calidad`.

**Normalización de texto.** Unificar la forma de escribir: espacios, mayúsculas, tildes.
- *En el proyecto:* `_texto()` y `_clave()` en `mercadopublico.py`.

**Campo calculado.** Columna que no viene en la fuente y se calcula a partir de otras.
- *En el proyecto:* `dias_publicacion_cierre`, `variacion_diaria_pct`, `monto_estimado_clp` y `monto_estimado_uf`.

**Recodificación / mapeo.** Traducir códigos a descripciones, o unificar valores.
- *En el proyecto:* `CodigoEstado` 8 se convierte en "Adjudicada"; los códigos desconocidos, en -1.

**Muestra y sesgo de muestreo.** Una muestra es un subconjunto de los datos. Tiene sesgo si no representa al total, por ejemplo cuando siempre se toman "los primeros".
- *En el proyecto:* la muestra aleatoria reproducible del detalle (Issue 3).

**Términos reales.** Montos expresados de modo que eliminan la inflación, por ejemplo en UF.
- *En el proyecto:* KPI4 en miles de UF.

**Datos personales y minimización.** Un dato personal identifica a una persona (nombre, RUT). La minimización consiste en tratar **solo** los datos necesarios. En Chile los regulan la Ley 19.628 y la Ley 21.719.
- *En el proyecto:* el nombre, RUT y cargo del funcionario no pasan a staging ni a dw.

**Diccionario de datos.** Documentación del significado de cada tabla y columna.
- *En el proyecto:* los `COMMENT ON` del DDL (Issue 2).

**Gobierno de datos.** Reglas sobre quién accede, cómo se protege, cuánto tiempo se guarda y cómo se documenta la información.
- *En el proyecto:* el rol `analista_kpi` y la sección 6.7 del informe.

## B. APIs y HTTP

**API (*Application Programming Interface*).** "Puerta" que un sistema ofrece para que otros programas le pidan datos o servicios.
- *En el proyecto:* la API de Mercado Público y la API BDE del Banco Central.

**API REST.** Estilo de API que usa URLs y los verbos de HTTP (GET para leer) y suele responder en JSON.
- *En el proyecto:* ambas APIs se consultan con **GET**.

**Endpoint.** URL concreta de una API que entrega un recurso.
- *En el proyecto:* `https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json` y `https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx`.

**Parámetros de consulta (*query string*).** Lo que va después de `?` en la URL, separado por `&`.
- *En el proyecto:* `?fecha=20102026&ticket=...` y `?function=GetSeries&timeseries=...`.

**JSON.** Formato de texto para datos estructurados, con objetos `{"clave": valor}` y listas `[ ... ]`.
- *En el proyecto:* las respuestas de ambas APIs, que se guardan como `JSONB`.

**Autenticación.** Demostrar quién eres ante la API.
- *En el proyecto:* el **ticket** personal en MP; el **usuario y clave activados** en el BCCh.

**Código de estado HTTP.** Número con el que el servidor indica el resultado:
- **200** OK;
- **3xx** redirección;
- **404** no existe;
- **429** demasiadas peticiones;
- **5xx** error del servidor.
- *En el proyecto:* `get_json()` falla si el código es ≥ 400; `Retry` reintenta ante 429 y 5xx.

**Error de negocio con HTTP 200.** La API responde "OK" en HTTP, pero el **cuerpo** trae un código de error.
- *En el proyecto:* el BCCh con `Codigo ≠ 0` (por ejemplo, -5 por credenciales inválidas) y MP con `Codigo` 10500 por peticiones simultáneas.

**Timeout.** Tiempo máximo de espera antes de rendirse. Puede ser de conexión o de lectura.
- *En el proyecto:* `HTTP_TIMEOUT = (10, 60)`.

**Reintento (*retry*) y backoff exponencial.** Volver a intentar tras un error, esperando cada vez más (2, 4, 8 s, …) para no saturar al servidor.
- *En el proyecto:* hay 3 niveles: HTTP (urllib3), de negocio (`_consultar`) y de tarea (Airflow).

**Cabecera `Retry-After`.** Indicación del servidor de cuánto esperar antes de reintentar.
- *En el proyecto:* `respect_retry_after_header=True`.

**Límite de uso / cuota / *rate limiting*.** Máximo de consultas permitidas en un período.
- *En el proyecto:* MP permite 10.000 por día y por ticket. Se maneja con pausas, `max_active_runs=1` y `mp_max_detalle`.

**Paginación.** Entregar los datos por partes (páginas) en vez de todos juntos.
- *En el proyecto:* en MP, una página por día más lotes de detalle; en el BCCh, una llamada por serie.

**User-Agent.** Cabecera que identifica al cliente que hace la petición.
- *En el proyecto:* `INF522-ETL/1.0`.

**`requests`, `Session`, `HTTPAdapter`, `urllib3.Retry`.** La librería HTTP de Python más usada y sus piezas:
- `Session` reutiliza la conexión;
- `HTTPAdapter` permite configurarla;
- `Retry` define la política de reintentos.
- *En el proyecto:* `http_session()` en `common.py`.

## C. Python

**Módulo y paquete.** Un módulo es un archivo `.py`. Un paquete es una carpeta de módulos con `__init__.py`.
- *En el proyecto:* el paquete `etl_lib`, con los módulos `common`, `bcch` y `mercadopublico`.

**Función, parámetros y `**context`.** `**context` recibe "todos los argumentos con nombre" como un diccionario. Airflow lo usa para entregar a la tarea `run_id`, `params`, `data_interval_start`, etc.
- *En el proyecto:* `def extraer(**context)`.

**Diccionario, set, tupla y comprensión de listas.**
- `dict`: pares clave → valor.
- `set`: colección sin repetidos.
- `tuple`: secuencia inmutable.
- `[x for x in ...]`: crea una lista en una línea.
- *En el proyecto:* `SERIES` (dict), `vistos` (set) y `pendientes` (lista).

**Excepción (`raise` / `try` / `except`).** Mecanismo para señalar y manejar errores. Se pueden crear excepciones propias.
- *En el proyecto:* `class ApiError(Exception)`.

**Clase y objeto.** La clase es un molde que agrupa datos y funciones; un objeto es una instancia de ese molde.
- *En el proyecto:* la clase `Auditoria` y su método `cerrar()`.

**Context manager (`with`, `@contextmanager`).** Bloque que asegura preparar y liberar un recurso, pase lo que pase.
- *En el proyecto:* `with pg_cursor() as cur:` (hace commit, rollback y close).

**f-string.** Texto con variables incrustadas: `f"Serie {serie}: {n} obs"`.
- *En el proyecto:* los mensajes de log y de error.

**logging.** Módulo para escribir mensajes de log con niveles (INFO, WARNING, ERROR).
- *En el proyecto:* `log.info(...)` y las líneas `[ALERTA]`.

**`datetime` y `date`.** Tipos de fecha y hora. `isoformat()` da el texto `AAAA-MM-DD`; `strptime()` interpreta un texto con un formato dado.
- *En el proyecto:* `datetime.strptime(o["indexDateString"], "%d-%m-%Y")`.

**`unicodedata.normalize("NFKD")`.** Separa las letras de sus tildes para poder quitarlas.
- *En el proyecto:* `_clave()`.

**`json.dumps`.** Convierte un objeto de Python en texto JSON.
- *En el proyecto:* para guardar el `payload` en raw.

**`random.Random(semilla)`.** Generador aleatorio **reproducible**: la misma semilla da siempre la misma secuencia.
- *En el proyecto:* la muestra del detalle (Issue 3).

**Entorno virtual (`venv`) y `pip`.** `venv` crea una instalación aislada de Python para un proyecto; `pip` instala librerías dentro de ella.
- *En el proyecto:* `.venv_docs` para generar el informe (Issue 5).

**`requirements.txt`.** Lista de las dependencias de Python de un proyecto.
- *En el proyecto:* documenta las versiones. La imagen de Airflow ya las trae instaladas.

## D. PostgreSQL y SQL

**PostgreSQL.** Base de datos relacional de código abierto.
- *En el proyecto:* la versión 16, en Docker, puerto **5433** en tu PC.

**Base de datos, esquema y tabla.** Un servidor contiene **bases**; una base contiene **esquemas**, que son como carpetas; los esquemas contienen **tablas**.
- *En el proyecto:* las bases `dw_gestion` y `airflow_meta`; los esquemas `raw`, `staging`, `dw`, `mart` y `audit`.

**DDL y DML.**
- *DDL* (*Data Definition Language*) define estructuras: `CREATE`, `ALTER`, `DROP`.
- *DML* (*Data Manipulation Language*) maneja los datos: `INSERT`, `UPDATE`, `DELETE`, `SELECT`.
- *En el proyecto:* el DDL está en `01_modelo.sql`; el DML, en los ETL y en `kpis.sql`.

**`IF NOT EXISTS`.** Crea el objeto solo si no existe, lo que permite ejecutar un script varias veces.
- *En el proyecto:* todo el DDL.

**Tipos de datos.**
- `INTEGER` y `SMALLINT`: enteros.
- `SERIAL` y `BIGSERIAL`: enteros autoincrementales.
- `NUMERIC(p,s)`: decimal **exacto**.
- `VARCHAR(n)` y `TEXT`: texto.
- `CHAR(n)`: texto de largo fijo.
- `DATE`: fecha.
- `TIMESTAMP` y `TIMESTAMPTZ`: fecha y hora, sin y con zona horaria.
- `BOOLEAN`: verdadero o falso.
- `JSONB`: JSON binario consultable.
- *En el proyecto:* Issue 2, tabla "¿Por qué estos tipos?".

**PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL y DEFAULT.**
- Clave primaria: identifica cada fila.
- Clave foránea: apunta a otra tabla.
- `UNIQUE`: no se permiten repetidos.
- `NOT NULL`: obligatorio.
- `DEFAULT`: valor por omisión.
- *En el proyecto:* por ejemplo `PRIMARY KEY (tiempo_key, serie_key)` o `REFERENCES dw.dim_tiempo`.

**Índice.** Estructura que acelera las búsquedas por una columna.
- *En el proyecto:* `ix_fact_lic_tiempo` e `ix_audit_dag`.

**Transacción, COMMIT, ROLLBACK y atomicidad.** Una transacción es un grupo de operaciones que se aplican **todas** (COMMIT) o **ninguna** (ROLLBACK). Mientras no hay COMMIT, los demás ven la versión anterior.
- *En el proyecto:* `pg_cursor()` y el recálculo del mart.

**`INSERT ... ON CONFLICT DO UPDATE / DO NOTHING` y `EXCLUDED`.** Ante una clave repetida, actualiza o ignora. `EXCLUDED` representa la fila que se intentó insertar.
- *En el proyecto:* todas las cargas a staging y dw.

**`COALESCE`, `NULLIF` y `LEAST`.**
- `COALESCE(a, b)`: el primer valor no nulo.
- `NULLIF(x, 0)`: NULL si x es 0, para evitar dividir por cero.
- `LEAST(a, b)`: el menor de los dos.
- *En el proyecto:* el upsert de staging y las fórmulas de los KPIs.

**`DISTINCT ON (col)`.** Toma **una** fila por cada valor de `col`, la primera según el `ORDER BY` (de PostgreSQL).
- *En el proyecto:* la última extracción de cada serie o licitación.

**JOIN, LEFT JOIN y USING.** Combinan tablas. `LEFT JOIN` conserva las filas de la izquierda aunque no tengan pareja. `USING (col)` une por una columna con el mismo nombre en ambas tablas.
- *En el proyecto:* `cargar_dw()` y `kpis.sql`.

**`JOIN LATERAL`.** Subconsulta que se evalúa **por cada fila** de la tabla anterior y puede usar sus columnas.
- *En el proyecto:* busca el último tipo de cambio menor o igual a la fecha de publicación de cada licitación.

**`GROUP BY`, agregaciones y `FILTER (WHERE ...)`.** Agrupa y calcula `count`, `sum` o `avg` por grupo. `FILTER` cuenta solo las filas que cumplen una condición.
- *En el proyecto:* KPI1 y KPI2.

**CTE (`WITH nombre AS (...)`).** Subconsulta con nombre, para escribir consultas largas por partes.
- *En el proyecto:* `lic`, `k`, `m` y `r` en `kpis.sql`; `base` en `bcch.py`.

**`UNION ALL`.** Apila los resultados de varias consultas sin eliminar repetidos.
- *En el proyecto:* las 5 consultas de KPI1 a KPI5.

**Funciones de ventana (`OVER (PARTITION BY ... ORDER BY ...)`).** Calculan sobre un grupo de filas **sin agruparlas**.
- `lag()`: el valor de la fila anterior.
- `row_number()`: el ranking.
- `sum() OVER`: el total del grupo.
- *En el proyecto:* la variación diaria del BCCh y el top 10 de KPI6.

**`generate_series`.** Genera una secuencia de números o fechas.
- *En el proyecto:* llena `dim_tiempo` con un día por fila.

**Vista (`VIEW`).** Consulta guardada con nombre que se usa como si fuera una tabla.
- *En el proyecto:* `mart.v_contexto_economico_mes`.

**`COMMENT ON`.** Agrega una descripción a un esquema, tabla o columna.
- *En el proyecto:* el diccionario de datos (Issue 2).

**Rol, `GRANT` y privilegios.** Un rol es un usuario o grupo de PostgreSQL. `GRANT` le da permisos.
- *En el proyecto:* `analista_kpi` solo lee `dw` y `mart`.

**psql, `pg_isready`, `pg_dump` y `pg_restore`.**
- `psql`: el cliente de terminal.
- `pg_isready`: indica si el servidor acepta conexiones.
- `pg_dump`: hace un respaldo.
- `pg_restore`: restaura un respaldo.
- *En el proyecto:* los comandos de las issues y del runbook.

**DBeaver y pgAdmin.** Clientes gráficos para explorar bases de datos.
- *En el proyecto:* conexión `localhost:5433`, base `dw_gestion`.

## E. Docker

**Contenedor vs. máquina virtual.** Un contenedor aísla un programa con sus dependencias y comparte el núcleo del sistema: es liviano y arranca en segundos. Una máquina virtual simula un computador completo.
- *En el proyecto:* PostgreSQL y Airflow corren en contenedores.

**Imagen y *tag*.** La imagen es la plantilla inmutable de un contenedor. El tag es su versión.
- *En el proyecto:* `postgres:16` y `apache/airflow:2.10.5-python3.11` (versiones fijas, para la reproducibilidad).

**Docker Desktop y WSL2.** Docker Desktop es la aplicación de Docker para Mac y Windows. WSL2 (*Windows Subsystem for Linux*) permite correr Linux real dentro de Windows.
- *En el proyecto:* en Windows, trabajen dentro de Ubuntu (WSL2).

**Docker Compose, servicio y nombre de proyecto.** Compose levanta varios contenedores descritos en `docker-compose.yml`; cada uno es un **servicio**. El nombre de proyecto agrupa sus contenedores y volúmenes.
- *En el proyecto:* los servicios `postgres`, `airflow-init`, `airflow-webserver` y `airflow-scheduler`; el proyecto se llama `inf522_e2`.

**Volumen con nombre vs. *bind mount*.**
- El *volumen* es un disco gestionado por Docker que sobrevive a los contenedores.
- El *bind mount* es una carpeta de tu PC vista dentro del contenedor.
- *En el proyecto:* el volumen `pgdata` y los bind mounts `./dags`, `./sql` y `./logs`.

**Puerto y mapeo.** `5433:5432` significa "el puerto 5433 de mi PC lleva al 5432 del contenedor".
- *En el proyecto:* 5433 para PostgreSQL y 8080 para Airflow.

**Variables de entorno, `.env` e interpolación `${VAR}`.**
- Las variables de entorno son valores que un programa lee de su entorno de ejecución.
- `.env` es el archivo donde se escriben.
- `${VAR}` en el `docker-compose.yml` se reemplaza por su valor.
- *En el proyecto:* las credenciales, que **nunca se suben**.

**`healthcheck` y `depends_on`.** El healthcheck es una prueba periódica de que un servicio está sano. `depends_on` hace esperar a un servicio hasta que otro esté sano o haya terminado.
- *En el proyecto:* Airflow espera a que Postgres pase su `pg_isready`.

**`/docker-entrypoint-initdb.d`.** Carpeta de la imagen de PostgreSQL cuyos scripts se ejecutan **solo la primera vez**, con el volumen vacío.
- *En el proyecto:* `00_init_meta.sh` y `01_modelo.sql`.

**Comandos de Compose.**
- `up -d`: levanta en segundo plano.
- `ps -a`: muestra el estado de los contenedores.
- `logs -f`: muestra los logs en vivo.
- `exec`: ejecuta un comando dentro de un contenedor.
- `down`: apaga y borra los contenedores.
- `down -v`: además **borra los volúmenes** (la base de datos).
- `config`: valida el archivo.
- `cp`: copia archivos entre el contenedor y tu PC.

## F. Apache Airflow (versión 2.10.5)

**Orquestador.** Programa que decide **qué** tarea se ejecuta, **cuándo**, **en qué orden** y **qué hacer si falla**.
- *En el proyecto:* Apache Airflow.

**DAG (*Directed Acyclic Graph*).** Flujo de tareas con dependencias en una sola dirección, sin ciclos. Se define en un archivo de Python.
- *En el proyecto:* `etl_bcch_indicadores`, `etl_mercadopublico_licitaciones` y `kpis_gestion_compras_publicas`.

**Tarea, operador y `python_callable`.** Una tarea es un paso del DAG. El operador es el "tipo" de tarea. `PythonOperator` ejecuta la función indicada en `python_callable`.
- *En el proyecto:* `PythonOperator(task_id="extraer_raw", python_callable=mp.extraer)`.

**Dependencias (`>>`).** `a >> b` significa que b corre después de que a termine bien.
- *En el proyecto:* `validar >> extraer >> transformar >> cargar >> calidad`.

**Webserver, scheduler, executor y base de metadatos.**
- *Webserver:* la interfaz web.
- *Scheduler:* decide qué corre y cuándo.
- *Executor:* ejecuta las tareas; el `LocalExecutor` usa procesos en la misma máquina.
- *Base de metadatos:* guarda runs, estados y Variables.
- *En el proyecto:* los servicios de Compose y la base `airflow_meta`.

**DAG run, `run_id`, task instance e intento (`try_number`).**
- *DAG run:* una ejecución del DAG.
- *`run_id`:* su identificador, por ejemplo `scheduled__2026-10-08T09:00:00+00:00`.
- *Task instance:* una tarea dentro de un run.
- *Intento:* cuántas veces se ha ejecutado esa tarea.
- *En el proyecto:* columnas `run_id` e `intento` de `audit.etl_audit`.

**Estados de una tarea.**
- `queued`: en cola;
- `running`: ejecutándose;
- `success`: verde;
- `failed`: rojo;
- `up_for_retry`: naranjo, esperando reintento;
- `upstream_failed`: no corrió porque falló una anterior;
- `skipped`: omitida.
- *En el proyecto:* se ven en la vista Grid.

**Pausa y activación.** Un DAG pausado no programa runs. En nuestra configuración los DAGs **nacen pausados** (`dags_are_paused_at_creation = True`).
- *En el proyecto:* se activan con el interruptor o con `airflow dags unpause <dag_id>`.

**`schedule` y expresión cron.** Cuándo se ejecuta un DAG. Un cron tiene 5 campos: minuto, hora, día del mes, mes y día de la semana.
- *En el proyecto:* `"0 6 * * *"` = todos los días a las 06:00; `"0 10 * * 1-5"` = de lunes a viernes a las 10:00.

**`start_date`, *data interval* y *logical date*.**
- `start_date`: desde cuándo existen los runs.
- *Data interval:* el período que "cubre" un run (`data_interval_start` a `data_interval_end`). Un run diario se ejecuta **al terminar** su intervalo.
- *Logical date:* la fecha que identifica al run.
- *En el proyecto:* MP procesa el día de `data_interval_start`.

**`catchup`.** Si es `True`, al activar el DAG Airflow crea los runs de **todos** los intervalos pendientes desde `start_date`.
- *En el proyecto:* MP usa `True` (un día por llamada); el BCCh usa `False` (un rango por llamada).

**Backfill.** Ejecutar un DAG para fechas pasadas. En la versión 2.10 se hace con `airflow dags backfill -s INICIO -e FIN <dag_id>`.
- *En el proyecto:* una alternativa a la recarga con `params`.

**`params`, `Param` y *Trigger DAG w/ config*.** Los parámetros se definen en el DAG y se entregan al lanzarlo a mano con un JSON. `dag_run.conf` sobrescribe los `params`.
- *En el proyecto:* `{"fecha_desde": "2024-01-01", "fecha_hasta": "2026-10-28"}`.

**`default_args`.** Configuración común para todas las tareas de un DAG.
- *En el proyecto:* `retries`, `retry_delay`, callbacks y otros.

**`retries`, `retry_delay`, `retry_exponential_backoff` y `max_retry_delay`.** Cuántas veces reintentar, cuánto esperar la primera vez, si la espera crece y cuál es la espera máxima.
- *En el proyecto:* MP usa 3 reintentos, 2 minutos, backoff y un máximo de 15 minutos.

**`execution_timeout`.** Tiempo máximo que puede durar una tarea antes de que Airflow la detenga.
- *En el proyecto:* 45 minutos en MP, 15 en el BCCh y 2 en `validar_api`.

**Callbacks (`on_failure_callback`, `on_retry_callback`).** Funciones que Airflow llama solo cuando una tarea falla definitivamente o queda en reintento.
- *En el proyecto:* escriben FAILED o RETRY en `audit.etl_audit` y `[ALERTA]` en el log.

**Connection y `AIRFLOW_CONN_*`.** Una Connection guarda los datos para conectarse a un sistema. Se puede definir con una variable de entorno en formato URI.
- *En el proyecto:* `AIRFLOW_CONN_DW_POSTGRES` crea la Connection `dw_postgres`.

**Variable y `AIRFLOW_VAR_*`.** Una Variable es un valor de configuración con nombre. Las definidas por variables de entorno **no se ven en la UI** y **tienen prioridad** sobre las que se crean en la UI o la CLI.
- *En el proyecto:* `mp_ticket`, `bcch_user` y `bcch_pass` vienen del entorno; `mp_base_url` y `mp_max_detalle` se cambian con la CLI.

**Hook.** Objeto que usa una Connection para hablar con un sistema externo.
- *En el proyecto:* `PostgresHook(postgres_conn_id="dw_postgres")`.

**Dataset, `outlets` y *data-aware scheduling*.**
- Un Dataset representa un dato (por ejemplo, una tabla) con un URI.
- Una tarea con `outlets=[ds]` lo marca como actualizado **solo si termina bien**.
- Un DAG con `schedule=ds` corre cuando el Dataset se actualiza.
- `|` significa OR y `&` significa AND.
- En **Airflow 3** los Datasets se llaman **Assets**.
- *En el proyecto:* `schedule=(DS_BCCH | DS_MP)` en el DAG de KPIs.

**`max_active_runs`.** Máximo de runs del mismo DAG que pueden correr al mismo tiempo.
- *En el proyecto:* 1, para no usar el ticket en paralelo.

**`.airflowignore`.** Archivo con patrones de carpetas o archivos que Airflow **no** revisa buscando DAGs. Igual se pueden importar.
- *En el proyecto:* `etl_lib/`.

**Vistas de la UI.**
- *Grid:* runs × tareas, en colores.
- *Graph:* el diagrama de tareas.
- *Logs:* la salida de cada intento.
- *Datasets:* quién produce y quién consume cada Dataset.
- **Clear:** vuelve a ejecutar tareas o runs; marca *Downstream* para incluir las tareas siguientes.
- **Trigger:** lanza un run manual.

**CLI de Airflow**, usada con `docker compose exec airflow-scheduler ...`:
- `airflow dags list`: lista los DAGs.
- `airflow dags list-import-errors`: errores al leer los DAGs.
- `airflow tasks test <dag> <tarea> <fecha>`: prueba una tarea sin registrar su estado.
- `airflow dags trigger <dag> -c '<json>'`: lanza un run con parámetros.
- `airflow dags unpause <dag>`: activa un DAG.
- `airflow variables set|delete <clave> [valor]`: crea o borra Variables.

**Fernet key.** Clave con que Airflow **cifra** las Variables y Connections que guarda en su base. Sin ella aparece el aviso `empty cryptography key`.
- *En el proyecto:* `AIRFLOW_FERNET_KEY` (Issue 1).

**Airflow 2 vs. Airflow 3.** La documentación que aparece primero en internet suele ser la de Airflow 3, que cambia nombres (Datasets → Assets), comandos y la UI. **Nosotros usamos la 2.10.5:** https://airflow.apache.org/docs/apache-airflow/2.10.5/

## G. Git y GitHub (resumen)

El detalle está en el [Manual de GitHub](02_MANUAL_GITHUB.md).

**Repositorio, commit, rama y `main`.** El repositorio es el proyecto con su historial; un commit es una "foto" de los cambios; una rama es una línea de trabajo paralela; `main` es la rama principal, que siempre funciona.

**clone, fetch, pull, push y merge.**
- `clone`: descargar el repositorio.
- `fetch`: traer información sin mezclar.
- `pull`: traer y mezclar.
- `push`: subir tus commits.
- `merge`: unir ramas.

**Conflicto.** Dos cambios en las mismas líneas: Git pide que una persona decida.

**Issue, etiqueta, hito y asignado.** Una tarea registrada, su categoría, su fecha meta y su responsable.

**Pull Request (PR), *review* y *squash and merge*.** El PR es la solicitud para unir una rama; la *review* es su revisión; *squash and merge* la une como un solo commit.

**`.gitignore` y `.gitattributes`.** `.gitignore` lista lo que Git **no** debe versionar, como `.env`, `logs/` y `referencia/`. `.gitattributes` define cómo tratar los archivos; por ejemplo, fines de línea LF para `.sh`.

**Fin de línea LF y CRLF.** Linux y Mac usan LF; Windows usa CRLF. Un script `.sh` con CRLF falla en Linux.
- *En el proyecto:* `*.sh text eol=lf`.

**Tag.** Marca con nombre en un commit.
- *En el proyecto:* `v1.0-entrega`.

**`git archive`.** Empaqueta en un .zip solo los archivos versionados.
- *En el proyecto:* genera `Codigo_E2_INF522.zip`.

## H. Seguridad

**Secreto o credencial.** Dato que da acceso: ticket, contraseña o clave de API.
- *En el proyecto:* `MP_TICKET`, `BCCH_PASS`, `POSTGRES_PASSWORD` y `AIRFLOW_ADMIN_PASSWORD`, todos solo en `.env`.

**"Nada de claves en el código".** Es una restricción de la pauta. Las credenciales se leen del entorno y los archivos de credenciales se excluyen del control de versiones y de las entregas.
- *En el proyecto:* `.env` → `AIRFLOW_VAR_*` y `AIRFLOW_CONN_*`; `.gitignore`; `git archive`.

**Rotar una credencial.** Cambiarla por una nueva cuando pudo quedar expuesta.
- *En el proyecto:* Manual, Parte H, "Subí `.env` por error".

**Principio de mínimo privilegio.** Dar a cada usuario solo los permisos que necesita.
- *En el proyecto:* el rol `analista_kpi` no puede leer `raw`.
