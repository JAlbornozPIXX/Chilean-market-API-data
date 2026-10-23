# Plan del proyecto — INF-522 Evaluación 2 (grupo de 5 integrantes)

> Documento maestro del equipo. Léelo completo una vez (unos 20 minutos) y vuelve a él cada vez que te preguntes "¿qué toca ahora?".
> Fecha de inicio del plan: **vie 23-10-2026** · Entrega: **dom 01-11-2026** · Presentación: **jue 05-11-2026**.

## 0. Resumen en un minuto

- **Qué entregamos.** Tres archivos y una exposición:
  1. **Informe PDF** con la evidencia.
  2. **Carpeta .zip** con los DAGs, los scripts de Python y el DDL.
  3. **Presentación PPTX** de 8 a 12 láminas.
  4. **Exposición grupal de 8 minutos**, con una demostración de falla en vivo y preguntas a **cada** integrante.
- **Punto de partida.** Ya existe un proyecto de referencia terminado: ETL en Python, orquestado con Airflow 2.10, que consume la API de **Mercado Público** y la del **Banco Central**, carga **PostgreSQL** por capas y calcula **6 KPIs**. No partimos de cero. Cada integrante **trae al repositorio una parte**, la **entiende**, la **prueba**, le **aplica mejoras** y la **defiende**.
- **Por qué así.** La pauta pondera **70 % presentación y preguntas** (30 % grupal y **40 % individual**) y **30 % informe y solución**. Un proyecto perfecto que el equipo no sabe explicar **no** llega al 7,0. Por eso cada issue enseña el "por qué" y trae su banco de preguntas.
- **Lo urgente HOY (vie 23-10):**
  1. Escribir a los profesores. La pauta dice *"grupos de no más de 3 personas"* y somos 5, y además hay que registrar nuestros links de API.
  2. **Activar las credenciales del Banco Central.** El proyecto de referencia falló justo ahí: 0 datos y el KPI4 vacío.
  3. Mantener el **proyecto completo fuera de git.** El repositorio es público, así que el proyecto completo vive en `referencia/` (ignorada por `.gitignore`) y a los compañeros se les comparte el zip por canal privado.
  4. Pedir los tickets de Mercado Público.

## 1. El proyecto en una imagen

```
 API Mercado Público ──► DAG etl_mercadopublico_licitaciones (diario 06:00, catchup)
 (licitaciones.json)       validar_api → extraer_raw → transformar_staging → cargar_dw → control_calidad ─┐
                                                                                                      │ Dataset
                         PostgreSQL dw_gestion:  raw → staging → dw (modelo dimensional) ───────────────┤ (OR)
                                                                                                      ▼
 API Banco Central  ───► DAG etl_bcch_indicadores (lun–vie 10:00)                   DAG kpis_gestion_compras_publicas
 (SieteRestWS)             validar_api → extraer_raw → transformar_staging → cargar_dw → control_calidad ─┘        │
                                                                                                    mart: 6 KPIs
           audit.etl_audit registra cada tarea (éxito / reintento / falla) · claves solo en .env (nunca en el código)
```

- **Pregunta de gestión:** *¿Qué tan eficiente, competitivo y transparente es el proceso de licitaciones del Estado y cuánto se licita en términos reales (UF)?*
- **Campo de cruce:** la **fecha** (dimensión de tiempo conformada) y la **moneda** (`dim_moneda` → serie del BCCh). Con ellos cada monto se convierte a CLP y a UF.

## 2. Qué pide la pauta y cómo llegamos al 7,0

### 2.1 Ponderación

| Parte | Peso | Cómo se mide |
|---|---|---|
| Parte 1: Informe + solución ETL (grupal) | **30 %** | 8 criterios de 1 a 5 puntos (total 40) |
| Parte 2: Presentación grupal | **30 %** | Claridad, estructura, PowerPoint y seguridad al mostrar los DAGs, el ETL y los indicadores |
| Parte 2: Preguntas individuales | **40 %** | *"Cada integrante responde con claridad, precisión técnica y dominio total del proceso"* |

- Con Destacado en todo (40/40, 5 y 5) se obtiene el **100 %, es decir, un 7,0**.
- Ojo: en el ejemplo de la pauta, con la misma nota grupal, un integrante que responde "Básico" (3) baja de 6,16 a **5,04**.
- **La presentación es obligatoria:** quien no se presenta obtiene **1,0**.
- **Art. 67:** la copia parcial o exacta significa **1,0 para todos los involucrados**.

### 2.2 Criterio por criterio: qué exige el "Destacado" y qué hacemos

| Criterio (5 pts c/u) | Qué exige "Destacado" | Estado del proyecto de referencia | Acción del plan | Issue |
|---|---|---|---|---|
| Informe y carpeta comprimida | Todos los apartados, evidencia completa (capturas, logs, consultas) y carpeta **reproducible con el README** | Completo, pero con nombres de ejemplo, evidencia del BCCh solo con fallas, sin anexo de consultas y con un README con un comando incompleto | Regenerar con datos finales, agregar el Anexo A de consultas, corregir el README y **probar la reproducibilidad en otra PC** | 5, 1, 2 |
| Selección y justificación de fuentes | 2 APIs o más bien justificadas, **campo de cruce claro** y **links propios** (no repetidos con otros grupos) | Bien justificadas, pero **el cruce no estaba demostrado** (0 datos BCCh) y los links no estaban registrados con los profesores | Activar el BCCh y hacer la recarga histórica; registrar los links con los profesores hoy | 4, coordinador |
| Extracción y transformación en Python | Autenticación, paginación, límites y errores HTTP; transformaciones con validaciones; **métricas completas** | Robusta, pero sin métricas del BCCh, y el detalle de MP cubría solo el ~8 % (92,2 % sin detalle) | Métricas del BCCh; **muestra aleatoria reproducible**; más detalle en la carga nocturna | 3, 4 |
| Modelo de datos | Capas, modelo dimensional coherente (hechos, dimensiones, claves y tipos) e idempotencia | Completo e idempotente, sin diagrama | **Diagrama ER**, diccionario de datos (`COMMENT ON`) y rol de solo lectura | 2 |
| Diseño de DAGs | 1 DAG por API + DAG de KPIs automático; schedule, dependencias, Connections y Variables justificados | Cumple (Datasets con OR, configuración desde `.env`) | Mantener; preparar la justificación de cada parámetro; agregar la Fernet key | 3, 4, 5, 1 |
| Tolerancia a fallas | Reintentos con espera, timeouts, validación previa, registro de fallas, auditoría, aislamiento y **demostración en vivo** | Cumple; la demo completa tarda ~10 min (backoff) y la presentación dura 8 | **Guion de demo** que dispara la falla al inicio, con plan B en video | 3, 1 |
| Indicadores de gestión | 4–6 KPIs con definición completa (**meta**), bien calculados en SQL | 6 KPIs, pero **KPI4 sin valor y sin meta** | KPI4 con datos (BCCh) y con **umbral de alerta** | 4, 5 |
| Estrategia de datos | Decisiones, calidad, origen, frecuencia, riesgos y escalabilidad, con fundamento | Buen contenido, pero de **~1,3 páginas** (la pauta pide **2 a 3**) | Tabla de calidad **medida**, matriz de riesgos y sección de gobierno: ya llega a **2 páginas** | 5 (+2, 3, 4) |
| Presentación grupal (30 %) | Clara, estructurada, fluida y con seguridad al mostrar Airflow | 10 láminas con nombres de ejemplo | Reparto de láminas, guion cronometrado y 3 ensayos | Todos |
| Preguntas individuales (40 %) | **Dominio total del proceso** por cada integrante | — | Banco de preguntas en cada issue y en la guía, capacitación cruzada y simulacro | Todos |

### 2.3 Indicadores de evaluación del curso: deben poder explicarse

1. *"Identifica los elementos fundamentales de la estrategia de sistemas de información"*: es el capítulo de Estrategia de datos (Issue 5).
2. *"Distingue los tipos de almacenamientos de los sistemas de información OLTP"*: OLTP vs. OLAP/DW (Issue 2, Paso 3).
3. *"Utiliza técnicas de depuración y extracción de datos desde bases de datos fuentes a esquemas globales"*: el ETL raw → staging → dw (Issues 3 y 4).

## 3. Brechas del proyecto de referencia y su solución

Se detectaron revisando el código, el informe entregado, la presentación, el .zip y la pauta. Los cambios de código propuestos **se probaron**: el DDL y las consultas en PostgreSQL 16, y el generador del informe de punta a punta.

| # | Severidad | Brecha | Evidencia | Solución | Issue |
|---|---|---|---|---|---|
| 1 | **Crítica** | El BCCh no tiene datos: el KPI4 está vacío y el cruce no está demostrado | 0 filas en las 3 tablas del BCCh; todos los runs con `Codigo=-5 Invalid username or password` | Registrar y **activar** las credenciales (*"Para activar las credenciales debe iniciar sesión"*) y hacer la recarga histórica desde 2024 **antes** que Mercado Público | 4 |
| 2 | **Crítica** | El grupo es de 5 y la pauta dice *"no más de 3 personas"* | Pauta, instrucción 2 | Correo a los profesores hoy (modelo en la sección 8). Plan B en el Anexo | Coordinador |
| 3 | Alta | Los links de API no deben coincidir con los de otro grupo, y MP y el BCCh aparecen como ejemplos en la pauta | Pauta, sección 1 | Registrar nuestros 2 links hoy, en el mismo correo | Coordinador |
| 4 | Alta | El repositorio es **público**: subir el proyecto completo de una vez lo dejaría expuesto a copia (Art. 67) | GitHub lo muestra como público | El proyecto completo **no se sube**: queda en `referencia/` (en `.gitignore`) y se comparte por canal privado. Cada parte entra a `proyecto_etl/` por el PR de su issue | Coordinador |
| 5 | Alta | La Estrategia de datos mide ~1,3 páginas (se piden 2 a 3) | Informe, págs. 12–13 | 3 secciones nuevas basadas en datos (ya probadas: el capítulo queda en 2 páginas) | 5 |
| 6 | Media | KPI4 sin meta ni umbral (la pauta exige *meta o umbral*) | Informe, sección 5 | Umbral de alerta de ±20 % mensual | 5 |
| 7 | Media | El 92,2 % de las licitaciones no tiene detalle; la muestra son siempre "los primeros" del listado | Informe, sección 8.1 | Muestra aleatoria reproducible; `mp_max_detalle=250` en la carga nocturna (sin pasar la cuota de 10.000/día) | 3 |
| 8 | Media | Detalles del informe: nombres de ejemplo, columna "left", "0,00 s" en KPIs, falta el tiempo del BCCh, la figura de arquitectura se descuadra, no hay diagrama del modelo y las capturas del BCCh solo muestran fallas | PDF de referencia | 22 mejoras automáticas en el generador y capturas nuevas | 5, 1, 2 |
| 9 | Media | En Windows, `* text=auto` puede dejar `00_init_meta.sh` con CRLF y romper el arranque de PostgreSQL | `.gitattributes` original | **Ya corregido** en este repo (`*.sh text eol=lf`), y se recomienda trabajar en WSL | — |
| 10 | Media | Ambos proyectos usan una carpeta llamada `proyecto_etl`: Docker Compose **compartiría el volumen de la base** con el de referencia | Nombre de proyecto de Compose = nombre de la carpeta | `name: inf522_e2` en `docker-compose.yml` | 1 |
| 11 | Baja | El README trae `airflow variables set ...` sin `docker compose exec ...` | README, paso 5 | Corregir y agregar las secciones de reproducibilidad | 1 |
| 12 | Baja | El log entregado muestra `empty cryptography key`: Variables sin cifrar | `log_falla.txt` | Fernet key opcional | 1 |
| 13 | Baja | El enlace de documentación del BCCh en `bcch.py` está roto; el de MP redirige | Verificado al abrirlos | Actualizar los docstrings | 4, 3 |
| 14 | Baja | La demo completa de reintentos tarda ~10 min (intentos a las 02:12, 02:16 y 02:22 en el log) | `log_falla.txt` | Guion: disparar la falla al empezar la presentación | 3 |

## 4. Organización del equipo

### 4.1 Por qué se dividió así

El trabajo se reparte **por tramos del flujo de datos**: plataforma → modelo → fuente 1 → fuente 2 → indicadores y entregables. Esto tiene tres ventajas:

1. Cada integrante es **dueño de archivos distintos**, así que **los conflictos de merge son casi imposibles**.
2. Cada tramo **se prueba por separado**.
3. En la presentación, cada uno expone **su** tramo y el relato sigue el orden natural del dato.

### 4.2 Roles

| Integrante | Issue | Responsabilidad | Entrega | Láminas | Revisa el PR de |
|---|---|---|---|---|---|
| Ivan (@ivvan-asv) | [#1](issues/ISSUE_1_infraestructura.md) | Plataforma, configuración segura, librería común, **máquina de demo**, evidencia y .zip | `docker-compose.yml`, `.env.example`, README, `common.py`, capturas, .zip | 3, 10 y el teclado en la demo | Gustavo (PR A) |
| Gustavo (@Rhezm) | [#2](issues/ISSUE_2_modelo_de_datos.md) | Modelo de datos, idempotencia, auditoría, diagrama y consultas de evidencia | DDL, diagrama ER, `consultas_evidencia.sql` | 5, 7 | Ivan; Sofia (PR A) |
| Jairo (@JAlbornozPIXX) | [#3](issues/ISSUE_3_etl_mercado_publico.md) | ETL Mercado Público y **demo de falla** | `mercadopublico.py` + DAG, carga final nocturna | 2 (MP), 8 | Benja |
| Benja (@Sonnyxpj) | [#4](issues/ISSUE_4_etl_banco_central.md) | ETL Banco Central, credenciales, recarga histórica y **cruce** | `bcch.py` + DAG | 2 (BCCh), 4 | Jairo |
| Sofia (@Sofia-espinosa) | [#5](issues/ISSUE_5_kpis_informe_presentacion.md) | KPIs, DAG de indicadores, **informe y PPTX** | `kpis.sql`, `dag_kpis.py`, generador, PDF, PPTX | 1, 6, 9 | Gustavo (PR B) |

### 4.3 El coordinador o la coordinadora

Es **Jairo (@JAlbornozPIXX)**, dueño del repositorio: solo él puede cambiar su configuración. Además tiene la Issue 3. Sus tareas:

- Habla con los profesores.
- Configura el repositorio (Manual, Parte C).
- Crea y asigna las issues.
- Vigila el calendario y el orden de merge.
- Hace el tag y sube los entregables.
- Desbloquea a quien lo necesite.

### 4.4 Todos responden por todo el flujo

Las preguntas son **individuales** y pueden ser sobre cualquier parte. Por eso hay **capacitación cruzada** (sección 11): cada uno debe explicar el flujo completo, de la API al KPI, y su parte con detalle técnico.

## 5. Propiedad de archivos (resumen)

La tabla completa está en el [Manual de GitHub](02_MANUAL_GITHUB.md), Parte G. La regla: **solo el dueño modifica su archivo; los demás piden cambios en la issue del dueño.** Los binarios (PNG, PDF, PPTX, ZIP) tienen un único dueño.

## 6. Dependencias y orden de merge

```
                ┌──────────────────────────────────────────────────────────────────────────────┐
 sáb 24  #2-A DDL ──► #1 Infra ──┬──► lun 26  #3 ETL MP ───┐                                    │
                                 └──► lun 26  #4 ETL BCCh ─┼──► mar 27  #5-A KPIs               │
                                           mar 27  #2-B diagrama + consultas ──────────────────┤
 mié 28 – jue 29  Integración en la máquina de demo (runbook, sección 9) ──► #1 evidencia (capturas)
 vie 30  #5-B informe + PPTX (revisión de todos) ──► sáb 31  #1 zip + tag v1.0-entrega ──► dom 01  ENTREGA
```

- **#2-A antes que #1:** `docker-compose.yml` monta `01_modelo.sql`. Si el archivo no existe, Docker crea una carpeta con ese nombre y PostgreSQL no arranca.
- **#1 antes que #3, #4 y #5:** todos necesitan Docker Compose y `common.py` para probar.
- **#5-B al final:** el informe se genera con los **datos finales** y las **capturas finales**.

## 7. Cronograma día a día

| Fecha | Qué se hace | Responsable | Listo cuando… |
|---|---|---|---|
| **vie 23-10 (hoy)** | **Día 0** (sección 8): correo a los profesores; colaboradores; subir la planificación (sin `referencia/`); crear y asignar las 5 issues; compartir el zip de referencia por canal privado. **Todos:** instalar las herramientas, clonar, pedir el ticket de MP y **activar** la cuenta del BCCh | Coordinador y todos | Las 5 issues asignadas; todos con el repo clonado y `docker run hello-world` OK; credenciales pedidas |
| sáb 24-10 | **Mañana:** PR A de la Issue 2 (DDL). **Tarde:** PR de la Issue 1 (infra). Reunión de arranque (45 min): recorrido del plan y de la arquitectura | Gustavo, Ivan y todos | `docker compose up -d` desde `main` funciona en todas las PC |
| dom 25-10 | Issues 3, 4 y 5 copian, entienden y prueban localmente. Control de avance de 15 min | Jairo, Benja, Sofia | Pruebas locales en verde |
| lun 26-10 | Merge de los PR de las Issues 3 y 4 | Jairo, Benja | Los 2 DAGs ETL en `main`, sin errores de importación |
| mar 27-10 | Merge del PR A de la Issue 5 (KPIs) y del PR B de la Issue 2 (diagrama + consultas). **Capacitación cruzada 1** (90 min) | Sofia, Gustavo y todos | KPIs calculados en local; cada uno expuso su parte |
| mié 28-10 | **Integración** en la máquina de demo (sección 9): arranque limpio, BCCh histórico, KPIs. **22:00:** catchup de MP con `mp_max_detalle=250` | Ivan, Jairo, Benja | BCCh 2024→hoy cargado; catchup corriendo |
| jue 29-10 | Verificación (todo verde, **KPI4 con valor**), prueba de idempotencia, ensayo de la demo, capturas, log y respaldo; PR de evidencia. Aportes al informe como comentarios en la Issue 5 | Ivan, Gustavo, Jairo, Benja | Capturas en `main`; aportes publicados |
| vie 30-10 | PR B de la Issue 5 (informe + PPTX), revisado por todos. **Prueba de reproducibilidad** en otra PC. **Ensayo 0** | Sofia, Gustavo y todos | PR B con merge; README probado por alguien que no lo escribió |
| sáb 31-10 | **Congelamiento:** últimos ajustes y regeneración final; .zip con `git archive` y su verificación; tag `v1.0-entrega` | Sofia, Ivan, coordinador | .zip y tag listos |
| **dom 01-11** | **ENTREGA:** subir PDF, .zip y PPTX; descargarlos y verificar que abren | Coordinador | Archivos subidos y verificados |
| lun 02-11 | **Ensayo 1** cronometrado + **capacitación cruzada 2** (rotación de partes) | Todos | Exposición en 8 min o menos |
| mar 03-11 | **Simulacro de preguntas individuales**; grabar el **video de respaldo** de la demo | Todos | Cada uno respondió 6 preguntas al azar |
| mié 04-11 | **Ensayo general** en condiciones reales + checklist de la máquina de demo | Todos | Todo OK |
| **jue 05-11** | **PRESENTACIÓN** | Todos | — |

## 8. Día 0 (hoy): checklist

**Coordinador (Jairo):**

- [ ] Enviar a los profesores el correo de abajo: tamaño del grupo y registro de los links de API
- [ ] Verificar que `referencia/` (el proyecto completo) **no** aparece en `git status` (Manual, C1)
- [ ] 4 colaboradores invitados y merge solo por squash (Manual, C2–C3)
- [ ] Protección de `main` activada: en un repositorio público es gratis (Manual, C4)
- [ ] Subir la planificación a `main` (C7) y crear etiquetas, hitos y **las 5 issues en orden** (C5, C6, C8)
- [ ] Compartir por un canal **privado** `Codigo_E2_INF522.zip` y la pauta en PDF (C9)
- [ ] Completar en el `README.md` la tabla con nombres y usuarios de GitHub

**Cada integrante:**

- [ ] Docker Desktop (en Windows, con WSL2 + Ubuntu), Git, VS Code y GitHub CLI instalados (Manual, B1)
- [ ] `gh auth login`, invitación aceptada, repo clonado y `referencia/` creada (B2–B4)
- [ ] Ticket de Mercado Público solicitado: https://www.chilecompra.cl/api/ → "Pide tu ticket" (con Clave Única; **uno por persona**)
- [ ] Cuenta del Banco Central registrada **y activada**: https://si3.bcentral.cl/Siete/es/Siete/API
- [ ] Leí este plan, mi issue y el glosario

**Correo modelo a los profesores:**

> **Asunto:** INF-522 Evaluación 2 – Conformación del grupo y registro de APIs
>
> Estimados profesores Natalia Morales y Sebastián Farías:
>
> Junto con saludar, les escribimos para (1) consultar por la conformación de nuestro grupo y (2) registrar las APIs que utilizaremos, para no coincidir con otro grupo.
>
> 1. Integrantes: [Nombre 1], [Nombre 2], [Nombre 3], [Nombre 4] y [Nombre 5]. La pauta indica grupos de no más de 3 personas: ¿podemos trabajar como grupo de 5, o debemos reorganizarnos?
> 2. APIs (links):
>    - Mercado Público (ChileCompra), licitaciones: `https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json`
>    - Banco Central de Chile, Base de Datos Estadísticos: `https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx` (series de dólar observado, euro, UF y UTM)
>
> Quedamos atentos a su respuesta.
> Saludos cordiales,
> [Nombre], en representación del grupo

## 9. Runbook de integración y carga final (mié 28-10 y jue 29-10, en la máquina de demo)

Lo opera Ivan, en su laptop, con Jairo y Benja presentes y Sofia disponible. Los comandos se ejecutan en `proyecto_etl/`, en macOS, Linux o WSL.

```bash
# 0) Si en esa PC corre otro stack (por ejemplo, el de referencia), detenlo: usa los mismos puertos
cd <ruta>/trabajo_de_gestion/proyecto_etl && docker compose down        # sin -v: no borra sus datos

# 1) Código final desde main
cd <ruta>/Chilean-market-API-data && git switch main && git pull
cd proyecto_etl
cp -n .env.example .env     # cada dueño escribe EN PERSONA su credencial (ticket MP, usuario y clave BCCh)

# 2) Arranque limpio: la base se crea vacía y el DDL se aplica solo
docker compose down -v      # ⚠ borra SOLO la base de este proyecto (inf522_e2); se usa una vez, aquí
docker compose up -d
docker compose ps -a        # postgres healthy · airflow-init Exited (0) · webserver y scheduler Up
docker compose exec airflow-scheduler airflow dags list-import-errors   # "No data found"

# 3) Primero el DAG de KPIs (así recibe los eventos de los Datasets desde el inicio)
docker compose exec airflow-scheduler airflow dags unpause kpis_gestion_compras_publicas

# 4) Banco Central: histórico ANTES que Mercado Público (para que las conversiones a UF queden listas)
docker compose exec airflow-scheduler airflow dags unpause etl_bcch_indicadores
docker compose exec airflow-scheduler airflow dags trigger etl_bcch_indicadores -c '{"fecha_desde": "2024-01-01", "fecha_hasta": "2026-10-28"}'
#    esperar a que quede verde (unos minutos) y verificar con la consulta 10 de consultas_evidencia.sql

# 5) Mercado Público, desde las 22:00 (ChileCompra recomienda las descargas masivas entre 22:00 y 07:00)
docker compose exec airflow-scheduler airflow variables set mp_max_detalle 250
docker compose exec airflow-scheduler airflow dags unpause etl_mercadopublico_licitaciones
#    catchup: ~27 runs (01-10 a 27-10), en serie; ≈ 27 × 252 ≈ 6.800 llamadas < 10.000 diarias
```

**Jueves 29-10, por la mañana:**

```bash
# 6) Verificación completa (todos los runs verdes, KPI4 con valor, conversiones, auditoría)
docker compose exec -T postgres psql -U etl -d dw_gestion < sql/consultas/consultas_evidencia.sql
```

7. **Si Mercado Público cargó antes que el BCCh:** en la UI, DAG MP → último run → `cargar_dw` → **Clear**, con *Downstream* marcado. Recalcula los montos y vuelve a disparar los KPIs.
8. **Prueba de idempotencia** (Gustavo): consulta 2 → *Clear* de un run ya exitoso → consulta 2 otra vez. Staging y dw deben quedar iguales. Publicar el resultado en la Issue 5.
9. **Ensayo de la demo de falla** (Jairo e Ivan), con el guion de [03_PRESENTACION_Y_DEFENSA.md](03_PRESENTACION_Y_DEFENSA.md). Se generan `07_grid_recuperado.png` y `log_falla.txt`.
10. **Capturas** 01 a 07 y **respaldo** de la base: Issue 1, Paso 10.
11. Dejar `mp_max_detalle` en **150** para la operación diaria:
    ```bash
    docker compose exec airflow-scheduler airflow variables set mp_max_detalle 150
    ```
12. **Desde ahora, esta PC no se formatea ni se reinstala Docker hasta el 05-11.** Si se apaga, al encenderla basta con `docker compose up -d`, y el catchup de MP recupera los días faltantes.

## 10. Entregables (vie 30-10 a dom 01-11)

| Entregable | Quién | Cómo | Verificación |
|---|---|---|---|
| Informe PDF | Sofia | `docs/generar_entregables.py` con las 22 mejoras (Issue 5) | Checklist del Paso 11 de la Issue 5 |
| Presentación PPTX (10 láminas) | Sofia | El mismo script | Nombres, imágenes y KPIs con valor |
| Carpeta .zip | Ivan | `git archive` desde `main` (Issue 1, Paso 11) | `tar -tf`: sin `.env`, logs ni caché; con DAGs, scripts y DDL |
| Prueba de reproducibilidad | Gustavo (con el README de Ivan) | Descomprimir en una PC distinta y seguir solo el README | Funciona sin ayuda verbal |
| Tag `v1.0-entrega` y subida a la plataforma | Coordinador | Manual, Parte I | Descargar y abrir los 3 archivos |

## 11. Preparación de la presentación y de las preguntas

El detalle completo está en [03_PRESENTACION_Y_DEFENSA.md](03_PRESENTACION_Y_DEFENSA.md): reparto de láminas, guion de 8 minutos, demo de falla paso a paso y banco de preguntas transversales.

- **Capacitación cruzada 1 (mar 27-10, 90 min):** cada integrante explica su parte en 12 minutos **mostrando su código**, y los demás preguntan usando la sección "Preguntas probables" de su issue.
- **Capacitación cruzada 2 (lun 02-11):** **rotación**. Cada uno explica una parte que **no** es suya. Si alguien no puede explicarla, el dueño la refuerza.
- **Simulacro (mar 03-11):** cada uno responde 6 preguntas al azar (2 de su parte y 4 de otras), con un máximo de 1 minuto por respuesta.

## 12. Riesgos y planes de contingencia

| Riesgo | Prob. | Impacto | Mitigación / contingencia | Dueño |
|---|---|---|---|---|
| Los profesores no aceptan un grupo de 5 | Media | Muy alto | Preguntar hoy. Plan B en el Anexo | Coordinador |
| Otro grupo usa los mismos links de API | Media | Alto | Registrarlos hoy. Si hay choque, cambiar **solo** el módulo de esa fuente: por ejemplo, el BCCh por mindicador.cl (mencionado en la pauta) o licitaciones por órdenes de compra de MP. Staging, dw y KPIs se mantienen | Coordinador, Jairo, Benja |
| No se logran activar las credenciales del BCCh | Baja | Alto | Hacerlo hoy. Si falla, usar la cuenta activada de otro integrante (sin compartir la clave por chat: la escribe su dueño) | Benja |
| Cuota de MP o bloqueo por IP | Media | Medio | Cada uno usa su ticket; `mp_max_detalle=20` en las pruebas; no descargar todos a la vez desde la misma red; carga final nocturna | Jairo |
| Fuga de credenciales | Baja | Alto | `.gitignore`, revisión en el PR y `git archive`. Si ocurre: **cambiar las claves** (Manual, Parte H) | Todos |
| Copia por terceros: el repositorio es público | Media | Alto | El proyecto completo no se sube de una vez (`referencia/` ignorada) y las partes entran por PR desde el sáb 24-10. Al final, `main` tendrá la solución completa visible. Si se detecta riesgo de copia, se puede hacer privado en cualquier momento (Manual, nota de C1) | Coordinador |
| Docker no funciona en una PC (RAM, WSL, CRLF) | Media | Medio | Guía de la Issue 1, `.gitattributes`; trabajar en WSL; pedir ayuda a Ivan | Ivan |
| Conflictos de merge | Baja | Medio | Matriz de propiedad, PRs pequeños, `merge origin/main` diario | Coordinador |
| La demo en vivo falla (internet, API o tiempo) | Media | Alto | Disparar la falla al inicio; hotspot de celular; **video de respaldo**; `mp_max_detalle=10` antes de presentar | Jairo, Ivan |
| La laptop de demo falla el día de la presentación | Baja | Muy alto | Respaldos `.dump` en el Drive del grupo. Una **segunda máquina** preparada (sugerida: la de Sofia) con `pg_restore` probado | Ivan, Sofia |
| Un integrante falta a la presentación | Baja | Muy alto | Para él o ella: nota 1,0. El resto cubre su parte: por eso todos ensayan todas las partes | Todos |
| Confusión con la documentación de Airflow 3 | Alta | Bajo | Usar siempre https://airflow.apache.org/docs/apache-airflow/2.10.5/. En Airflow 3 los Datasets se llaman *Assets* | Todos |
| Un integrante se atrasa | Media | Medio | Control diario breve en la issue; etiqueta `bloqueante`; el coordinador reasigna | Coordinador |

**Restaurar el respaldo en otra PC** (emergencia), con el repo clonado y `.env` creado:

```bash
docker compose up -d
docker compose cp ../respaldo_dw_gestion.dump postgres:/tmp/dw.dump
docker compose exec postgres pg_restore -U etl -d dw_gestion --clean --if-exists /tmp/dw.dump
```

## 13. Checklist final contra la pauta (sáb 31-10, antes de entregar)

**Restricciones:**

- [ ] ETL en Python, PostgreSQL como destino, orquestación con Airflow y datos obtenidos **solo por API**
- [ ] **Sin contraseñas ni claves** en el código ni en el .zip (revisado con `tar -tf` y `git grep`)

**Fuentes:**

- [ ] 2 APIs públicas chilenas justificadas (viabilidad, documentación, límites, volumen y licencia)
- [ ] Links registrados con los profesores
- [ ] Campo de cruce explicado **y demostrado** (montos en UF)

**Extracción:**

- [ ] Respuesta cruda en `raw` con fecha y hora de extracción
- [ ] Manejo de errores, paginación y límites de uso

**Transformación:**

- [ ] Las 9 técnicas de la pauta aplicadas y documentadas
- [ ] Validaciones de calidad antes de cargar

**Carga:**

- [ ] Esquemas raw, staging, dw y mart
- [ ] Al menos 1 hecho y 2 dimensiones, incluida la de tiempo (tenemos 2 y 5)
- [ ] DDL entregado
- [ ] Idempotencia demostrada

**Airflow:**

- [ ] 3 DAGs: uno por API más el de KPIs, que se dispara automáticamente
- [ ] Validación previa (`validar_api`)
- [ ] Schedule justificado
- [ ] Recarga de un rango pasado

**Auditoría:**

- [ ] `etl_audit` con DAG, run_id, filas leídas y cargadas, % de nulos, duración y estado
- [ ] `on_failure_callback`

**Demo de falla:**

- [ ] Ensayada 3 veces o más, con su video de respaldo

**KPIs:**

- [ ] 6 KPIs con nombre, objetivo, fórmula, fuente, periodicidad y **meta o umbral**
- [ ] Calculados en SQL sobre `mart`
- [ ] Presentados en tablas y gráficos

**Estrategia de datos:**

- [ ] Entre 2 y 3 páginas, con decisiones, calidad, origen, frecuencia, riesgos (API, límites, licencias, datos personales) y escalabilidad

**Información de carga:**

- [ ] Registros por tabla
- [ ] % de errores o nulos
- [ ] Tiempo de **cada** DAG

**Análisis del proceso:**

- [ ] Hallazgos
- [ ] Problemas y su solución
- [ ] Fallas y la respuesta del diseño

**Informe:**

- [ ] Portada, introducción, fuentes, paso a paso, DAGs (diagrama, capturas exitosas y fallidas, logs), KPIs, estrategia, información de carga, análisis y conclusiones

**Presentación:**

- [ ] Entre 8 y 12 láminas, con problema y fuentes, arquitectura, DAGs, modelo, indicadores y hallazgos, estrategia y lecciones aprendidas
- [ ] **Se entrega junto con el informe**

**Archivos:**

- [ ] Los 3 entregables subidos y verificados

## 14. Comunicación y reglas de trabajo

- **Las decisiones técnicas se escriben en GitHub,** en issues y PRs, para que quede el registro y nadie dependa del chat. El chat sirve solo para coordinar.
- **Control diario breve.** Un comentario en tu issue con tres líneas: *hice / haré / bloqueos*.
- **¿Bloqueado más de 30 minutos?** Comenta el error exacto en tu issue (sin credenciales), ponle la etiqueta `bloqueante` y avisa por el chat.
- **"Listo" significa** que los criterios de aceptación de la issue están cumplidos **y** el PR está aprobado y unido a `main`.
- **Nunca se comparten credenciales** por ningún medio.

## Anexo — Si los profesores exigen grupos de 3 como máximo

Hay que reorganizarse **antes de avanzar**. Considera dos cosas:

1. **Dos grupos no pueden entregar el mismo proyecto** (Art. 67). Cada grupo necesita su propia solución y sus propios links de API.
2. **El grupo que mantenga este proyecto** redistribuye las 5 issues así:
   - **Persona A:** Issues 1 y 2 (plataforma y modelo).
   - **Persona B:** Issues 3 y 4 (los dos ETL).
   - **Persona C:** Issue 5 y la coordinación.

   Las issues, el manual y el calendario siguen sirviendo sin cambios.
