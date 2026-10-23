# Chilean-market-API-data — INF-522 Evaluación 2

Solución de analítica de datos para medir la **gestión de las compras públicas del Estado de Chile**:

- **Procesos ETL en Python** orquestados con **Apache Airflow 2.10**.
- Consumen dos APIs públicas chilenas: **Mercado Público (ChileCompra)** y **Banco Central de Chile**.
- Cargan un **modelo dimensional en PostgreSQL** por capas (`raw → staging → dw → mart`, más `audit`).
- Calculan **6 indicadores de gestión (KPIs)**.

> **Estado:** planificación lista. El código se incorpora por partes mediante **5 issues**, una por integrante. Cada issue explica qué hacer, cómo probarlo y por qué.

## Equipo y asignación

| Issue | Parte del proyecto | Rol | Integrante | Usuario GitHub |
|---|---|---|---|---|
| [#1](planificacion/issues/ISSUE_1_infraestructura.md) | Infraestructura, configuración segura y librería común | Plataforma y máquina de demo | Ivan Sanchez | [@ivvan-asv](https://github.com/ivvan-asv) |
| [#2](planificacion/issues/ISSUE_2_modelo_de_datos.md) | Modelo de datos en PostgreSQL, idempotencia y auditoría | Modelado de datos | Gustavo | [@Rhezm](https://github.com/Rhezm) |
| [#3](planificacion/issues/ISSUE_3_etl_mercado_publico.md) | ETL Mercado Público y demostración de falla | Ingeniería ETL, fuente 1 | Jairo | [@JAlbornozPIXX](https://github.com/JAlbornozPIXX) |
| [#4](planificacion/issues/ISSUE_4_etl_banco_central.md) | ETL Banco Central y cruce de monedas | Ingeniería ETL, fuente 2 | Benja | [@Sonnyxpj](https://github.com/Sonnyxpj) |
| [#5](planificacion/issues/ISSUE_5_kpis_informe_presentacion.md) | KPIs, DAG de indicadores, informe y presentación | Análisis y entregables | Sofia | [@Sofia-espinosa](https://github.com/Sofia-espinosa) |

**Coordinador:** Jairo ([@JAlbornozPIXX](https://github.com/JAlbornozPIXX)), dueño del repositorio. Lo administra, aprueba los merges y habla con los profesores.

## Lee en este orden

1. [`planificacion/00_PLAN_DEL_PROYECTO.md`](planificacion/00_PLAN_DEL_PROYECTO.md): qué haremos, cuándo, quién y por qué. Incluye el análisis de la pauta, las brechas detectadas, el calendario y el runbook de integración.
2. [`planificacion/01_GLOSARIO.md`](planificacion/01_GLOSARIO.md): todos los términos técnicos explicados desde cero.
3. [`planificacion/02_MANUAL_GITHUB.md`](planificacion/02_MANUAL_GITHUB.md): cómo asignar issues, trabajar en ramas, abrir Pull Requests y hacer merge sin conflictos.
4. **Tu issue**, en [`planificacion/issues/`](planificacion/issues/).
5. [`planificacion/03_PRESENTACION_Y_DEFENSA.md`](planificacion/03_PRESENTACION_Y_DEFENSA.md): guion de 8 minutos, demo de falla y banco de preguntas.

Recursos ya probados que usan las issues: [`planificacion/recursos/`](planificacion/recursos/) (SQL, diagrama, DAG de laboratorio y script de mejoras del informe).

## Fechas clave

| Fecha | Hito |
|---|---|
| vie 23-10 | **Día 0:** confirmar el grupo y los links de API con los profesores, issues asignadas, zip de referencia compartido por canal privado y credenciales solicitadas |
| sáb 24-10 | Base lista: modelo de datos e infraestructura en `main` |
| lun 26-10 | ETL de ambas fuentes en `main` |
| mié 28-10 – jue 29-10 | Integración y carga final en la máquina de demo |
| sáb 31-10 | Congelamiento: informe, presentación y .zip finales |
| **dom 01-11** | **Entrega** (PDF + .zip + .pptx) |
| **jue 05-11** | **Presentación** (8 min + preguntas a cada integrante) |

## Estructura final del repositorio

```
Chilean-market-API-data/
├── README.md                       ← este archivo
├── planificacion/                  ← plan, glosario, manual de GitHub, guía de defensa, issues y recursos
├── scripts/crear_issues.sh         ← (opcional) crea etiquetas, hitos e issues con GitHub CLI
├── .github/pull_request_template.md
├── proyecto_etl/                   ← la solución (se completa con las issues 1 a 5)
│   ├── docker-compose.yml, .env.example, requirements.txt, README.md
│   ├── dags/ (3 DAGs + etl_lib/)
│   ├── sql/ddl, sql/kpi, sql/consultas
│   └── docs/ (generador del informe, capturas, figuras)
└── entregables/                    ← Informe PDF, Presentación PPTX y Código .zip
```

## Reglas de oro

1. **Nunca** subas el archivo `.env` ni pegues tickets, usuarios o claves en issues, chats o capturas.
2. **Nunca** subas la carpeta `referencia/`: es el proyecto completo y este repositorio es público. Está en `.gitignore`, y cada parte entra solo por el PR de su issue.
3. **Nadie** trabaja directo en `main`: una issue, una rama y un Pull Request revisado.
4. Solo modificas los archivos de **tu** issue (matriz de propiedad en el manual, Parte G).
5. Antes de abrir un PR: `git fetch origin` + `git merge origin/main`, y vuelve a probar.
6. Usa la documentación de **Airflow 2.10.5**, no la de Airflow 3: https://airflow.apache.org/docs/apache-airflow/2.10.5/

Fuentes de datos: Dirección ChileCompra (API Mercado Público) y Banco Central de Chile (Base de Datos Estadísticos).
