# Manual de GitHub para el equipo: issues, ramas, Pull Requests y merge sin conflictos

> Para quién: los 5 integrantes. La **Parte C** es solo para el coordinador o la coordinadora, que administra el repositorio.
> Tiempo de lectura: 25 minutos. Tenlo abierto mientras trabajas: las Partes D a F son la rutina diaria.

Repositorio: https://github.com/JAlbornozPIXX/Chilean-market-API-data

---

## Parte A — Conceptos en 3 minutos

| Término | Qué es | En nuestro proyecto |
|---|---|---|
| **Repositorio (repo)** | Carpeta del proyecto con todo su historial de cambios | `Chilean-market-API-data` |
| **Commit** | "Foto" de los cambios, con un mensaje que explica qué cambió | `feat(mp): ETL de licitaciones (#3)` |
| **Rama (branch)** | Línea de trabajo paralela. Puedes cambiar cosas sin afectar a los demás | `feature/3-etl-mercadopublico` |
| **`main`** | La rama principal: **siempre debe funcionar** | Solo cambia mediante un Pull Request aprobado |
| **clone / pull / push / fetch** | `clone` descarga el repo la primera vez. `pull` trae cambios y los mezcla. `push` sube tus commits. `fetch` solo trae información, sin mezclar | — |
| **Merge** | Unir los cambios de una rama con los de otra | Tu rama se une a `main` al aprobar tu PR |
| **Conflicto** | Dos personas cambiaron **las mismas líneas** y Git no sabe con cuál quedarse | Se evita con la matriz de propiedad de archivos (Parte G) |
| **Issue** | Tarea registrada en GitHub, con responsable, etiquetas y discusión | Las 5 issues, una por integrante |
| **Pull Request (PR)** | Solicitud para unir tu rama a `main`. Ahí se revisa el código antes de aceptarlo | Un PR por cada parte de cada issue |
| **Review** | Revisión de un PR por otro integrante: comenta, aprueba o pide cambios | Cada PR tiene un revisor asignado |
| **Squash and merge** | Une el PR a `main` como **un solo commit** limpio | Es el único método de merge que usaremos |
| **Label / Milestone / Assignee** | Etiqueta / hito con fecha / persona responsable | Ver la Parte C |

Cómo se ve el trabajo en paralelo:

```
main     ●───────────●─────────────●──────────────●      ← siempre funciona; solo cambia vía PR
          \         ↗ (squash #2)   ↖ (squash #1)   ↖ (squash #3)
feature/2  ●──●────┘                 \               \
feature/1   ●──●──●──────────────────┘                \
feature/3        ●──●──(merge origin/main)──●──●──────┘   ← trae lo nuevo de main antes del PR
```

## Parte B — Configuración inicial (cada integrante, una sola vez)

### B1. Cuenta y herramientas

1. Crea una cuenta en https://github.com, si no tienes. Envía tu **nombre de usuario** al coordinador.
2. Instala lo siguiente:
   - **Git:**
     - macOS: `xcode-select --install`, o `brew install git`.
     - Windows: instala **WSL2 + Ubuntu** (`wsl --install` en PowerShell como administrador) y, dentro de Ubuntu, `sudo apt install git`.
     - Linux: `sudo apt install git`.
   - **VS Code**, desde https://code.visualstudio.com. En Windows agrega la extensión **WSL** para abrir carpetas de Ubuntu.
   - **GitHub CLI** (`gh`):
     - macOS: `brew install gh`.
     - Windows o Ubuntu: sigue https://github.com/cli/cli#installation.
3. Configura tu identidad, que aparecerá en cada commit:
   ```bash
   git config --global user.name "Nombre Apellido"
   git config --global user.email "el-correo-de-tu-cuenta-github@ejemplo.cl"
   git config --global init.defaultBranch main
   git config --global pull.rebase false        # "pull" = merge (lo que usaremos siempre)
   ```

### B2. Autenticación con GitHub

GitHub **no acepta la contraseña de tu cuenta** para Git desde la terminal. Usa GitHub CLI:

```bash
gh auth login
# GitHub.com → HTTPS → "Authenticate Git with your GitHub credentials?" Yes → Login with a web browser
gh auth status        # debe decir "Logged in to github.com account <tu_usuario>"
```

### B3. Acepta la invitación y clona

1. Acepta la invitación que llega a tu correo, o entra a https://github.com/JAlbornozPIXX/Chilean-market-API-data/invitations.
2. Clona el repositorio. En Windows, hazlo **dentro de Ubuntu (WSL)**, en tu carpeta `/home/<usuario>`:
   ```bash
   git clone https://github.com/JAlbornozPIXX/Chilean-market-API-data.git
   cd Chilean-market-API-data
   ```

### B4. Copia local del proyecto de referencia (`referencia/`)

El coordinador comparte por **canal privado** (Drive o similar, **nunca en el repo**) el archivo `Codigo_E2_INF522.zip`. Descomprímelo dentro de la carpeta `referencia/`:

```bash
mkdir -p referencia
unzip ~/Descargas/Codigo_E2_INF522.zip -d referencia/       # ajusta la ruta del zip
ls referencia/proyecto_etl                                   # dags, sql, docs, docker-compose.yml, ...
git status                                                   # referencia/ NO debe aparecer
git check-ignore -v referencia/proyecto_etl/README.md        # confirma que .gitignore la ignora
```

`referencia/` está en `.gitignore`: es tu material de consulta y **nunca se sube**.

## Parte C — Configuración del repositorio (solo el coordinador, Día 0)

### C1. El repositorio es público: el proyecto completo NO se sube

El repositorio se mantiene **público**. Para no exponer la solución completa de una vez, el proyecto completo de referencia vive **solo en tu PC**, en la carpeta `referencia/`, que está en `.gitignore`. Cada parte entra a `proyecto_etl/` únicamente por el PR de su issue, revisado y en el orden del plan.

Verifica que git realmente la ignora antes del primer push:

```bash
git status                                                  # referencia/ NO debe aparecer
git check-ignore -v referencia/proyecto_etl/README.md       # debe mostrar la regla "referencia/" de .gitignore
git add -A --dry-run | grep referencia/ || echo "OK: referencia/ nunca se agregará"
```

> **Ojo:** a medida que se hace merge de los PRs, las partes del proyecto **sí quedan públicas** en `main`; al final estará la solución completa. El reglamento (Art. 67) sanciona la copia *"entre compañeros o reproducidos de algún medio"* con un 1,0 para todos los involucrados. Si en algún momento quieren cerrarlo: *Settings* → *General* → **Danger Zone** → **Change repository visibility** → **Make private**. Si lo hacen, revisen la nota de la sección C4.

### C2. Agregar a los 4 compañeros

*Settings* → *Collaborators* → **Add people** → escribe el usuario de cada uno → *Add*. Les llega un correo y **deben aceptar**: mientras no acepten, no se les puede asignar issues. En un repositorio personal, los colaboradores reciben permiso de **escritura**.

### C3. Configurar cómo se hace merge

*Settings* → *General* → sección **Pull Requests**:

- ✅ **Allow squash merging**. En el mensaje por defecto, elige "Pull request title and description".
- ⬜ Desmarca **Allow merge commits**.
- ⬜ Desmarca **Allow rebase merging**.
- ✅ **Always suggest updating pull request branches**: aparece el botón *Update branch*.
- ✅ **Automatically delete head branches**: borra la rama después del merge.

### C4. Proteger `main` (que nadie suba directo)

Como el repositorio es **público**, la protección de ramas está disponible **gratis** en el plan Free de GitHub.

1. Entra a *Settings* → *Branches* → **Add classic branch protection rule** (o *Add branch ruleset*).
2. En *Branch name pattern* escribe `main`.
3. Marca ✅ **Require a pull request before merging**, con *Required approvals* = **1**.
4. Marca ✅ **Require conversation resolution before merging**.
5. Pulsa *Create*.

Hazlo **después** del paso C7, porque la planificación se sube directo a `main` una sola vez.

> Si más adelante el repositorio pasa a **privado**, GitHub exige el plan **Pro** para mantener la protección. El plan Pro es gratis con el *GitHub Student Developer Pack* (https://education.github.com/pack). Sin Pro, rige un **acuerdo del equipo**: nadie hace `push` a `main`, solo se hace merge por PR aprobado, y el coordinador revisa el historial (*Insights → Network*).

### C5. Etiquetas (labels)

*Issues* → *Labels* → **New label**. Crea estas. El script de la sección C8 también las crea.

| Etiqueta | Color sugerido | Uso |
|---|---|---|
| `infraestructura` | `#0E8A16` | Issue 1 |
| `modelo-datos` | `#1D76DB` | Issue 2 |
| `etl-mercadopublico` | `#FBCA04` | Issue 3 |
| `etl-bcch` | `#B60205` | Issue 4 |
| `kpis-entregables` | `#5319E7` | Issue 5 |
| `bloqueante` | `#D93F0B` | "Estoy bloqueado, necesito ayuda" |
| `duda` | `#C5DEF5` | Preguntas técnicas |
| `bug` | `#EE0701` | Algo que funcionaba y dejó de funcionar |

### C6. Hitos (milestones)

*Issues* → *Milestones* → **New milestone**:

| Hito | Fecha límite |
|---|---|
| Base lista | sáb 24-10-2026 |
| ETL e integración | jue 29-10-2026 |
| Entrega | dom 01-11-2026 |
| Presentación | jue 05-11-2026 |

### C7. Subir la planificación a `main` (primer y único push directo)

Hazlo **antes** de proteger la rama y **antes** de crear issues o PRs:

```bash
cd Chilean-market-API-data
git status
git add README.md .gitignore .gitattributes .github planificacion scripts
git commit -m "docs: plan del proyecto, glosario, manual de GitHub, guía de defensa e issues"
git push origin main
```

### C8. Crear y asignar las 5 issues

> **Crea las 5 issues ANTES de cualquier PR.** Issues y PRs comparten la numeración: si se crean primero, quedan como **#1 a #5** y coinciden con los nombres de rama y con los `Closes #N`.

**Opción A — En la web, recomendada la primera vez:**

Cada archivo de issue ya trae el nombre y el usuario de su responsable:

| Issue | Archivo | Assignee | Etiqueta | Hito |
|---|---|---|---|---|
| #1 | `ISSUE_1_infraestructura.md` | Ivan Sanchez (@ivvan-asv) | `infraestructura` | Base lista |
| #2 | `ISSUE_2_modelo_de_datos.md` | Gustavo (@Rhezm) | `modelo-datos` | Base lista |
| #3 | `ISSUE_3_etl_mercado_publico.md` | Jairo (@JAlbornozPIXX) | `etl-mercadopublico` | ETL e integración |
| #4 | `ISSUE_4_etl_banco_central.md` | Benja (@Sonnyxpj) | `etl-bcch` | ETL e integración |
| #5 | `ISSUE_5_kpis_informe_presentacion.md` | Sofia (@Sofia-espinosa) | `kpis-entregables` | Entrega |

1. Pestaña **Issues** → **New issue**.
2. **Title:** el título de la primera línea del archivo, sin el `#`. Por ejemplo: `Issue 1 · Infraestructura, configuración segura y librería común`.
3. **Description:** abre en GitHub el archivo `planificacion/issues/ISSUE_1_infraestructura.md` → botón **Raw** → selecciona todo, copia y pega.
4. En el panel derecho, completa **Assignees**, **Labels** y **Milestone** según la tabla.
5. **Create** (o *Submit new issue*).
6. Repite en orden: Issue 2, 3, 4 y 5. Verifica que quedaron con los números **#1 a #5**.

**Opción B — Con un script (GitHub CLI):**

```bash
./scripts/crear_issues.sh
#   crea etiquetas, hitos y las 5 issues, y asigna cada una a su responsable (tabla de arriba).
#   Si alguien aún no acepta la invitación, su issue se crea igual y el script avisa para asignarla después.
```

**Asignar o reasignar después:** abre la issue → panel derecho **Assignees** → engranaje → elige a la persona.

**Vincular una rama a la issue (opcional):** en la issue, panel derecho **Development** → **Create a branch**. Usa exactamente el nombre de la issue, por ejemplo `feature/3-etl-mercadopublico`, y luego, en tu PC:

```bash
git fetch origin
git switch feature/3-etl-mercadopublico
```

### C9. Compartir el material de referencia

En tu PC, la carpeta `referencia/` ya tiene el proyecto completo y los entregables originales. Comparte estos dos archivos por un canal **privado**, como una carpeta de Drive solo para el grupo y **nunca por GitHub**:

- `referencia/entregables/Codigo_E2_INF522.zip`: el código; cada compañero lo descomprime en su `referencia/` (Parte B4).
- `referencia/entregables/PAUTA E2-INF-522.pdf`: la pauta.

El zip no tiene claves: `.env` nunca formó parte de él.

## Parte D — Rutina de trabajo de cada integrante

### D1. Crear tu rama (una vez por cada PR)

```bash
git switch main
git pull                                        # trae lo último de main
git switch -c feature/3-etl-mercadopublico      # crea tu rama y te cambia a ella
git push -u origin feature/3-etl-mercadopublico # la publica en GitHub (respaldo + seguimiento)
```

Nombres de rama: `feature/<número de issue>-<tema-corto>` para trabajo nuevo y `fix/<número>-<tema>` para correcciones.

### D2. Al empezar cada sesión de trabajo

```bash
git switch feature/3-etl-mercadopublico
git fetch origin
git merge origin/main           # trae a TU rama lo que ya se aprobó en main
```

Así detectas los conflictos temprano y pequeños, no al final.

### D3. Trabajar y guardar (commit)

```bash
git status                      # ¿qué archivos cambiaron?
git diff                        # ¿qué líneas cambiaron?
git add proyecto_etl/dags/etl_lib/mercadopublico.py      # agrega SOLO tus archivos, uno por uno
git commit -m "feat(mp): muestra aleatoria reproducible del detalle (#3)"
git push                        # sube tus commits a tu rama en GitHub
```

- **No uses `git add .` a ciegas.** Revisa primero `git status`: así nunca subes `.env`, `logs/` ni archivos de otros.
- Haz **commits chicos y frecuentes**, uno por cada cambio con sentido.
- Usa este formato de mensaje: `tipo(alcance): qué cambió (#issue)`.

| Tipo | Cuándo |
|---|---|
| `feat` | Funcionalidad nueva |
| `fix` | Corrección de un error |
| `docs` | Documentación, informe o README |
| `test` | Pruebas |
| `chore` | Tareas de mantención (zip, configuración) |
| `refactor` | Reordenar código sin cambiar lo que hace |

**Equivalente en VS Code:** panel **Source Control** (ícono de ramas) → `+` junto a cada archivo (equivale a `add`) → escribes el mensaje → ✓ **Commit** → **Sync Changes** (equivale a `push`).

### D4. Antes de abrir el Pull Request

```bash
git fetch origin
git merge origin/main           # tu rama al día con main
# vuelve a ejecutar tus pruebas (las de tu issue)
git diff --stat origin/main     # SOLO deben aparecer tus archivos
git ls-files | grep -E "(^|/)\.env$" && echo "¡PELIGRO! .env versionado" || echo "OK: .env no está en git"
git push
```

## Parte E — El Pull Request: abrir, revisar y hacer merge

### E1. Abrir el PR (autor)

1. En GitHub aparece el aviso amarillo **"Compare & pull request"**. También puedes ir a *Pull requests* → *New pull request* → `base: main` ← `compare: tu-rama`.
2. **Título:** `Issue 3: ETL Mercado Público`.
3. **Descripción:** se carga sola desde la plantilla. Complétala. Escribe **`Closes #3`**, que cierra la issue al hacer merge, o **`Parte de #3`** si tu issue tiene un segundo PR pendiente.
4. **Reviewers:** el revisor indicado en tu issue. **Assignees:** tú.
5. **Create pull request**. Si todavía no está listo pero quieres opiniones, usa **Create draft pull request**.

### E2. Revisar un PR (revisor)

1. Pestaña **Files changed** → lee cada archivo.
2. Para comentar una línea, pasa el mouse sobre ella y haz clic en el **+**.
3. Botón **Review changes**:
   - **Comment:** solo comentarios.
   - **Approve:** se puede unir.
   - **Request changes:** hay que corregir antes.
4. Antes de aprobar, revisa:
   - [ ] Solo cambian archivos del dueño de la issue (Parte G)
   - [ ] No hay `.env`, tickets, usuarios ni claves
   - [ ] La descripción dice cómo se probó, y yo repetí al menos una prueba
   - [ ] Entiendo el cambio lo suficiente para explicarlo en la defensa, porque las preguntas son individuales

### E3. Responder a la revisión (autor)

Corrige en **la misma rama** → `commit` → `push`. El PR se actualiza solo. Responde cada comentario y pulsa **Resolve conversation**.

### E4. Hacer merge (el autor tras la aprobación, o el coordinador)

1. Verifica que el PR diga **"This branch has no conflicts with the base branch"**. Si dice *"out-of-date"*, pulsa **Update branch**.
2. En el botón verde, elige **Squash and merge** → **Confirm squash and merge**.
3. Pulsa **Delete branch**, si no se borró sola. La issue se cierra sola gracias a `Closes #N`.

### E5. Después de cada merge (TODOS)

```bash
git switch main
git pull                                   # tu main local al día
git branch -d feature/3-etl-mercadopublico # (opcional) borra tu rama local ya unida
```

Si estás trabajando en otra rama, tráele lo nuevo: `git switch <tu-rama>` + `git fetch origin` + `git merge origin/main`.

## Parte F — Resolver conflictos paso a paso

### F1. Qué es un conflicto

Ocurre cuando `main` y tu rama cambiaron **las mismas líneas de un mismo archivo**. Git te avisa así:

```
CONFLICT (content): Merge conflict in proyecto_etl/README.md
Automatic merge failed; fix conflicts and then commit the result.
```

### F2. Resolverlo en tu PC (recomendado)

```bash
git switch feature/1-infraestructura
git fetch origin
git merge origin/main               # aparece el CONFLICT
git status                          # muestra "both modified: proyecto_etl/README.md"
```

Abre el archivo en VS Code. Verás marcas como estas:

```
<<<<<<< HEAD
Tu versión (lo que hay en tu rama)
=======
La versión que viene de main
>>>>>>> origin/main
```

1. VS Code ofrece botones sobre el bloque: **Accept Current Change** (la tuya), **Accept Incoming Change** (la de main), **Accept Both Changes** (ambas) o **Compare**. También puedes usar **Resolve in Merge Editor**.
2. Deja el texto **final correcto**. A veces es una mezcla de ambos. **Borra las marcas** `<<<<<<<`, `=======` y `>>>>>>>`.
3. Guarda, vuelve a probar y cierra el merge:
   ```bash
   git add proyecto_etl/README.md
   git commit                       # acepta el mensaje "Merge remote-tracking branch 'origin/main'..."
   git push
   ```
4. Si te perdiste, vuelve atrás sin riesgo: `git merge --abort` y pide ayuda en tu issue.

### F3. Resolverlo en la web, solo para conflictos simples de texto

En el PR → **Resolve conflicts** → edita dejando el texto correcto y sin marcas → **Mark as resolved** → **Commit merge**.

### F4. Conflictos en archivos binarios (PNG, PDF, PPTX, ZIP)

No se pueden mezclar: hay que **elegir una versión**. Durante un merge, `--ours` es tu rama y `--theirs` es lo que viene de `main`:

```bash
git checkout --theirs entregables/Informe_E2_INF522.pdf    # me quedo con la versión de main
git add entregables/Informe_E2_INF522.pdf
git commit
```

La regla de la Parte G (cada binario tiene **un solo dueño**) evita que esto pase.

## Parte G — Reglas para CERO colisiones

1. **Cada archivo tiene un dueño.** Solo el dueño lo modifica; los demás piden cambios en la issue del dueño.

   | Archivo o carpeta | Dueño |
   |---|---|
   | `docker-compose.yml`, `.env.example`, `.gitignore` y `requirements.txt` de `proyecto_etl` | Issue 1 |
   | `proyecto_etl/README.md`, `sql/ddl/00_init_meta.sh`, `dags/.airflowignore`, `dags/etl_lib/__init__.py`, `dags/etl_lib/common.py` | Issue 1 |
   | `proyecto_etl/docs/capturas/*`, `entregables/Codigo_E2_INF522.zip` | Issue 1 |
   | `sql/ddl/01_modelo.sql`, `sql/consultas/*`, `docs/figuras/modelo_dimensional.png` | Issue 2 |
   | `dags/etl_lib/mercadopublico.py`, `dags/dag_etl_mercadopublico.py` | Issue 3 |
   | `dags/etl_lib/bcch.py`, `dags/dag_etl_bcch.py` | Issue 4 |
   | `sql/kpi/kpis.sql`, `dags/dag_kpis.py`, `docs/generar_entregables.py`, `docs/figuras/*` (salvo el diagrama), `entregables/*.pdf`, `entregables/*.pptx` | Issue 5 |
   | `README.md` (raíz), `planificacion/*`, `.github/*`, `scripts/*` | Coordinador |
2. **Una issue → una rama → un PR pequeño.** No mezcles temas en un PR.
3. **Actualiza tu rama a diario** con `git merge origin/main`.
4. **Respeta el orden de merge:** Issue 2 (PR A) → Issue 1 → Issues 3 y 4 → Issue 5 (PR A) → Issue 2 (PR B) → evidencia y Issue 5 (PR B) → zip.
5. **No reformatees archivos ajenos** ni cambies sus finales de línea: el `.gitattributes` ya los controla.
6. **Los binarios** (PNG, PDF, PPTX, ZIP) solo los sube su dueño.
7. **Nunca uses `git push --force` ni `git rebase`.** Tampoco uses `git reset --hard`, salvo en el caso de la Parte H ("commits en `main` por error"). Los manuales de internet a veces recomiendan estos comandos; aquí no los necesitamos.
8. **No hagas forks:** todos trabajamos en el mismo repositorio, con ramas.
9. **Las credenciales no viajan:** nunca en commits, issues, PRs, chats ni capturas. Si alguien necesita la máquina de demo, el dueño de la credencial la escribe en persona en el `.env`.
10. **¿Dudas antes de hacer merge?** Pregunta en el PR. Un merge apurado cuesta más que esperar 10 minutos.

## Parte H — Errores frecuentes y cómo salir

| Mensaje o situación | Qué pasó | Solución |
|---|---|---|
| `! [rejected] ... (fetch first)` o `non-fast-forward` | Alguien subió a esa rama antes que tú | `git pull` (hace merge), resuelve si hay conflicto, y `git push` |
| `remote: Permission to ... denied` / `403` | No aceptaste la invitación, o estás con otra cuenta | Acepta la invitación; `gh auth status`; `gh auth login` |
| `Support for password authentication was removed` | Intentaste usar tu contraseña | `gh auth login` (Parte B2) |
| `fatal: not a git repository` | Estás en otra carpeta | `cd` a la carpeta `Chilean-market-API-data` |
| `error: Your local changes ... would be overwritten by merge` | Tienes cambios sin guardar | Haz commit de ellos, o guárdalos temporalmente con `git stash`, haz el merge y recupéralos con `git stash pop` |
| Hice commits en `main` por error (sin `push`) | Trabajaste en la rama equivocada | `git switch -c feature/N-tema` (tus commits se van a la rama nueva) → `git switch main` → `git reset --hard origin/main` (⚠ solo si ya creaste la rama nueva, porque descarta lo de main local) → `git switch feature/N-tema` |
| Subí `.env` por error | Las credenciales quedaron expuestas en el historial | 1) `git rm --cached proyecto_etl/.env` + commit + push. 2) **Cambia** tus claves: la del BCCh (en su portal), la de Postgres y la de Airflow. Para el ticket de Mercado Público, consulta en el formulario de soporte de ChileCompra. 3) Avisa al coordinador. Borrar el archivo **no** lo elimina del historial: lo que te protege es cambiar las claves |
| `CONFLICT (content)` | Cambios en las mismas líneas | Parte F |
| Mi PR muestra archivos que no toqué | Tu rama partió de un `main` antiguo, o cambiaste finales de línea | `git merge origin/main` y luego `git diff --stat origin/main` para revisar |
| `warning: LF will be replaced by CRLF` | Aviso normal en Windows | `.gitattributes` protege los archivos críticos. Trabaja en WSL |
| `error: pathspec 'feature/...' did not match` | La rama existe en GitHub pero no en tu PC | `git fetch origin` y luego `git switch feature/...` |
| `You are in 'detached HEAD' state` | Hiciste checkout de un commit y no de una rama | `git switch <tu-rama>` |
| La issue no se cerró sola al hacer merge | El PR no decía `Closes #N` | Ciérrala a mano con un comentario que enlace el PR |

## Parte I — Cierre del proyecto

1. **Etiqueta (tag) de la versión entregada.** Solo el coordinador, después del último merge del sáb 31-10:
   ```bash
   git switch main && git pull
   git tag -a v1.0-entrega -m "Entrega Evaluación 2 INF-522"
   git push origin v1.0-entrega
   ```
   La etiqueta marca exactamente qué código se entregó.
2. **El .zip** lo genera Ivan con `git archive` (Issue 1, Paso 11). Solo incluye archivos versionados, así que **nunca** incluye `.env`.
3. **Subir a la plataforma del curso** (coordinador, dom 01-11): el PDF, el .zip y el PPTX. Después **descárgalos de vuelta** y ábrelos para verificar que no se corrompieron.

## Chuleta (cheat sheet)

| Quiero… | Comando |
|---|---|
| Ver en qué rama estoy y qué cambió | `git status` |
| Traer lo último de main | `git switch main && git pull` |
| Crear mi rama | `git switch -c feature/N-tema && git push -u origin feature/N-tema` |
| Poner mi rama al día con main | `git fetch origin && git merge origin/main` |
| Guardar cambios | `git add <archivo>` → `git commit -m "tipo(alcance): mensaje (#N)"` → `git push` |
| Ver las diferencias con main | `git diff --stat origin/main` |
| Cancelar un merge con conflictos | `git merge --abort` |
| Ver el historial | `git log --oneline --graph -15` |
| Cambiar de rama | `git switch <rama>` |
| Ver las ramas | `git branch -a` |
