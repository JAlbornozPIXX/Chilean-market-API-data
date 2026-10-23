#!/usr/bin/env bash
# Crea etiquetas, hitos y las 5 issues del plan en GitHub usando GitHub CLI (gh).
#
# Uso, desde la raíz del repositorio y después de `gh auth login`:
#   ./scripts/crear_issues.sh
# Por defecto asigna cada issue a su responsable (ver EQUIPO). Para usar otros usuarios:
#   ./scripts/crear_issues.sh usuario1 usuario2 usuario3 usuario4 usuario5
# Solo se puede asignar a quien ya ACEPTÓ la invitación como colaborador; si alguien aún no
# acepta, la issue se crea igual y el script avisa para asignarla después en la web.
set -euo pipefail

REPO="${REPO:-JAlbornozPIXX/Chilean-market-API-data}"
DIR="planificacion/issues"
# Issue 1 Ivan · Issue 2 Gustavo · Issue 3 Jairo · Issue 4 Benja · Issue 5 Sofia
EQUIPO=(ivvan-asv Rhezm JAlbornozPIXX Sonnyxpj Sofia-espinosa)
[ "$#" -eq 5 ] && EQUIPO=("$@")
[ "$#" -ne 0 ] && [ "$#" -ne 5 ] && { echo "Indica 0 o 5 usuarios"; exit 1; }

command -v gh >/dev/null || { echo "Falta GitHub CLI: https://cli.github.com"; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "Primero ejecuta: gh auth login"; exit 1; }
[ -d "$DIR" ] || { echo "Ejecuta el script desde la raíz del repositorio"; exit 1; }

# Issues y PRs comparten numeración: si ya existen, las issues no quedarán como #1..#5
n_issues=$(gh issue list --repo "$REPO" --state all --limit 1 --json number --jq 'length')
n_prs=$(gh pr list --repo "$REPO" --state all --limit 1 --json number --jq 'length')
if [ "$n_issues" != "0" ] || [ "$n_prs" != "0" ]; then
  echo "El repositorio ya tiene issues o PRs: la numeración #1..#5 no coincidirá con el plan."
  read -r -p "¿Continuar de todos modos? (s/N) " r
  [ "$r" = "s" ] || exit 1
fi

etiqueta() {
  gh label create "$1" --repo "$REPO" --color "$2" --description "$3" --force >/dev/null
  echo "etiqueta: $1"
}
etiqueta infraestructura    0E8A16 "Issue 1: Docker, configuración y librería común"
etiqueta modelo-datos       1D76DB "Issue 2: DDL, modelo dimensional y auditoría"
etiqueta etl-mercadopublico FBCA04 "Issue 3: ETL Mercado Público y demo de falla"
etiqueta etl-bcch           B60205 "Issue 4: ETL Banco Central y cruce"
etiqueta kpis-entregables   5319E7 "Issue 5: KPIs, informe y presentación"
etiqueta bloqueante         D93F0B "Estoy bloqueado, necesito ayuda"
etiqueta duda               C5DEF5 "Pregunta técnica"
etiqueta bug                EE0701 "Algo que funcionaba dejó de funcionar"

hito() {
  if gh api "repos/$REPO/milestones" -f title="$1" -f due_on="$2" >/dev/null 2>&1; then
    echo "hito: $1"
  else
    echo "hito ya existía (o no se pudo crear): $1"
  fi
}
hito "Base lista"        "2026-10-24T23:59:00Z"
hito "ETL e integración" "2026-10-29T23:59:00Z"
hito "Entrega"           "2026-11-01T23:59:00Z"
hito "Presentación"      "2026-11-05T23:59:00Z"

crear() { # archivo etiqueta hito asignado
  local archivo="$DIR/$1" titulo url num
  titulo=$(head -n 1 "$archivo" | sed 's/^# //')
  url=$(gh issue create --repo "$REPO" --title "$titulo" --body-file "$archivo" --label "$2" --milestone "$3")
  num="${url##*/}"
  echo "creada #$num: $titulo"
  if gh issue edit "$num" --repo "$REPO" --add-assignee "$4" >/dev/null 2>&1; then
    echo "  asignada a @$4"
  else
    echo "  AVISO: no se pudo asignar a @$4 (¿ya aceptó la invitación?). Asígnala después en la web."
  fi
}
crear ISSUE_1_infraestructura.md           infraestructura    "Base lista"        "${EQUIPO[0]}"
crear ISSUE_2_modelo_de_datos.md           modelo-datos       "Base lista"        "${EQUIPO[1]}"
crear ISSUE_3_etl_mercado_publico.md       etl-mercadopublico "ETL e integración" "${EQUIPO[2]}"
crear ISSUE_4_etl_banco_central.md         etl-bcch           "ETL e integración" "${EQUIPO[3]}"
crear ISSUE_5_kpis_informe_presentacion.md kpis-entregables   "Entrega"           "${EQUIPO[4]}"

echo "Listo. Revisa en GitHub que quedaron como #1..#5."
