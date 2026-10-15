#!/usr/bin/env bash
#
# Archive a folder of a MOD's existing main PDFs in ABC WITHOUT sending the
# papers to text conversion (SCRUM-6633), one file at a time through
# POST /reference/referencefile/pdf_archive_upload/.
#
# Each file must be named after the MOD's ID for the paper, e.g.
# FBrf0111489.pdf (FB), WBPaper00031514.pdf (WB), XB-ART-59369.pdf (XB).
#
# Usage:
#   pdf_archive_upload.sh <folder> <MOD> [--dry-run] [--include-existing] [--stage]
#
#   --dry-run           report what would happen; nothing is written
#   --include-existing  also upload to papers that already have a different
#                       main PDF of the MOD (skipped by default)
#   --stage             use the stage API instead of production
#
# Authentication: log in to the ABC literature UI, choose "copy access token"
# in the user menu, then run the script; it asks for the token (or set
# ABC_TOKEN). The token expires after a while: the script then stops, and
# running the same command again with a new token carries on where it stopped.
#
# Every response is appended to <MOD>_pdf_archive_<env>[_dry_run].tsv in the
# current directory (file, HTTP code, JSON result), where <env> is production,
# stage, or custom (ABC_API set). Files with a 200 there are skipped on a rerun;
# anything else is retried. Each environment has its own file, so a stage run
# never makes a production run skip files. Needs only bash and curl.

set -u

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

[[ $# -lt 2 ]] && usage
FOLDER=$1
MOD=$2
shift 2
DRY_RUN=false
INCLUDE_EXISTING=false
API="https://literature-rest.alliancegenome.org"
ENV_NAME=production
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --include-existing) INCLUDE_EXISTING=true ;;
    --stage) API="https://stage-literature-rest.alliancegenome.org"; ENV_NAME=stage ;;
    *) usage ;;
  esac
done
if [[ -n "${ABC_API:-}" ]]; then
  API=$ABC_API
  ENV_NAME=custom
fi

if [[ ! -d "$FOLDER" ]]; then
  echo "ERROR: folder '$FOLDER' does not exist." >&2
  exit 1
fi

if [[ -z "${ABC_TOKEN:-}" ]]; then
  read -r -s -p "Paste your ABC access token: " ABC_TOKEN
  echo
fi

RESULTS="${MOD}_pdf_archive_${ENV_NAME}.tsv"
[[ "$DRY_RUN" == true ]] && RESULTS="${MOD}_pdf_archive_${ENV_NAME}_dry_run.tsv"
touch "$RESULTS"

URL="${API}/reference/referencefile/pdf_archive_upload/?mod_abbreviation=${MOD}&dry_run=${DRY_RUN}&include_existing=${INCLUDE_EXISTING}"
echo "Sending PDFs in $FOLDER to $API as $MOD (dry run: $DRY_RUN); results in $RESULTS"

done_files=$(awk -F'\t' '$2 == 200 {print $1}' "$RESULTS")
sent=0
skipped=0
failed=0
for path in "$FOLDER"/*.pdf "$FOLDER"/*.PDF; do
  [[ -f "$path" ]] || continue
  name=$(basename "$path")
  if grep -qxF "$name" <<< "$done_files"; then
    skipped=$((skipped + 1))
    continue
  fi
  # The token goes in on stdin so it never shows in the process list. A stalled
  # transfer gives up after 10 minutes; a resend is safe (already_loaded).
  response=$(curl -s --max-time 600 --retry 2 -w $'\n%{http_code}' -X POST "$URL" \
    -H 'accept: application/json' \
    -H @- \
    -F "file=@\"${path}\";type=application/pdf" <<< "Authorization: Bearer ${ABC_TOKEN}")
  http_code=$(tail -n1 <<< "$response")
  body=$(sed '$d' <<< "$response" | tr -d '\n\t')
  printf '%s\t%s\t%s\n' "$name" "$http_code" "$body" >> "$RESULTS"
  if [[ "$http_code" == 401 ]]; then
    echo "Token rejected or expired at $name. Copy a new access token and run the same command again." >&2
    exit 2
  fi
  [[ "$http_code" == 200 ]] || failed=$((failed + 1))
  sent=$((sent + 1))
  (( sent % 100 == 0 )) && echo "$sent sent ($failed failed)"
done

echo "Done: $sent sent, $skipped already done in an earlier run, $failed failed (non-200, retried on a rerun)."
echo "Latest result per file in $RESULTS, by status:"
awk -F'\t' '{latest[$1] = $3} END {for (f in latest) print latest[f]}' "$RESULTS" \
  | grep -o '"status": *"[a-z_]*"' | tr -d ' ' | sort | uniq -c
