#!/bin/bash
#
# Wrapper for the automated_scripts crontab entries (SCRUM-6248).
#
# Runs one cron job, keeps its full output in the per-job log file under
# ${LOG_PATH} exactly as before (so ${LOG_URL} links in the report emails keep
# working, and the file is truncated on each run), and additionally emits a
# short, machine-readable record onto the CONTAINER's stdout/stderr so it
# reaches the Docker GELF driver -> logs.alliancegenome.org -> S3/Athena.
#
# Two details make the explicit /proc/1/fd/* writes necessary:
#
#   * A cron child does not inherit PID 1's stdout -- cron daemonises, so its
#     children's output goes to cron's mail/discard path.  PID 1 here is the
#     container's `bash -c`, whose streams the GELF driver captures.
#   * The GELF driver derives the severity from the stream alone (stdout -> 6
#     "INFO", stderr -> 3 "ERROR").  A failure must therefore be written to
#     fd 2 to be searchable as level_name='ERROR' in Athena.
#
# Usage: run_cron_job.sh <job-name> <command> [args...]
#
# Always exits 0, so a failing job does not also fail the cron entry.

set -u

TAIL_LINES="${CRON_LOG_TAIL_LINES:-40}"

# Directory holding PID 1's file descriptors.  Overridable so the tests can
# point it at a temporary directory instead of the real container's streams.
STREAM_DIR="${CRON_LOG_STREAM_DIR:-/proc/1/fd}"

# Write to the container's stdout (fd 1) or stderr (fd 2) so the GELF driver
# picks the line up.  Falls back to our own stream if that is not writable:
# a record we cannot ship must never fail the job.
emit() {
    local fd="$1"
    shift
    local target="${STREAM_DIR}/${fd}"
    if [ -w "$target" ] && printf '%s\n' "$@" >>"$target" 2>/dev/null; then
        return 0
    fi
    if [ "$fd" = "2" ]; then
        printf '%s\n' "$@" >&2
    else
        printf '%s\n' "$@"
    fi
}

# Not HOST: docker-compose.yaml:221 injects an exported HOST into this
# container, and reassigning it would pass the mutated value down to every
# wrapped job. HOSTNAME is set by bash itself.
JOB_HOST="${HOSTNAME:-$(hostname)}"

if [ "$#" -lt 2 ]; then
    emit 2 "ABC-JOB-FAILED job=unknown exit=64 host=${JOB_HOST} reason=usage" \
        "usage: $(basename "$0") <job-name> <command> [args...]"
    exit 0
fi

JOB_NAME="$1"
shift

# The job name becomes a filename and a line prefix; keep it to safe characters.
SAFE_JOB_NAME="${JOB_NAME//[^A-Za-z0-9._-]/_}"
if [ "$SAFE_JOB_NAME" != "$JOB_NAME" ]; then
    emit 2 "ABC-JOB-FAILED job=${SAFE_JOB_NAME} exit=64 host=${JOB_HOST} reason=bad-job-name" \
        "job name '${JOB_NAME}' contains unsupported characters"
    exit 0
fi

LOG_DIR="${LOG_PATH:-/var/log/automated_scripts}"
LOG_DIR="${LOG_DIR%/}"
LOG_FILE="${LOG_DIR}/${JOB_NAME}.log"

mkdir -p "$LOG_DIR" 2>/dev/null || true

SECONDS=0
"$@" >"$LOG_FILE" 2>&1
STATUS=$?
DURATION=$SECONDS

if [ "$STATUS" -eq 0 ]; then
    emit 1 "ABC-JOB-OK job=${JOB_NAME} exit=0 duration=${DURATION}s host=${JOB_HOST}"
else
    emit 2 "ABC-JOB-FAILED job=${JOB_NAME} exit=${STATUS} duration=${DURATION}s host=${JOB_HOST} log=${LOG_FILE}"
    if [ -s "$LOG_FILE" ]; then
        emit 2 "$(tail -n "$TAIL_LINES" "$LOG_FILE" | sed -e "s|^|${JOB_NAME}\| |")"
    fi
fi

exit 0
