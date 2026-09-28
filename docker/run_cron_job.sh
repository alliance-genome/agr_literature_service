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
# "+2" makes tail print from line 2 onward, i.e. the whole log; anything
# non-numeric makes it fail outright. Fall back rather than trust it.
case "$TAIL_LINES" in
    '' | *[!0-9]*) TAIL_LINES=40 ;;
esac

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

# Print the line of a failed job's log that best explains the failure, cut to
# LAST_ERROR_CHARS so the record fits the alerter's Slack sample: the last Python
# exception line if there is one, else the last line logged at ERROR/CRITICAL,
# else nothing. Not simply the last line -- SQLAlchemy, for one, prints DETAIL,
# SQL and "(Background on this error ...)" lines after the exception.
# Double quotes become single ones so the value stays a single quoted field.
LAST_ERROR_CHARS=200
EXCEPTION_LINE='^[[:space:]]*[A-Za-z_][A-Za-z0-9_.]*(Error|Exception|Exit|Interrupt)(:|$)'
# Same forms the alerter treats as text-labelled errors.
LOGGED_ERROR_LINE=' - (ERROR|CRITICAL) - |\[(ERROR|CRITICAL)\]|(^|[[:space:]])(ERROR|CRITICAL):'
last_error() {
    local line
    line="$(grep -E "$EXCEPTION_LINE" "$1" 2>/dev/null | tail -n 1)"
    if [ -z "$line" ]; then
        line="$(grep -E "$LOGGED_ERROR_LINE" "$1" 2>/dev/null | tail -n 1)"
    fi
    printf '%s' "$line" | tr -d '\r' | tr '"' "'" | sed -e 's/^[[:space:]]*//' | cut -c "1-${LAST_ERROR_CHARS}"
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
# The emptiness check is separate: "" survives the substitution unchanged and
# would otherwise yield a log file called ".log" and a record reading "job= ".
SAFE_JOB_NAME="${JOB_NAME//[^A-Za-z0-9._-]/_}"
if [ -z "$JOB_NAME" ] || [ "$SAFE_JOB_NAME" != "$JOB_NAME" ]; then
    emit 2 "ABC-JOB-FAILED job=${SAFE_JOB_NAME} exit=64 host=${JOB_HOST} reason=bad-job-name" \
        "job name '${JOB_NAME}' contains unsupported characters"
    exit 0
fi

# LOG_PATH carries a trailing slash in docker-compose.yaml; stripping it is what
# makes the produced path byte-identical to the one the crontab used to hardcode.
LOG_DIR="${LOG_PATH:-/var/log/automated_scripts}"
LOG_DIR="${LOG_DIR%/}"
LOG_FILE="${LOG_DIR}/${JOB_NAME}.log"

# Establish the log file up front rather than letting the redirect below fail.
# If it cannot be written -- a bind mount gone read-only is the realistic case --
# bash would never exec the command and would return 1, which is indistinguishable
# from the job running and failing. Report the real reason and run anyway with the
# output discarded: a job silently not running is the worse outcome.
if ! mkdir -p "$LOG_DIR" 2>/dev/null || ! : >"$LOG_FILE" 2>/dev/null; then
    emit 2 "ABC-JOB-FAILED job=${JOB_NAME} exit=64 host=${JOB_HOST} reason=log-unwritable log=${LOG_FILE}" \
        "cannot write the job log; running with output discarded"
    LOG_FILE=/dev/null
fi

SECONDS=0
"$@" >"$LOG_FILE" 2>&1
STATUS=$?
DURATION=$SECONDS

if [ "$STATUS" -eq 0 ]; then
    emit 1 "ABC-JOB-OK job=${JOB_NAME} exit=0 duration=${DURATION}s host=${JOB_HOST}"
else
    # 128+N means killed by signal N -- an OOM kill (137) is a plausible failure
    # mode for the heavier ingest jobs, and the log file is usually empty in that
    # case, so name the signal rather than leaving a bare exit code.
    SIGNAL=""
    if [ "$STATUS" -gt 128 ] && [ "$STATUS" -lt 192 ]; then
        SIGNAL=" signal=$((STATUS - 128))"
    fi
    # The failure record is the only line written to fd 2. Every stderr line is
    # its own ERROR row in Athena, and the alerter groups rows by message, so a
    # 40-line tail there turned one failure into ~37 alert groups and pushed this
    # record past the group cap. Instead the record carries the error itself, and
    # the tail goes to fd 1 (INFO), where it still sits next to it in the viewer.
    LAST_ERROR=""
    if [ -s "$LOG_FILE" ]; then
        LAST_ERROR="$(last_error "$LOG_FILE")"
    fi
    emit 2 "ABC-JOB-FAILED job=${JOB_NAME} exit=${STATUS}${SIGNAL} duration=${DURATION}s host=${JOB_HOST} log=${LOG_FILE}${LAST_ERROR:+ last_error=\"${LAST_ERROR}\"}"
    if [ -s "$LOG_FILE" ]; then
        # Guard on the result, not on the file: a tail that fails or prints
        # nothing would otherwise put a blank line on the stream.
        TAIL_TEXT="$(tail -n "$TAIL_LINES" "$LOG_FILE" 2>/dev/null | sed -e "s|^|${JOB_NAME}\| |")"
        if [ -n "$TAIL_TEXT" ]; then
            emit 1 "$TAIL_TEXT"
        fi
    fi
fi

exit 0
