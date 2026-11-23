"""Write a failure record onto the container's stderr so it reaches Athena.

A cron job's own output goes only to its log file (see docker/run_cron_job.sh),
and run_cron_job.sh reports a job only as a whole: a job that handles a failure
and exits 0 is logged as ABC-JOB-OK. A job that skips part of its work (one MOD's
DQM file, say) calls emit_error_record so that part is searchable too. The line
goes to PID 1's fd 2, which the Docker GELF driver ships as level_name='ERROR',
the same way run_cron_job.sh writes ABC-JOB-FAILED.
"""
import socket
import sys
from os import environ, path

MAX_ERROR_CHARS = 200


def _quoted(value):
    """One line, double quotes made single and cut to MAX_ERROR_CHARS, so the
    value stays a single quoted field, as run_cron_job.sh does for last_error."""
    text = " ".join(str(value).split()).replace('"', "'")[:MAX_ERROR_CHARS]
    return '"' + text.rstrip("\\") + '"'


def format_error_record(record_type, error=None, **fields):
    """'<record_type> key=value ... host=<host> error="<error>"'"""
    parts = [record_type]
    parts += [f"{key}={'_'.join(str(value).split())}" for key, value in fields.items()]
    parts.append(f"host={socket.gethostname()}")
    if error is not None:
        parts.append(f"error={_quoted(error)}")
    return " ".join(parts)


def emit_error_record(record_type, error=None, **fields):
    """Write one record line to the container's stderr. Falls back to our own
    stderr (the job log, under cron) when that is not writable, e.g. outside a
    container. A record that cannot be shipped must never fail the job."""
    line = format_error_record(record_type, error, **fields)
    # Overridable, as in run_cron_job.sh, so tests can use a temporary directory.
    target = path.join(environ.get("CRON_LOG_STREAM_DIR", "/proc/1/fd"), "2")
    try:
        with open(target, "a") as stream:
            stream.write(line + "\n")
        return line
    except OSError:
        pass
    try:
        sys.stderr.write(line + "\n")
    except Exception:
        pass
    return line
