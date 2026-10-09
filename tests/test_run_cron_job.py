"""Tests for docker/run_cron_job.sh, the automated_scripts cron wrapper (SCRUM-6248).

The wrapper keeps each job's full output in ${LOG_PATH}/<job>.log and emits a
short record onto the container's own streams so the Docker GELF driver ships
it to logs.alliancegenome.org -> S3 -> Athena. The severity Athena records is
derived from the stream alone, so a failure record has to land on fd 2.

CRON_LOG_STREAM_DIR stands in for /proc/1/fd here.
"""

import re
import subprocess
from pathlib import Path

import pytest

WRAPPER = Path(__file__).resolve().parents[1] / "docker" / "run_cron_job.sh"


@pytest.fixture
def run_wrapper(tmp_path):
    """Run the wrapper with isolated log and stream directories.

    Returns a callable taking the wrapper's argv plus optional extra env, and
    giving back (completed_process, stdout_record, stderr_record, log_dir).
    """
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    stream_dir = tmp_path / "streams"
    stream_dir.mkdir()
    stdout_file = stream_dir / "1"
    stderr_file = stream_dir / "2"
    stdout_file.touch()
    stderr_file.touch()

    def _run(args, env_extra=None, stream_dir_override=None):
        env = {
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "LOG_PATH": str(log_dir),
            "CRON_LOG_STREAM_DIR": stream_dir_override or str(stream_dir),
        }
        env.update(env_extra or {})
        proc = subprocess.run(
            ["bash", str(WRAPPER), *args],
            env=env,
            capture_output=True,
            text=True,
        )
        return proc, stdout_file.read_text(), stderr_file.read_text(), log_dir

    return _run


def test_success_writes_log_file_and_ok_record(run_wrapper):
    proc, out, err, log_dir = run_wrapper(
        ["selftest_ok", "bash", "-c", "echo hello; echo also-stderr >&2"]
    )

    assert proc.returncode == 0
    # The job's full output still goes to the per-job file, both streams merged.
    log_text = (log_dir / "selftest_ok.log").read_text()
    assert "hello" in log_text
    assert "also-stderr" in log_text
    # The heartbeat goes to fd 1, so Athena records it as INFO.
    assert "ABC-JOB-OK job=selftest_ok exit=0" in out
    assert "duration=" in out
    assert err == ""


def test_failure_record_goes_to_stderr_and_tail_to_stdout(run_wrapper):
    proc, out, err, log_dir = run_wrapper(
        ["selftest_fail", "bash", "-c", "echo boom >&2; exit 3"]
    )

    # Exit 0 so a failing job does not also fail the cron entry.
    assert proc.returncode == 0
    # fd 2 -> GELF level 3 -> level_name='ERROR' in Athena.
    assert "ABC-JOB-FAILED job=selftest_fail exit=3" in err
    assert f"log={log_dir}/selftest_fail.log" in err
    # The record is the ONLY line on fd 2. Every stderr line is an ERROR row, and
    # a 40-line tail there became ~37 alert groups per failure, pushing the record
    # itself past the alerter's group cap. The tail goes to fd 1 (INFO) instead,
    # where it still sits next to the record in the viewer.
    assert len(err.splitlines()) == 1
    assert "selftest_fail| boom" in out


def test_failure_record_names_the_last_exception(run_wrapper):
    # Shape of the real 2026-09-27 pubmed_update_references_by_doi failure:
    # SQLAlchemy prints DETAIL / SQL / Background lines AFTER the exception line,
    # so "the last line" would be the useless "(Background on this error ...)".
    script = r"""
cat <<'LOG'
2026-09-27 17:14:50,100 - root - INFO - Adding PMID to papers (20) with_DOI:
Traceback (most recent call last):
  File "/usr/src/app/script.py", line 10, in <module>
    update_database()
sqlalchemy.exc.IntegrityError: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "idx_curie"
DETAIL:  Key (curie)=(PMID:28118817) already exists.

[SQL: INSERT INTO cross_reference (curie) VALUES (%(curie__0)s)]
(Background on this error at: https://sqlalche.me/e/20/gkpj)
LOG
exit 1
"""
    _proc, _out, err, _log_dir = run_wrapper(["selftest_exc", "bash", "-c", script])

    record = err.strip()
    assert record.startswith("ABC-JOB-FAILED job=selftest_exc exit=1")
    # Double quotes are swapped for single ones so the value stays one field.
    assert record.endswith(
        ' last_error="sqlalchemy.exc.IntegrityError: (psycopg2.errors.UniqueViolation) '
        "duplicate key value violates unique constraint 'idx_curie'\""
    )


def test_failure_record_falls_back_to_last_logged_error(run_wrapper):
    # A job that logs an error and exits non-zero without a traceback.
    script = "echo '2026-09-27 06:01:10,144 - __main__ - ERROR - Failed to query PubMed for XB'; echo done; exit 1"
    _proc, _out, err, _log_dir = run_wrapper(["selftest_logged", "bash", "-c", script])

    assert err.strip().endswith(
        'last_error="2026-09-27 06:01:10,144 - __main__ - ERROR - Failed to query PubMed for XB"'
    )


def test_last_error_is_truncated(run_wrapper):
    script = "python3 -c 'print(\"ValueError: \" + \"x\" * 1000)'; exit 1"
    _proc, _out, err, _log_dir = run_wrapper(["selftest_long", "bash", "-c", script])

    last_error = err.strip().split(' last_error="', 1)[1].removesuffix('"')
    # Keeps the record inside the alerter's 400-character Slack sample.
    assert len(last_error) == 200
    assert last_error.startswith("ValueError: xxx")


def test_last_error_ignores_exceptions_before_the_tail(run_wrapper):
    # An exception the job logged and recovered from long before it failed for
    # another reason must not be reported as the cause.
    script = (
        "echo 'ValueError: recovered from this one'; seq 1 60; "
        "echo '2026-09-27 06:01:10,144 - __main__ - ERROR - the real problem'; exit 1"
    )
    _proc, _out, err, _log_dir = run_wrapper(["selftest_stale", "bash", "-c", script])

    assert err.strip().endswith('last_error="2026-09-27 06:01:10,144 - __main__ - ERROR - the real problem"')


def test_signal_kill_has_no_last_error(run_wrapper):
    # A killed process never prints its own exception, so any exception line in
    # its log is from before the kill and would name the wrong cause.
    script = "echo 'ValueError: handled earlier'; kill -9 $$"
    _proc, _out, err, _log_dir = run_wrapper(["selftest_oom", "bash", "-c", script])

    assert "exit=137 signal=9" in err
    assert " last_error=" not in err


def test_last_error_found_in_log_with_binary_bytes(run_wrapper):
    # GNU grep treats a file containing NUL as binary and prints no matching lines.
    script = r"printf 'stray\000bytes\nValueError: boom\n'; exit 1"
    _proc, _out, err, _log_dir = run_wrapper(["selftest_nul", "bash", "-c", script])

    assert err.strip().endswith('last_error="ValueError: boom"')
    # bash >= 4.4 warns "ignored null byte in input" when a captured command
    # substitution contains one; the wrapper must strip them in the pipe instead.
    assert "null byte" not in _proc.stderr


def test_last_error_does_not_end_in_backslash(run_wrapper):
    # A value ending in a backslash reads as an escaped closing quote to a parser
    # that honours escapes. Here the 200-character cut lands on the backslash.
    script = "python3 -c 'print(\"ValueError: \" + \"x\" * 187 + \"\\\\tail\")'; exit 1"
    _proc, _out, err, _log_dir = run_wrapper(["selftest_bs", "bash", "-c", script])

    record = err.strip()
    assert record.endswith('x"')
    assert not record.endswith('\\"')


def test_no_last_error_when_nothing_looks_like_one(run_wrapper):
    _proc, _out, err, _log_dir = run_wrapper(["selftest_plain", "bash", "-c", "seq 1 3; exit 2"])

    assert "ABC-JOB-FAILED job=selftest_plain exit=2" in err
    assert " last_error=" not in err


def test_failure_tail_is_bounded(run_wrapper):
    proc, out, _err, _log_dir = run_wrapper(
        ["selftest_tail", "bash", "-c", "seq 1 100; exit 1"],
        env_extra={"CRON_LOG_TAIL_LINES": "5"},
    )

    assert proc.returncode == 0
    tail_lines = [line for line in out.splitlines() if line.startswith("selftest_tail| ")]
    assert len(tail_lines) == 5
    assert tail_lines[-1] == "selftest_tail| 100"
    assert tail_lines[0] == "selftest_tail| 96"


def test_log_file_is_truncated_each_run(run_wrapper):
    run_wrapper(["selftest_trunc", "bash", "-c", "echo first-run"])
    _proc, _out, _err, log_dir = run_wrapper(
        ["selftest_trunc", "bash", "-c", "echo second-run"]
    )

    log_text = (log_dir / "selftest_trunc.log").read_text()
    assert "second-run" in log_text
    assert "first-run" not in log_text


def test_wrapper_does_not_mutate_the_job_environment(run_wrapper, tmp_path):
    """The wrapped job must see the environment the container configured.

    docker-compose.yaml:221 injects an exported HOST into automated_scripts, so
    a wrapper variable named HOST would be inherited by all 33 jobs with the
    container id in place of the configured value.
    """
    probe = tmp_path / "probe.sh"
    probe.write_text('env | grep -E "^HOST=" || echo "HOST unset"\n')

    _proc, _out, _err, log_dir = run_wrapper(
        ["envprobe", "bash", str(probe)],
        env_extra={"HOST": "https://literature.alliancegenome.org"},
    )

    assert (log_dir / "envprobe.log").read_text().strip() == \
        "HOST=https://literature.alliancegenome.org"


def test_missing_command_is_reported_not_silently_skipped(run_wrapper):
    proc, _out, err, _log_dir = run_wrapper(["selftest_only_name"])

    assert proc.returncode == 0
    assert "ABC-JOB-FAILED job=unknown exit=64" in err
    assert "reason=usage" in err
    # One ERROR row per failure; the explanation goes to fd 1.
    assert len(err.splitlines()) == 1
    assert "usage:" in _out


def test_unsafe_job_name_is_rejected(run_wrapper):
    proc, out, err, log_dir = run_wrapper(
        ["../escape", "bash", "-c", "echo should-not-run"]
    )

    assert proc.returncode == 0
    assert "reason=bad-job-name" in err
    assert len(err.splitlines()) == 1
    assert "unsupported characters" in out
    assert list(log_dir.iterdir()) == []


def test_trailing_slash_log_path_produces_the_same_file(tmp_path):
    """LOG_PATH is "/var/log/automated_scripts/" in production, with the slash.

    Stripping it is the single line that keeps the produced path byte-identical
    to the one the crontab used to hardcode, so pin the production value.
    """
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    stream_dir = tmp_path / "streams"
    stream_dir.mkdir()
    (stream_dir / "1").touch()
    (stream_dir / "2").touch()

    subprocess.run(
        ["bash", str(WRAPPER), "slashy", "bash", "-c", "echo hi"],
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "LOG_PATH": f"{log_dir}/",
            "CRON_LOG_STREAM_DIR": str(stream_dir),
        },
        capture_output=True,
        text=True,
    )

    assert [p.name for p in log_dir.iterdir()] == ["slashy.log"]


def test_empty_log_emits_no_blank_error_line(run_wrapper):
    proc, _out, err, _log_dir = run_wrapper(["quiet", "bash", "-c", "exit 9"])

    assert proc.returncode == 0
    assert "ABC-JOB-FAILED job=quiet exit=9" in err
    # A blank line on fd 2 becomes a content-free ERROR row in Athena.
    assert err.strip().splitlines() == [line for line in err.splitlines() if line.strip()]
    assert not err.endswith("\n\n")


def test_signal_kills_are_named(run_wrapper):
    _proc, _out, err, _log_dir = run_wrapper(["killed", "bash", "-c", "kill -9 $$"])

    # The log file is usually empty for a signal kill, so a bare exit code would
    # be the entire alert. 137 = 128 + SIGKILL.
    assert "exit=137 signal=9" in err


def test_unwritable_log_directory_runs_the_job_anyway(run_wrapper, tmp_path):
    """A read-only bind mount must not silently skip the job.

    Without the explicit check, bash cannot open the redirect target, never
    execs the command, and returns 1 - reported as an ordinary job failure.
    """
    # A mode-0500 directory would not block root, and CI runs the suite as root
    # in a container. A path under a regular file fails with ENOTDIR for everyone.
    not_a_dir = tmp_path / "regular-file"
    not_a_dir.write_text("")
    marker = tmp_path / "it-ran"

    proc, _out, err, _log_dir = run_wrapper(
        ["roJob", "bash", "-c", f"touch {marker}"],
        env_extra={"LOG_PATH": str(not_a_dir / "sub")},
    )

    assert proc.returncode == 0
    assert marker.exists(), "the job must still run when its log cannot be written"
    assert "reason=log-unwritable" in err
    assert len(err.splitlines()) == 1
    assert "cannot write the job log" in _out


def test_empty_job_name_is_rejected(run_wrapper):
    proc, _out, err, log_dir = run_wrapper(["", "bash", "-c", "echo nope"])

    assert proc.returncode == 0
    assert "reason=bad-job-name" in err
    assert len(err.splitlines()) == 1
    assert list(log_dir.iterdir()) == []


def test_falls_back_to_own_streams_when_pid1_unavailable(run_wrapper, tmp_path):
    proc, out, err, _log_dir = run_wrapper(
        ["selftest_fallback", "bash", "-c", "exit 4"],
        stream_dir_override=str(tmp_path / "does-not-exist"),
    )

    # A record we cannot ship must never fail the job, but it must not vanish
    # without trace either.
    assert proc.returncode == 0
    assert out == ""
    assert err == ""
    assert "ABC-JOB-FAILED job=selftest_fallback exit=4" in proc.stderr


# ------------------------------------------------------------------
# The crontab and the wrapper have to agree
# ------------------------------------------------------------------

CRONTAB = Path(__file__).resolve().parents[1] / "crontab"
WRAPPER_PATH = "/usr/local/bin/run_cron_job.sh"
JOB_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")

# The weekly prune is deliberately left unwrapped: its `&&` binding means
# wrapping it would change which command's output reaches the log file, and its
# output is not a job report anyone reads.
UNWRAPPED_PREFIXES = ("docker system prune",)


def _crontab_jobs():
    """Yield (schedule, command) for every sub-job in the crontab."""
    for line in CRONTAB.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" in line.split()[0]:
            continue
        fields = line.split(None, 5)
        schedule, rest = " ".join(fields[:5]), fields[5]
        for command in rest.split(";"):
            yield schedule, command.strip()


def test_every_cron_job_goes_through_the_wrapper():
    """The crontab was rewritten wholesale; keep it that way.

    A hand-edited line that reverts to a bare redirect would silently drop that
    job out of the alerting this wrapper exists to provide, and nothing else
    would notice.
    """
    unwrapped = [
        command for _schedule, command in _crontab_jobs()
        if not command.startswith(WRAPPER_PATH)
        and not command.startswith(UNWRAPPED_PREFIXES)
    ]

    assert unwrapped == [], f"cron entries bypassing the wrapper: {unwrapped}"


def test_wrapped_jobs_have_usable_distinct_names():
    names = [
        command.split()[1] for _schedule, command in _crontab_jobs()
        if command.startswith(WRAPPER_PATH)
    ]

    assert names, "expected the crontab to contain wrapped jobs"
    # A name the wrapper would reject means the job's output goes nowhere.
    assert [n for n in names if not JOB_NAME_RE.match(n)] == []
    # Two jobs sharing a name would share a log file and truncate each other.
    assert len(names) == len(set(names)), f"duplicate job names: {sorted(names)}"


def test_no_cron_entry_still_redirects_to_the_log_directory():
    """A leftover `> …log 2>&1` would send that job's output to a file only."""
    leftovers = [
        command for _schedule, command in _crontab_jobs()
        if "/var/log/automated_scripts/" in command
        and not command.startswith(UNWRAPPED_PREFIXES)
    ]

    assert leftovers == [], f"cron entries still redirecting by hand: {leftovers}"
