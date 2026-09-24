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


def test_failure_record_and_tail_go_to_stderr(run_wrapper):
    proc, out, err, log_dir = run_wrapper(
        ["selftest_fail", "bash", "-c", "echo boom >&2; exit 3"]
    )

    # Exit 0 so a failing job does not also fail the cron entry.
    assert proc.returncode == 0
    # fd 2 -> GELF level 3 -> level_name='ERROR' in Athena.
    assert "ABC-JOB-FAILED job=selftest_fail exit=3" in err
    assert f"log={log_dir}/selftest_fail.log" in err
    assert "selftest_fail| boom" in err
    assert out == ""


def test_failure_tail_is_bounded(run_wrapper):
    proc, _out, err, _log_dir = run_wrapper(
        ["selftest_tail", "bash", "-c", "seq 1 100; exit 1"],
        env_extra={"CRON_LOG_TAIL_LINES": "5"},
    )

    assert proc.returncode == 0
    tail_lines = [line for line in err.splitlines() if line.startswith("selftest_tail| ")]
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


def test_unsafe_job_name_is_rejected(run_wrapper):
    proc, _out, err, log_dir = run_wrapper(
        ["../escape", "bash", "-c", "echo should-not-run"]
    )

    assert proc.returncode == 0
    assert "reason=bad-job-name" in err
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


def test_empty_job_name_is_rejected(run_wrapper):
    proc, _out, err, log_dir = run_wrapper(["", "bash", "-c", "echo nope"])

    assert proc.returncode == 0
    assert "reason=bad-job-name" in err
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
