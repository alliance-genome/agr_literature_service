"""The conversion worker is deployed next to the api and shares its job state (SCRUM-6998).

The api queues on-demand conversion jobs in CONVERSION_JOB_STATE_DIR and the
conversion_worker service runs them, so both must mount the same volume there,
and every deployment of the api must restart the worker (it runs the same code).
"""
import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
STATE_DIR = "/var/lib/abc_conversion_jobs"
STATE_VOLUME = "${CONVERSION_JOB_STATE_PATH:-agr-literature-conversion-jobs}"


def _compose():
    with open(REPO / "docker-compose.yaml") as handle:
        return yaml.safe_load(handle)


def _make_recipe(target):
    """The recipe lines of a Makefile target, plus its prerequisites line."""
    text = (REPO / "Makefile").read_text()
    match = re.search(rf"^{re.escape(target)}:(.*)\n((?:\t.*\n)*)", text, re.MULTILINE)
    assert match, f"no {target} target in the Makefile"
    return match.group(1) + "\n" + match.group(2)


def test_worker_service_runs_the_worker_module_from_the_api_image():
    worker = _compose()["services"]["conversion_worker"]
    assert worker["build"]["dockerfile"] == "docker/api.dockerfile"
    assert worker["command"] == ["python3", "-m", "agr_literature_service.api.utils.conversion_worker"]
    assert worker["restart"] == "always"


def test_api_and_worker_share_the_job_state_volume():
    compose = _compose()
    for name in ("api", "conversion_worker"):
        service = compose["services"][name]
        assert f"{STATE_VOLUME}:{STATE_DIR}" in service["volumes"], name
        assert service["environment"]["CONVERSION_JOB_STATE_DIR"] == STATE_DIR, name
    assert "agr-literature-conversion-jobs" in compose["volumes"]


def test_every_api_deployment_restarts_the_worker():
    worker_restart = _make_recipe("restart-conversion-worker")
    for verb in ("build --no-cache conversion_worker", "rm -s -f conversion_worker",
                 "up -d conversion_worker"):
        assert verb in worker_restart
    for target in ("restart-api", "restart-api-and-automated-scripts"):
        assert "restart-conversion-worker" in _make_recipe(target).splitlines()[0], target


def test_migrations_stop_the_worker_with_the_api():
    script = (REPO / "apply_alembic_migration.sh").read_text()
    assert script.count("rm -s -f api conversion_worker") == 2
