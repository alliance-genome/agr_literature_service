"""Which commit this image was built from (SCRUM-6878).

The api and automated_scripts dockerfiles read the commit from the build
context's .git into BUILD_INFO_DIR, so it is known however the image is built.
GIT_SHA and BUILD_DATE, which the Makefile passes to `docker compose build`,
take precedence: they also mark a checkout with local changes as "-dirty".
Anything not found is reported as "unknown" rather than guessed.
"""
import os
from pathlib import Path
from typing import Dict

UNKNOWN = "unknown"
BUILD_INFO_DIR = Path("/etc/abc_build_info")


def _lookup(env_name: str, file_name: str) -> str:
    value = os.environ.get(env_name, "").strip()
    if value and value != UNKNOWN:
        return value
    try:
        value = (BUILD_INFO_DIR / file_name).read_text().strip()
    except OSError:
        value = ""
    return value or UNKNOWN


def get_build_info() -> Dict[str, str]:
    return {
        "commit": _lookup("GIT_SHA", "commit"),
        "build_date": _lookup("BUILD_DATE", "build_date"),
    }
