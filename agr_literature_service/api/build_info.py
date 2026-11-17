"""Which commit this image was built from (SCRUM-6878).

The Makefile passes GIT_SHA and BUILD_DATE to `docker compose build`, and the
api and automated_scripts dockerfiles turn them into environment variables.
An image built some other way reports "unknown" rather than guessing.
"""
import os
from typing import Dict

UNKNOWN = "unknown"


def get_build_info() -> Dict[str, str]:
    return {
        "commit": os.environ.get("GIT_SHA") or UNKNOWN,
        "build_date": os.environ.get("BUILD_DATE") or UNKNOWN,
    }
