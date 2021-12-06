#!/usr/bin/env bash
export ENV_STATE=test
# pytest -vv
pytest --cov --cov-fail-under=80 -vv --cov-report html

