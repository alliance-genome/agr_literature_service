#!/usr/bin/env bash
export ENV_STATE=test
# pytest -vv
sleep 30
pytest -m "not webtest"

