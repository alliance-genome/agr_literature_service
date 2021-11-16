#!/usr/bin/env bash
export ENV_STATE=test
# pytest -vv
cd backend/app
pytest --cov --cov-fail-under=80 -vv --cov-report html

cd ../../src/xml_processing
pytest
