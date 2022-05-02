docker-compose --env-file .env.test build api
docker-compose --env-file .env.test build test_runner
make run-functest
docker-compose --env-file .env.test run test_runner /bin/bash
