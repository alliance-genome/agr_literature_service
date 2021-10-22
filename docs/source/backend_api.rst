API
===

Deploying the full stack
------------------------

*make sure you have docker and docker-compose installed*

The backend API stack has these dependencies

- `Postgres`_
- `ElasticSearch`_
- `Redis`_
- `RESTful`_

Spin up stack
^^^^^^^^^^^^^

To create an application container with your local changes run::

    make build-env
    make build-dev


and to start up all the components of the stack::

    docker-compose up


Building images
^^^^^^^^^^^^^^^

Create a base image that can be used to run ``pytest`` and ``flake8``, among other modules::

    make build-env

Create an application image::

    make build-app

Run tests/PEP8 adherence
^^^^^^^^^^^^^^^^^^^^^^^^

For now mainly ``flake8`` is being used::


    make run-flake8


Coverage
^^^^^^^^

This will calculate the code coverage for the API::

    make run-dev-bash
    pytest --cov --cov-fail-under=100





Backup and Restore
------------------

Backup
^^^^^^

Run the following command to create a backup of the database inside a development container
(by running ``make run-dev-bash``). The compressed file will show up in the root of the project::

    export PGPASSWORD=postgres
    pg_dump -h postgres -U postgres -p 5432 -Fc <database-name> > <database-name>.dump

Restore
^^^^^^^

After creating a blank database (with ``pgadmin`` or ``psql``, still inside the development container,
run the following command::

    pg_restore --clean --if-exists -d <newdb> -h postgres -U postgres -p 5432 < <database-name>.dump



Development
^^^^^^^^^^^

First create application image

Use ``docker-compose`` to spin up all the containers::

    docker-compose up

Develop and run application server in Docker::

    make run-dev-bash
    python src/server.py --port=<port>

Using curl on the endpoint
^^^^^^^^^^^^^^^^^^^^^^^^^^

Add a reference::

    curl http://localhost:<app port>/references/add/ -d 'data={"title": "Worms abi-1", "pubmed_id": "PMID:53e565", "mod": "WB", "pubmod_id": "WBPub:0e0000003"}' -X POST

Get a Reference::

    curl http://localhost:<app-port>/reference/PMID:4040


Production
^^^^^^^^^^

use ``--prod`` argument to use the WSGI server in production when running the application



--------------------

Current Makefile
^^^^^^^^^^^^^^^

.. code-block:: bash


    REG=100225593120.dkr.ecr.us-east-1.amazonaws.com
    TAG=latest

    login-ecr:
        docker run -v ~/.aws/credentials:/root/.aws/credentials --rm -it amazon/aws-cli ecr get-login-password | docker login --username AWS --password-stdin ${REG}

    build-env:
        docker build . \
            --build-arg REG=${REG} \
            --build-arg aws_secret_access_key=${AWS_SECRET_ACCESS_KEY} \
            --build-arg aws_access_key_id=${AWS_ACCESS_KEY_ID} \
            --build-arg okta_client_id=${OKTA_CLIENT_ID} \
            --build-arg okta_client_secret=${OKTA_CLIENT_SECRET} \
            -t ${REG}/agr_literature_env:${TAG} \
            -f ./docker/Dockerfile.env

    build-dev:
        docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_dev:${TAG} -f ./docker/Dockerfile.dev.env

    build-app:
        docker build . --build-arg REG=${REG} -t ${REG}/agr_literature_app:${TAG} -f ./docker/Dockerfile.app.env

    run-flake8:
        docker run --rm -v ${PWD}:/workdir -i ${REG}/agr_literature_dev:${TAG} /bin/bash -c "python3 -m flake8 ."

    run-dev-bash:
        docker run --rm \
            --network=agr_literature_service_agr-literature \
            -p ${API_PORT}:8080 \
            -v ${PWD}:/workdir \
            -t -i ${REG}/agr_literature_dev:${TAG} \
            /bin/bash

    run-dev-zsh:
        docker run --rm -v "${HOME}/.vimrc:/root/.vimrc:rw" -v "${HOME}/.zshrc:/root/.zshrc:rw" -v ${PWD}:/workdir -t -i ${REG}/agr_literature_dev:${TAG} /bin/zsh

    docker-compose-up:
        docker run -itd --env-file=.env -v /var/run/docker.sock:/var/run/docker.sock -v /home/core/.docker:/root/.docker -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml up -d

    docker-compose-down:
        docker run -itd --env-file=.env -v /var/run/docker.sock:/var/run/docker.sock -v /home/core/.docker:/root/.docker -v ${PWD}:/var/tmp/ docker/compose:1.24.1  -f /var/tmp/docker-compose.yaml down

    run-test-bash: build-env build-dev
        -docker volume rm agr_literature_service_agr-literature-test-pg-data
        docker-compose -f docker-compose-test.yml up -d
        sleep 5
        # Minus at start means ignore exit code for that line
        -docker run -it --rm \
            --network=agr_literature_service_agr-literature-test \
            -p 8080:8080 \
            -v ${PWD}:/workdir \
            ${REG}/agr_literature_dev:${TAG} \
            ./run_tests.sh
        docker-compose -f docker-compose-test.yml down


Current docker-compose file
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

    version: "3.7"

    services:
      postgres:
        container_name: agr-literature-pg
        image: postgres:13.1-alpine
        restart: always
        environment:
          POSTGRES_USER: ${PSQL_USERNAME}
          POSTGRES_PASSWORD: ${PSQL_PASSWORD}
          POSTGRES_DB: ${PSQL_DATABASE}
          POSTGRES_PORT: ${PSQL_PORT}
        volumes:
          - "agr-literature-pg-data:/var/lib/postgresql/data"
          - "./postgresql.conf:/etc/postgresql.conf"
        networks:
          - agr-literature
        healthcheck:
          test: ["CMD-SHELL", "pg_isready -U postgres"]
          interval: 10s
          timeout: 5s
          retries: 5

      pgadmin:
        container_name: agr-literature-pgadmin
        image: dpage/pgadmin4:5.1
        restart: always
        environment:
          PGADMIN_DEFAULT_EMAIL: ${PGADMIN_DEFAULT_EMAIL}
          PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_DEFAULT_PASSWORD}
          PGADMIN_ACCESS_CONTROL_ALLOW_ORIGIN: "*"
          PGADMIN_LISTEN_PORT: 81
        ports:
          - "8882:81"
        volumes:
          - "agr-literature-pgadmin-data:/var/lib/pgadmin"
        networks:
          - agr-literature
        depends_on:
          - postgres

      elasticsearch:
        container_name: agr-literature-es
        image: docker.elastic.co/elasticsearch/elasticsearch:7.10.1
        restart: always
        environment:
          - node.name=aws-literature-es1
          - cluster.name=es-docker-cluster
          - discovery.type=single-node
          - bootstrap.memory_lock=true
          - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
        ulimits:
          memlock:
            soft: -1
            hard: -1
        volumes:
          - "agr-literature-es-data:/usr/share/elasticsearch/data"
        ports:
          - 9201:9200
        networks:
          - agr-literature

    volumes:
      agr-literature-pg-data:
      agr-literature-redis-data:
      agr-literature-pgadmin-data:
      agr-literature-es-data:

    networks:
      agr-literature:





.. _Postgres: https://www.postgresql.org
.. _Elasticsearch: https://www.elastic.co/elasticsearch/
.. _Redis: https://redis.com
.. _RESTful: https://flask-restful.readthedocs.io/en/latest/quickstart.html