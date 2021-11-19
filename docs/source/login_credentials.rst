Generating login credentials
============================

*(must repeat every 12 hours to access base linux image and neo4j env image)*

- Make sure you have `AWS-CLI`_ installed locally
- Make sure you have AWS login credentials for the ``agr_aws`` account, with the permissions  **AWS group for ECR access**
- Create a ``~/.aws/config`` file with the following contents::

        [default]
        region=us-east-1

- Create a ``~/.aws/credentials`` file with the following content::

        [default]
        aws_access_key_id = <your access key goes here>
        aws_secret_access_key = <your secret access key goes here>

- To test that your credentials are working correctly, run the command below and verify if a token is produced.::

        aws ecr get-login-password

- Touch ``~/.docker/config.json``

- Run this command to push the credentials generated into ``config.json``::

        aws ecr get-login-password | docker login --username AWS --password-stdin 100225593120.dkr.ecr.us-east-1.amazonaws.com

- Verify that you can pull the neo4j env image::

        docker pull 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_neo4j_env:4.0.0

- Proceed with the appropriate ``make`` commands as usual


*Reminder: this process needs to be repeated every time you get an error like this (usually ~ every 12 hours)*::

        Error response from daemon: pull access denied for
        100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_neo4j_env,
        repository does not exist or may require
        'docker login': denied: Your authorization token has expired. Reauthenticate and try again.



.. _AWS-CLI: https://aws.amazon.com/cli/