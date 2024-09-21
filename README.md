# Airflow DAG: ETL pipeline for Shopify configs.

**This repo contains:**
- A docker-compose config to allow you to get Airflow up and running locally in Docker.
- An Airflow DAG (`load_shopify_configs`) to orchestrate the Shopify configs ETL pipeline
- Unit tests to check the DAG.

**DAG behaviour**:

The `load_shopify_configs` DAG is a daily DAG, triggered at 3.00 UTC every day. 
For the `yyyy-mm-dd` execution date, this DAG will run the following tasks:

1. `wait_csv_on_s3` task: Wait for `yyyy-mm-dd.csv` file to be available on `alg-data-public` S3 bucket. After 12h, this task will fail.
2. `delete_existing_data` task: In Postgres DWH, delete the existing data matching with the current execution date (loaded by a previous dagrun).
This task is for idempotency, which is necessary for backfilling.
3. `process_csv task`:
- Extract the content of the CSV file from S3
- Tranform the data (clean and enrich)
- Load the transformed data in the `shopify_configs` table of the Postgres DWH.

Note that the Extract, Tranform and Load operations are grouped in a single Airflow task to avoid data transfers between tasks using XCOM.


## Run Airflow locally

For dev and debug, you can run Airflow locally and execute the DAG:

        # Set your host user id in the .env file
        echo -e "AIRFLOW_UID=$(id -u)" > .env
        
        # Optionally, set custom credentials for DWH DB, Airflow DB and Airflow UI.
        echo -e "_DWH_POSTGRES_PASSWORD=password" >> .env
        echo -e "_AIRFLOW_POSTGRES_PASSWORD=airflow" >> .env
        echo -e "_AIRFLOW_WWW_USER_PASSWORD=airflow" >> .env
        
        # Initialize Airflow DB once
        docker compose up airflow-init
        
        # Start all services in background
        docker compose up -d
        
        # Wait for all the services status to be healthy (it may take a few minutes)
        watch -d 'docker compose ps --format "{{.Service}} {{.Status}}"'
        
        # Once all services are healthy, create the Airflow connections to AWS and Postgres DWH
        ./init_connections.sh
        
You can then connect to Airflow UI on http://localhost:8080.
From the UI, activate the `load_shopify_configs` DAG to start the pipelines.

To connect to the DWH DB for debugging, you need to have psql client installed and run:

        psql -h 0.0.0.0 -U user -d algolia
        # The default password is: password


To clean everything, run this command and then rerun the local run commands above:
        
        docker compose down --volumes --remove-orphans



## Unit Tests

To run the unit tests, execute these commands:

        # Create a virtual env
        python -m venv airflow
        source airflow/bin/activate
        
        # Install all the test dependencies
        pip install -r requirements-test.txt
        
        # Run the tests
        pytest -v
        
## Limitations and next steps

- To reduce the size of the containers, a slim Airflow docker image could be used and extended.
- Setting up a Dev Container would eliminate the need for developers to manually create virtual environments for unit tests.
- To enable having different behaviours in Prod and Dev, environment-specific configs should be used. Some parameters of the DAG should be loaded from these configs.
- To scale and process much bigger files, we could consider using a Spark job, for example with the AwsGlueJobOperator operator.
- To improve code quality, we should set up a linter, pre-commit hooks and CICD pipelines to validate and deploy DAGs.
- Monitoring and alerting on DAG's failures would be needed in production.
- To improve security, AWS SecretsManager could be used as Airflow backend to store connections with credentials.