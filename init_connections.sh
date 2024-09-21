#!/usr/bin/env bash
# This script initilizes the connections needed by Airflow.

source .env

echo "Create AWS connection in Airflow..."

# Typical config: CONN_EXTRA='{ "aws_iam_role": "my_airflow_role", "region_name": "eu-west-1" }'
# An anonymous connection is enough here:
CONN_EXTRA='{"config_kwargs": {"signature_version": "unsigned"}}'
docker exec airflow-webserver airflow connections add 'aws' \
        --conn-extra "$CONN_EXTRA" \
        --conn-type aws


echo "Create Postgres connection in Airflow..."

docker exec airflow-webserver airflow connections add 'pg' \
        --conn-type postgres \
        --conn-host "postgres-dwh" \
        --conn-login "algolia" \
        --conn-password "${_DWH_POSTGRES_PASSWORD:-password}" \
        --conn-port "5432"