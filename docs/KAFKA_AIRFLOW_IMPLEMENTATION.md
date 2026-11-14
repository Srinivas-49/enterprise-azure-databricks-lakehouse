# Kafka and Airflow integration scope

The repository contains `integrations/kafka_orders.py` and `orchestration/airflow_dag.py`. The Kafka component supports bounded CSV landing and commits offsets only after local persistence; downstream data processing deduplicates on order ID. The Airflow DAG schedules the local transformation with retries and one active run at a time.

The local test `tests/test_kafka_landing.py` exercises Kafka-adapter data contracts, duplicate handling and quarantine using synthetic events. **Four Lakehouse tests passed locally** after adding this integration.

Neither the Kafka consumer nor an Airflow scheduler has been verified against a live broker or scheduler. Deploying these requires kafka-python / Apache Airflow, compatible infrastructure, credentials and integration validation. The Azure Databricks notebook is also an adapter example, not proof of an active cloud deployment.
