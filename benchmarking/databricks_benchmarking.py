# Databricks notebook source
import os

from benchmarking.utils.benchmark import generate_benchmarks

# COMMAND ----------

dbutils.widgets.text("NEO4J_USERNAME", "neo4j", "Neo4j user")
dbutils.widgets.text("NEO4J_PASSWORD", "password", "Neo4j password")
dbutils.widgets.text(
    "NEO4J_URI", "neo4j+s://<aura_instance_id>.databases.neo4j.io", "Neo4j URI"
)
dbutils.widgets.text("NEO4J_DATABASE", "neo4j", "Neo4j database")


# COMMAND ----------

for key, value in dbutils.widgets.getAll().items():
    os.environ[key] = value

# COMMAND ----------

results_df = generate_benchmarks("databricks", "downloaded")

# COMMAND ----------
