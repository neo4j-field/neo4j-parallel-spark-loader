# Databricks notebook source
from benchmarking.utils.benchmark import generate_benchmarks

import os

# COMMAND ----------

for key, value in dbutils.widgets.getAll().items():
    os.environ[key] = value

# COMMAND ----------

results_df = generate_benchmarks("databricks", "downloaded")

# COMMAND ----------

results_df

# COMMAND ----------


