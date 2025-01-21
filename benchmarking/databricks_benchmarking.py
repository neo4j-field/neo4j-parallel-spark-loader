# Databricks notebook source
from benchmarking.utils.benchmark import generate_benchmarks

from benchmarking.utils.visualize import create_row_count_v_load_time_line_plot, create_load_v_total_time_plot, create_num_groups_v_time_bar_plot, create_time_v_row_count_for_graph_structure_line_plot

import os

# COMMAND ----------

for key, value in dbutils.widgets.getAll().items():
    os.environ[key] = value

# COMMAND ----------

results_df = generate_benchmarks("databricks", "downloaded")

# COMMAND ----------

results_df

# COMMAND ----------

create_row_count_v_load_time_line_plot(results_df)

# COMMAND ----------

create_time_v_row_count_for_graph_structure_line_plot(results_df, "bipartite")

# COMMAND ----------

create_num_groups_v_time_bar_plot(results_df, "load_time")

# COMMAND ----------

create_num_groups_v_time_bar_plot(results_df, "process_time")

# COMMAND ----------

create_num_groups_v_time_bar_plot(results_df, "total_time")

# COMMAND ----------

create_load_v_total_time_plot(results_df)

# COMMAND ----------


