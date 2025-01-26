# Databricks notebook source
from benchmarking.utils.spark import (
    create_spark_session
)
from benchmarking.data.remote_datasets import (
    get_amazon_ratings_bipartite_spark_dataframe,
    get_reddit_threads_predefined_components_spark_dataframe,
    get_twitch_gamers_monopartite_spark_dataframe
)
from neo4j_parallel_spark_loader.visualize.heatmap import *
from neo4j_parallel_spark_loader import bipartite, monopartite, predefined_components

# COMMAND ----------

spark_session = create_spark_session()

#bp_sdf = get_amazon_ratings_bipartite_spark_dataframe(spark_session)
mp_sdf = get_twitch_gamers_monopartite_spark_dataframe(spark_session)
#pc_sdf = get_reddit_threads_predefined_components_spark_dataframe(spark_session)

# COMMAND ----------

bp_sdf_5 = bipartite.group_and_batch_spark_dataframe(bp_sdf, 'source', 'target', 5)

# COMMAND ----------

create_ingest_heatmap(bp_sdf_5, "Bipartite 5 Groups")

# COMMAND ----------

bp_sdf_20 = bipartite.group_and_batch_spark_dataframe(bp_sdf, 'source', 'target', 20)

# COMMAND ----------

create_ingest_heatmap(bp_sdf_20, "Bipartite 20 Groups", [25, 20])

# COMMAND ----------

mp_sdf_9 = monopartite.group_and_batch_spark_dataframe(mp_sdf, 'source', 'target', 9)

# COMMAND ----------

create_ingest_heatmap(mp_sdf_9, "Monopartite 9 Groups", [12, 9])

# COMMAND ----------

mp_sdf_10 = monopartite.group_and_batch_spark_dataframe(mp_sdf, 'source', 'target', 10)

# COMMAND ----------

create_ingest_heatmap(mp_sdf_10, "Monopartite 10 Groups", [12, 9])

# COMMAND ----------

mp_sdf_19 = monopartite.group_and_batch_spark_dataframe(mp_sdf, 'source', 'target', 19)

# COMMAND ----------

create_ingest_heatmap(mp_sdf_19, "Monopartite 19 groups", [15, 10])

# COMMAND ----------

mp_sdf_20 = monopartite.group_and_batch_spark_dataframe(mp_sdf, 'source', 'target', 20)

# COMMAND ----------

create_ingest_heatmap(mp_sdf_20, "Monopartite 20 groups", [24, 12])

# COMMAND ----------

pc_sdf_5 = predefined_components.group_and_batch_spark_dataframe(pc_sdf, 'partition_col', 5)

# COMMAND ----------

create_ingest_heatmap(pc_sdf_5, "Predefined Components 5 Groups", [8, 2])

# COMMAND ----------

pc_sdf_20 = predefined_components.group_and_batch_spark_dataframe(pc_sdf, 'partition_col', 20)

# COMMAND ----------

create_ingest_heatmap(pc_sdf_20, "Predefined Components 20 groups", [25, 2])

# COMMAND ----------


