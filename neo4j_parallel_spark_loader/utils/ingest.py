from typing import Any, Dict, Literal

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_set
from pyspark.sql.functions import max as spark_max


def ingest_spark_dataframe(
    spark_dataframe: DataFrame,
    save_mode: Literal["Overwrite", "Append"],
    options: Dict[str, Any],
) -> None:
    """
    Saves a Spark DataFrame in multiple batches based on the 'batch' column values.
    Each batch is partitioned by 'group' column and saved according to the provided options.
    Ingest will be performed in parallel according to the `group` column values.

    Parameters
    ----------
    spark_dataframe : DataFrame
        DataFrame containing 'batch' and 'grouping' columns.
        The 'batch' column is used to split the data into separate batches.
        The 'group' column is used for partitioning each batch.
    save_mode : Literal["append", "overwrite"]
        The Spark `mode` to use when ingesting data.
    options : Dict[str, Any]
        Dictionary of options to configure the DataFrame writer.
        Refer to example for more information.

    Example
    -------
    >>> options = {
            "relationship": "BOUGHT",
            "relationship.save.strategy": "keys",
            "relationship.source.save.mode": "Match",
            "relationship.source.labels": ":Customer",
            "relationship.source.node.keys": "customerID:id",
            "relationship.target.save.mode": "Match",
            "relationship.target.labels": ":Product",
            "relationship.target.node.keys": "customerID:id",
            "relationship.properties": "quantity,order"
        }
    >>> save_batches(my_spark_df, "Append", options)

    Note
    ----
    - The input DataFrame must contain 'batch' and 'grouping' columns
    - Each unique value in the 'batch' column will create a separate save operation
    - Uses the Neo4j Spark Connector for writing data to Neo4j
    """

    assert save_mode in {
        "Append",
        "Overwrite",
    }, "`save_mode` must be either 'Append' or 'Overwrite'"
    assert (
        "batch" in spark_dataframe.columns
    ), "Spark DataFrame must contain column `batch`"
    assert (
        "group" in spark_dataframe.columns
    ), "Spark DataFrame must contain column `group`"

    batch_list = spark_dataframe.select(collect_set("batch")).first()[0]

    batches = [
        spark_dataframe.filter(col("batch") == batch_value)
        for batch_value in batch_list
    ]

    num_groups = spark_dataframe.select("group").distinct().count()

    # write batches serially to Neo4j database
    for batch in batches:
        (
            batch.repartition(num_groups, "group")  # define parallel groups for ingest
            .write.mode(save_mode)
            .format("org.neo4j.spark.DataSource")
            .options(**options)
            .save()
        )
