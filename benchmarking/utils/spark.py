from typing import Any, Dict, Literal

from pyspark.sql import DataFrame, SparkSession


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Benchmarking")
        .config(
            "spark.jars.packages",
            "org.neo4j:neo4j-connector-apache-spark_2.12:5.1.0_for_spark_3",
        )
        .config("neo4j.url", "neo4j://localhost:7687")
        .config("url", "neo4j://localhost:7687")
        .config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .getOrCreate()
    )


def get_spark_details(spark_session: SparkSession) -> Dict[str, Any]:
    conf = spark_session.sparkContext.getConf()

    to_return = dict()
    to_return.update({"spark.jars.packages": conf.get("spark.jars.packages")})

    available_cpus_per_node = spark_session.sparkContext._jvm.java.lang.Runtime.getRuntime().availableProcessors()
    to_return.update({"available_cpus_per_node": available_cpus_per_node})

    return to_return


def get_current_spark_num_workers(spark_session: SparkSession) -> Dict[str, int]:
    number_of_workers = (
        len(
            [
                executor.host()
                for executor in spark_session.sparkContext._jsc.sc()
                .statusTracker()
                .getExecutorInfos()
            ]
        )
        - 1
    )
    return {"num_workers": number_of_workers}


def load_data_into_spark_dataframe(
    spark_session: SparkSession,
    category: Literal["bipartite", "monopartite", "predefined_components"],
) -> DataFrame:
    file_path = f"benchmarking/data/{category}_data.csv"

    return spark_session.read.option("header", True).csv(file_path)


def sample_spark_dataframe(
    spark_dataframe: DataFrame, desired_number: int
) -> DataFrame:
    """Work-around for Spark's inaccurate sampling method."""

    if desired_number == spark_dataframe.count():
        return spark_dataframe

    fraction = min(desired_number / spark_dataframe.count() * 1.1, 1.0)

    if fraction == 1.0:
        return spark_dataframe

    return spark_dataframe.sample(False, fraction, seed=42).limit(desired_number)
