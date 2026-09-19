from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from pytest_mock import MockerFixture

from neo4j_parallel_spark_loader.utils.build_relationship import build_relationship


def _small_dataframe(spark_fixture: SparkSession) -> DataFrame:
    return spark_fixture.range(3).selectExpr(
        "id as source", "id + 1 as target", "cast(id % 2 as string) as component"
    )


def test_build_relationship_options_include_rel_props_when_provided(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )
    mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined"
    )

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        rel_props=["quantity", "order"],
        group_keys=["component"],
        max_serial=0,
    )

    options = mock_ingest.call_args.kwargs["options"]
    assert options["relationship.properties"] == "quantity,order"


def test_build_relationship_options_omit_rel_props_when_not_provided(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )
    mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined"
    )

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        group_keys=["component"],
        max_serial=0,
    )

    options = mock_ingest.call_args.kwargs["options"]
    assert "relationship.properties" not in options
    assert options == {
        "relationship": "BOUGHT",
        "relationship.save.strategy": "keys",
        "relationship.source.save.mode": "Match",
        "relationship.source.labels": "Customer",
        "relationship.source.node.keys": "source:id",
        "relationship.target.save.mode": "Match",
        "relationship.target.labels": "Product",
        "relationship.target.node.keys": "target:id",
    }


def test_build_relationship_uses_predefined_grouping_for_single_group_key(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)
    grouped = mocker.MagicMock(name="grouped_df")
    mock_predefined = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined",
        return_value=grouped,
    )
    mock_bipartite = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_bipartite"
    )
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        group_keys=["component"],
        num_groups=3,
        max_serial=0,
        strategy="hash",
    )

    mock_predefined.assert_called_once_with(df, "component", 3, strategy="hash")
    mock_bipartite.assert_not_called()
    assert mock_ingest.call_args.kwargs["spark_dataframe"] is grouped


def test_build_relationship_uses_bipartite_grouping_for_two_group_keys(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)
    grouped = mocker.MagicMock(name="grouped_df")
    mock_predefined = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined"
    )
    mock_bipartite = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_bipartite",
        return_value=grouped,
    )
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        group_keys=["source", "target"],
        num_groups=5,
        max_serial=0,
        strategy="hash",
    )

    mock_bipartite.assert_called_once_with(df, "source", "target", 5, strategy="hash")
    mock_predefined.assert_not_called()
    assert mock_ingest.call_args.kwargs["spark_dataframe"] is grouped


def test_build_relationship_threads_optional_parameters_to_ingest(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)
    mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined"
    )
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        group_keys=["component"],
        max_serial=0,
        on_null_batch="skip",
        checkpoint_path="s3://bucket/checkpoint",
        sort_columns=["source"],
    )

    kwargs = mock_ingest.call_args.kwargs
    assert kwargs["save_mode"] == "Overwrite"
    assert kwargs["on_null_batch"] == "skip"
    assert kwargs["checkpoint_path"] == "s3://bucket/checkpoint"
    assert kwargs["sort_columns"] == ["source"]


def test_build_relationship_serial_path_when_row_count_at_or_below_max_serial(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)  # 3 rows
    mock_predefined = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined"
    )
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )
    mock_save = mocker.patch.object(DataFrameWriter, "save")

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        group_keys=["component"],
        max_serial=3,
    )

    mock_predefined.assert_not_called()
    mock_ingest.assert_not_called()
    mock_save.assert_called_once()


def test_build_relationship_serial_path_when_no_group_keys(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    df = _small_dataframe(spark_fixture)
    mock_predefined = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_predefined"
    )
    mock_bipartite = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.group_and_batch_bipartite"
    )
    mock_ingest = mocker.patch(
        "neo4j_parallel_spark_loader.utils.build_relationship.ingest_spark_dataframe"
    )
    mock_save = mocker.patch.object(DataFrameWriter, "save")

    build_relationship(
        df,
        "BOUGHT",
        "Customer",
        "source:id",
        "Product",
        "target:id",
        group_keys=None,
        max_serial=0,
    )

    mock_predefined.assert_not_called()
    mock_bipartite.assert_not_called()
    mock_ingest.assert_not_called()
    mock_save.assert_called_once()
