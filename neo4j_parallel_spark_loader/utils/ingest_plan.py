from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class IngestPlan:
    """
    What `ingest_spark_dataframe` needs to know about a grouped and batched DataFrame, as
    worked out by grouping and batching.

    Without a plan, `ingest_spark_dataframe` runs a pass over the DataFrame to discover the
    `(batch, group)` pairs and the rows it cannot ingest. The grouping and batching step already
    knows all of this, so `group_and_batch_spark_dataframe(..., return_plan=True)` returns it and
    ingest skips that pass.

    Attributes
    ----------
    batches : Dict[Any, List[Any]]
        Every batch value mapped to the group values that can occur in it. A listed group
        with no rows costs one idle Spark task. Rows in a batch missing from the plan are not
        written.
    total_rows : Optional[int]
        The number of rows in the DataFrame, if known.
    null_rows : Optional[int]
        The number of rows with a `null` `batch` or `group`, if known. `ingest_spark_dataframe`
        uses it to apply `on_null_batch` without counting. When `None` the check is skipped and
        such rows are not written.
    """

    batches: Dict[Any, List[Any]]
    total_rows: Optional[int] = None
    null_rows: Optional[int] = None
