from pyspark.sql import Column
from pyspark.sql.functions import col, hash, lit, pmod, when
from pyspark.sql.types import LongType


def hash_group_column(column_name: str, num_groups: int) -> Column:
    """
    Compute a group assignment in the range [0, num_groups) for `column_name` using Spark's
    built-in `hash` function. Unlike the greedy strategy, this requires no `groupBy().count()`,
    no `collect()` to the driver, and no join back against the source DataFrame.

    Because `hash()` is deterministic for a given value, the same input value always maps to
    the same group. This also means the same group columns can be reused across the source and
    target columns of a monopartite relationship so a node ID lands in the same group regardless
    of whether it appears as a source or a target.

    Note
    ----
    Spark's `hash()` function returns a constant, non-null value for `null` input. To keep this
    consistent with the greedy strategy -- where `value_key_column` gives a `null` node ID a
    `null` key and `apply_key_groupings` therefore leaves it with a `null` group -- `null` values
    in `column_name` are explicitly mapped to a `null` group here as well.

    Parameters
    ----------
    column_name : str
        The name of the column to compute a group assignment for.
    num_groups : int
        The number of groups to assign values to.

    Returns
    -------
    Column
        A column expression producing a group id in [0, num_groups), or `null` if the source
        value is `null`.
    """

    hashed_group = pmod(hash(col(column_name)), lit(num_groups)).cast(LongType())

    return when(col(column_name).isNull(), lit(None).cast(LongType())).otherwise(
        hashed_group
    )
