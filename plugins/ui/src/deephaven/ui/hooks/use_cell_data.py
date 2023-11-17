from __future__ import annotations

from deephaven.table import Table
from .use_viewport_data import use_viewport_data


def use_cell_data(
    table: Table,
    row: int,
    column: str,
) -> None:
    """
    Listen to a table and call a listener when the table updates.

    Args:
        table: Table: The table to listen to.
        listener: Callable[[TableUpdate, bool], None] | TableListener: Either a function or a TableListener with an
        on_update function. The function must take a TableUpdate and is_replay bool.
        description: str | None: An optional description for the UpdatePerformanceTracker to append to the listener’s
        entry description, default is None.
        do_replay: bool: Whether to replay the initial snapshot of the table, default is False.
        replay_lock: LockType: The lock type used during replay, default is ‘shared’, can also be ‘exclusive’.
    """

    table_data = use_viewport_data(table, first_row=row, last_row=row, columns=column)

    return (
        table_data.iloc[0][0]
        if table_data is not None and not table_data.empty
        else None
    )
