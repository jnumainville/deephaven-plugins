from __future__ import annotations

from typing import Literal

from deephaven.table import Table
from deephaven.execution_context import get_exec_ctx, ExecutionContext
import deephaven.pandas as dhpd

from .use_table_listener import use_table_listener
from .use_memo import use_memo
from .use_state import use_state

LockType: Literal["shared", "exclusive"]


def slice_with_ctx(
    table: Table,
    first_row: int = None,
    last_row: int = None,
    columns: str = None,
):
    table.slice(first_row, last_row + 1).view(columns)


def use_viewport_data(
    t: Table,
    first_row: int = None,
    last_row: int = None,
    columns: str = None,
) -> dict:
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

    table, set_table = use_state(t.slice(first_row, last_row + 1).view(columns))

    data, set_data = use_state(None)

    use_table_listener(table, lambda update, is_replay: set_data(dhpd.to_pandas(table)))

    return data
