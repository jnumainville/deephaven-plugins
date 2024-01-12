from __future__ import annotations

from functools import partial
from typing import Callable

from deephaven.execution_context import get_exec_ctx, ExecutionContext

from .use_ref import use_ref


def func_with_ctx(
    exec_ctx: ExecutionContext,
    func: Callable,
) -> None:
    """
    Call the function within an execution context.

    Args:
        exec_ctx: ExecutionContext: The execution context to use.
        func: Callable: The function to call.
    """
    with exec_ctx:
        func()


def use_execution_context(exec_ctx: ExecutionContext = None) -> None:
    """
    Create an execution context wrapper for a function.

    Args:
        exec_ctx: ExecutionContext: The execution context to use. Defaults to
            the current execution context if not provided.
    """
    exec_ctx = use_ref(exec_ctx if exec_ctx else get_exec_ctx())

    return partial(func_with_ctx, exec_ctx.current)
