from __future__ import annotations

from typing import Any, Callable, Set, cast, Sequence
from inspect import signature
import sys
from functools import partial
from deephaven.dtypes import Instant, ZonedDateTime, LocalDate
from deephaven.time import to_j_instant, to_j_zdt, to_j_local_date

from ..types import Date

DateConverter = (
    Callable[[Date], Instant]  # type: ignore
    | Callable[[Date], ZonedDateTime]  # type: ignore
    | Callable[[Date], LocalDate]  # type: ignore
)

_UNSAFE_PREFIX = "UNSAFE_"
_ARIA_PREFIX = "aria_"
_ARIA_PREFIX_REPLACEMENT = "aria-"

_CONVERTERS = {
    "java.time.Instant": to_j_instant,
    "java.time.ZonedDateTime": to_j_zdt,
    "java.time.LocalDate": to_j_local_date,
}


def get_component_name(component: Any) -> str:
    """
    Get the name of the component

    Args:
        component: The component to get the name of.

    Returns:
        The name of the component.
    """
    try:
        return component.__module__ + "." + component.__name__
    except Exception:
        return component.__class__.__module__ + "." + component.__class__.__name__


def get_component_qualname(component: Any) -> str:
    """
    Get the name of the component

    Args:
        component: The component to get the name of.

    Returns:
        The name of the component.
    """
    try:
        return component.__module__ + "." + component.__qualname__
    except Exception:
        return component.__class__.__module__ + "." + component.__class__.__qualname__


def to_camel_case(snake_case_text: str) -> str:
    """
    Convert a snake_case string to camelCase.

    Args:
        snake_case_text: The snake_case string to convert.

    Returns:
        The camelCase string.
    """
    components = snake_case_text.split("_")
    return components[0] + "".join((x[0].upper() + x[1:]) for x in components[1:])


def to_react_prop_case(snake_case_text: str) -> str:
    """
    Convert a snake_case string to camelCase, with exceptions for special props like `UNSAFE_` or `aria_` props.

    Args:
        snake_case_text: The snake_case string to convert.

    Returns:
        The camelCase string with the `UNSAFE_` prefix intact if present, or `aria_` converted to `aria-`.
    """
    if snake_case_text.startswith(_UNSAFE_PREFIX):
        return _UNSAFE_PREFIX + to_camel_case(snake_case_text[len(_UNSAFE_PREFIX) :])
    if snake_case_text.startswith(_ARIA_PREFIX):
        return _ARIA_PREFIX_REPLACEMENT + to_camel_case(
            snake_case_text[len(_ARIA_PREFIX) :]
        )
    return to_camel_case(snake_case_text)


def dict_to_camel_case(
    snake_case_dict: dict[str, Any],
    omit_none: bool = True,
    convert_key: Callable[[str], str] = to_react_prop_case,
) -> dict[str, Any]:
    """
    Convert a dict with snake_case keys to a dict with camelCase keys.

    Args:
        snake_case_dict: The snake_case dict to convert.
        omit_none: Whether to omit keys with a value of None.
        convert_key: The function to convert the keys. Can be used to customize the conversion behaviour

    Returns:
        The camelCase dict.
    """
    camel_case_dict: dict[str, Any] = {}
    for key, value in snake_case_dict.items():
        if omit_none and value is None:
            continue
        camel_case_dict[convert_key(key)] = value
    return camel_case_dict


def remove_empty_keys(dict: dict[str, Any]) -> dict[str, Any]:
    """
    Remove keys from a dict that have a value of None.

    Args:
        dict: The dict to remove keys from.

    Returns:
        The dict with keys removed.
    """
    return {k: v for k, v in dict.items() if v is not None}


def _wrapped_callable(
    max_args: int | None,
    kwargs_set: set[str] | None,
    func: Callable,
    *args: Any,
    **kwargs: Any,
) -> None:
    """
    Filter the args and kwargs and call the specified function with the filtered args and kwargs.

    Args:
        max_args: The maximum number of positional arguments to pass to the function.
          If None, all args are passed.
        kwargs_set: The set of keyword arguments to pass to the function.
          If None, all kwargs are passed.
        func: The function to call
        *args: args, used by the dispatcher
        **kwargs: kwargs, used by the dispatcher
    """
    args = args if max_args is None else args[:max_args]
    kwargs = (
        kwargs
        if kwargs_set is None
        else {k: v for k, v in kwargs.items() if k in kwargs_set}
    )
    func(*args, **kwargs)


def wrap_callable(func: Callable) -> Callable:
    """
    Wrap the function so args are dropped if they are not in the signature.

    Args:
        func: The callable to wrap

    Returns:
        The wrapped callable
    """
    try:
        if sys.version_info.major == 3 and sys.version_info.minor >= 10:
            sig = signature(func, eval_str=True)  # type: ignore
        else:
            sig = signature(func)

        max_args: int | None = 0
        kwargs_set: Set | None = set()

        for param in sig.parameters.values():
            if param.kind == param.POSITIONAL_ONLY:
                max_args = cast(int, max_args)
                max_args += 1
            elif param.kind == param.POSITIONAL_OR_KEYWORD:
                # Don't know until runtime whether this will be passed as a positional or keyword arg
                max_args = cast(int, max_args)
                kwargs_set = cast(Set, kwargs_set)
                max_args += 1
                kwargs_set.add(param.name)
            elif param.kind == param.VAR_POSITIONAL:
                max_args = None
            elif param.kind == param.KEYWORD_ONLY:
                kwargs_set = cast(Set, kwargs_set)
                kwargs_set.add(param.name)
            elif param.kind == param.VAR_KEYWORD:
                kwargs_set = None

        return partial(_wrapped_callable, max_args, kwargs_set, func)
    except ValueError or TypeError:
        # This function has no signature, so we can't wrap it
        # Return the original function should be okay
        return func


def create_props(args: dict[str, Any]) -> tuple[tuple[Any], dict[str, Any]]:
    """
    Create props from the args. Combines the named props with the kwargs props.

    Args:
        args: A dictionary of arguments, from locals()

    Returns:
        A tuple of children and props
    """
    children, props = args.pop("children"), args.pop("props")
    props.update(args)
    return children, props


def _convert_to_java_date(
    date: Date,
) -> Instant | ZonedDateTime | LocalDate:  # type: ignore
    """
    Convert a Date to a Java date type.
    In order of preference, tries to convert to Instant, ZonedDateTime, and LocalDate.
    If none of these work, raises a TypeError.

    Args:
        date: The date to convert to a Java date type.

    Returns:
        The Java date type.
    """
    try:
        return to_j_instant(date)
    except Exception:
        # ignore, try next
        pass

    try:
        return to_j_zdt(date)
    except Exception:
        # ignore, try next
        pass

    try:
        return to_j_local_date(date)
    except Exception:
        raise TypeError(
            f"Could not convert {date} to one of Instant, ZonedDateTime, or LocalDate."
        )


def get_jclass_name(value: Any) -> str:
    """
    Get the name of the Java class of the value.

    Args:
        value: The value to get the Java class name of.

    Returns:
        The name of the Java class of the value.
    """
    return str(value.jclass)[6:]


def _jclass_converter(
    value: Instant | ZonedDateTime | LocalDate,  # type: ignore
) -> Callable[[Date], Any]:
    """
    Get the converter for the Java date type.

    Args:
        value: The Java date type to get the converter for.

    Returns:
        The converter for the Java date type.
    """
    return _CONVERTERS[get_jclass_name(value)]


def _wrap_date_callable(
    date_callable: Callable[[Date], None],
    converter: Callable[[Date], Any],
) -> Callable[[Date], None]:
    """
    Wrap a callable to convert the Date argument to a Java date type.
    This maintains the original callable signature so that the Date argument can be dropped.

    Args:
        date_callable: The callable to wrap.
        converter: The date converter to use.

    Returns:
        The wrapped callable.
    """
    return lambda date: wrap_callable(date_callable)(converter(date))


def _prioritized_callable_converter(
    props: dict[str, Any],
    priority: Sequence[str],
    default_converter: Callable[[Date], Any],
) -> DateConverter:
    """
    Get a callable date converter based on the priority of the props.
    If none of the priority props are present, uses the default converter.

    Args:
        props: The props passed to the component.
        priority: The priority of the props to check.
        default_converter: The default converter to use if none of the priority props are present.

    Returns:
        The callable date converter.
    """

    for prop in priority:
        if props.get(prop):
            return _jclass_converter(props[prop])

    return default_converter


def convert_date_props(
    props: dict[str, Any],
    simple_date_props: set[str],
    list_date_props: set[str],
    callable_date_props: set[str],
    priority: Sequence[str],
    default_converter: Callable[[Date], Any] = to_j_instant,
) -> None:
    """
    Convert date props to Java date types in place.

    Args:
        props: The props passed to the component.
        simple_date_props: A set of simple date keys to convert. The prop value should be a single Date.
        list_date_props: A set of list date keys to convert. The prop value should be a list of Dates.
        callable_date_props: A set of callable date keys to convert.
            The prop value should be a callable that takes a Date.
        priority: The priority of the props to check.
        default_converter: The default converter to use if none of the priority props are present.

    Returns:
        The converted props.
    """
    for key in simple_date_props:
        if props.get(key) is not None:
            props[key] = _convert_to_java_date(props[key])

    for key in list_date_props:
        if props.get(key) is not None:
            if not isinstance(props[key], list):
                raise TypeError(f"{key} must be a list of Dates")
            props[key] = [_convert_to_java_date(date) for date in props[key]]

    # the simple props must be converted before this to simplify the callable conversion
    converter = _prioritized_callable_converter(props, priority, default_converter)

    for key in callable_date_props:
        if props.get(key) is not None:
            if not callable(props[key]):
                raise TypeError(f"{key} must be a callable")
            props[key] = _wrap_date_callable(props[key], converter)
