from __future__ import annotations
import logging
from typing import Any, Dict, List, Tuple, Union
from .._internal import RenderContext
from ..elements import Element, PropsType
from .RenderedNode import RenderedNode

logger = logging.getLogger(__name__)


def _get_context_key(item: Any, index_key: str) -> Union[str, None]:
    """
    Get a key for an item provided at the array/dict `index_key`. This is used to uniquely identify the item in the
    render context.

    Args:
        item: The item to get a key for.
        index_key: The key of the item in the array/dict.

    Returns:
        The key for the item in the render context.
        - If `item` is an `Element` generate a key based on the `index_key` and the `name` of the `Element`.
        - If the item is another iterable, just return the `index_key`.
        - Otherwise, return `None` as the key.
        - TODO #731: use a `key` prop if it exists on the `Element`.
    """
    if isinstance(item, Element):
        return f"{index_key}-{item.name}"
    if isinstance(item, (Dict, List, Tuple)):
        return index_key
    return None


def _render_child_item(item: Any, index_key: str, context: RenderContext) -> Any:
    """
    Renders a child item with a child context. If the child item does not need to be rendered, just return the item.

    Args:
        item: The item to render.
        index_key: The key of the item in the array/dict.
        context: The context to render the item in.

    Returns:
        The rendered item.
    """
    key = _get_context_key(item, index_key)
    return (
        _render_item(item, context.get_child_context(key)) if key is not None else item
    )


def _render_item(item: Any, context: RenderContext) -> Any:
    """
    Render an item. If the item is a list or tuple, render each item in the list.

    Args:
        item: The item to render.
        context: The context to render the item in.

    Returns:
        The rendered item.
    """
    logger.debug("_render_item context is %s", context)
    if isinstance(item, (list, tuple)):
        # I couldn't figure out how to map a `list[Unknown]` to a `list[Any]`, or to accept a `list[Unknown]` as a parameter
        return _render_list(item, context)  # type: ignore
    if isinstance(item, dict):
        return _render_dict(item, context)  # type: ignore
    if isinstance(item, Element):
        logger.debug(
            "render_child element %s: %s",
            type(item),
            item,
        )
        return _render_element(item, context)
    else:
        logger.debug("render_item returning child (%s): %s", type(item), item)
        return item


def _render_list(
    item: list[Any] | tuple[Any, ...], context: RenderContext
) -> list[Any]:
    """
    Render a list. You may be able to pass in an element as a prop that needs to be rendered, not just as a child.
    For example, a `label` prop of a button can accept a string or an element.

    Args:
        item: The list to render.
        context: The context to render the list in.

    Returns:
        The rendered list.
    """
    logger.debug("_render_list %s", item)
    with context.open():
        return [
            _render_child_item(value, str(key), context)
            for key, value in enumerate(item)
        ]


def _render_dict(item: PropsType, context: RenderContext) -> PropsType:
    """
    Render a dictionary. You may be able to pass in an element as a prop that needs to be rendered, not just as a child.
    For example, a `label` prop of a button can accept a string or an element.

    Args:
        item: The dictionary to render.
        context: The context to render the dictionary in.

    Returns:
        The rendered dictionary.
    """
    logger.debug("_render_dict %s", item)

    with context.open():
        return _render_dict_in_open_context(item, context)


def _render_dict_in_open_context(item: PropsType, context: RenderContext) -> PropsType:
    """
    Render a dictionary. You may be able to pass in an element as a prop that needs to be rendered, not just as a child.
    For example, a `label` prop of a button can accept a string or an element.

    Args:
        item: The dictionary to render.
        context: The context to render the dictionary in.

    Returns:
        The rendered dictionary.
    """
    return {key: _render_child_item(value, key, context) for key, value in item.items()}


def _render_element(element: Element, context: RenderContext) -> RenderedNode:
    """
    Render an Element.

    Args:
        element: The element to render.
        context: The context to render the component in.

    Returns:
        The RenderedNode representing the element.
    """
    logger.debug("Rendering element %s in context %s", element.name, context)

    with context.open():
        props = element.render(context)

        # We also need to render any elements that are passed in as props (including `children`)
        props = _render_dict_in_open_context(props, context)

    return RenderedNode(element.name, props)


class Renderer:
    """
    Renders Elements provided into the RenderContext provided and returns a RenderedNode.
    At this step it executing the render() method of the Element within the RenderContext state to generate the
    realized Document tree for the Element provided.
    """

    _context: RenderContext
    """
    Context to render the element into. This is essentially the state of the element.
    """

    def __init__(self, context: RenderContext):
        self._context = context

    def render(self, element: Element) -> RenderedNode:
        """
        Render an element. Will update the liveness scope with the new objects from the render.

        Args:
            element: The element to render.

        Returns:
            The rendered element.
        """
        return _render_element(element, self._context)
