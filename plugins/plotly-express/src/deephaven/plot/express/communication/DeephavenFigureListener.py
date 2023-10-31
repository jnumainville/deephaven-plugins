from __future__ import annotations

import json
from functools import partial
from typing import Any

from deephaven.plugin.object_type import MessageStream
from deephaven.table_listener import listen, TableUpdate

from ..exporter import Exporter
from ..deephaven_figure import DeephavenFigure, DeephavenFigureNode


class DeephavenFigureListener:
    """
    Listener for DeephavenFigure

    Attributes:
        _connection: MessageStream: The connection to send messages to
        _figure: DeephavenFigure: The figure to listen to
        _exporter: Exporter: The exporter to use for exporting the figure
        _liveness_scope: Any: The liveness scope to use for the listeners
        _listeners: list[Any]: The listeners for the partitioned tables
        _partitioned_tables: dict[str, tuple[PartitionedTable, DeephavenFigureNode]]:
            The partitioned tables to listen to
    """

    def __init__(
        self, figure: DeephavenFigure, connection: MessageStream, liveness_scope: Any
    ):
        """
        Create a new listener for the figure

        Args:
            figure: DeephavenFigure: The figure to listen to
            connection: MessageStream: The connection to send messages to
            liveness_scope: The liveness scope to use for the listeners
        """
        self._connection = connection
        self._figure = figure
        self._exporter = Exporter()
        self._liveness_scope = liveness_scope
        self._listeners = []

        head_node = figure.get_head_node()
        self._partitioned_tables = head_node.partitioned_tables

        self._setup_listeners()

    def _setup_listeners(self) -> None:
        """
        Setup listeners for the partitioned tables
        """
        for table, node in self._partitioned_tables.values():
            listen_func = partial(self._on_update, node)
            handle = listen(table.table, listen_func)
            self._liveness_scope.manage(handle.listener)

        self._figure.listener = self

    def _get_figure(self) -> DeephavenFigure:
        """
        Get the current figure

        Returns:
            The current figure
        """
        return self._figure.get_figure()

    def _on_update(
        self, node: DeephavenFigureNode, update: TableUpdate, is_replay: bool
    ) -> None:
        """
        Update the figure. Because this is called when the PartitionedTable
        meta table is updated, it will always trigger a rerender.

        Args:
            node: DeephavenFigureNode: The node to update. Changes will propagate up from this node.
            update: TableUpdate: Not used. Required for the listener.
            is_replay: bool: Not used. Required for the listener.
        """
        if self._connection:
            node.recreate_figure()
            self._connection.on_data(
                *self._build_figure_message(self._get_figure(), self._exporter)
            )

    def _handle_retrieve_figure(self, exporter: Exporter) -> tuple[bytes, list[Any]]:
        """
        Handle a retrieve message. This will return a message with the current
        figure.

        Args:
            exporter: Exporter: The exporter to use for exporting the figure

        Returns:
            tuple[bytes, list[Any]]: The result of the message as a tuple of
              (new payload, new references)
        """
        return self._build_figure_message(self._get_figure(), exporter)

    def _build_figure_message(
        self, figure: DeephavenFigure, exporter: Exporter
    ) -> tuple[bytes, list[Any]]:
        """
        Build a message to send to the client with the current figure.

        Args:
            figure: DeephavenFigure: The figure to send
            exporter: Exporter: The exporter to use for exporting the figure

        Returns:
            tuple[bytes, list[Any]]: The result of the message as a tuple of
              (new payload, new references)
        """
        new_figure = figure.to_dict(exporter=exporter)

        new_objects, new_references, removed_references = exporter.references()

        message = {
            "type": "NEW_FIGURE",
            "figure": new_figure,
            "new_references": new_references,
            "removed_references": removed_references,
        }

        return json.dumps(message).encode(), new_objects

    def process_message(
        self, payload: bytes, references: list[Any]
    ) -> tuple[bytes, list[Any]]:
        """
        The main message processing function. This will handle the message
        and return the result.

        Args:
            payload: bytes: The payload to process
            references:  list[Any]: References to objects on the server

        Returns:
            tuple[bytes, list[Any]]: The result of the message as a tuple of
              (new payload, new references)

        """
        # need to create a new exporter for each message
        message = json.loads(payload.decode())
        if message["type"] == "RETRIEVE":
            return self._handle_retrieve_figure(Exporter())