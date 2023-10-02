from __future__ import annotations

from typing import Any
import threading


class Reference:
    """A reference to an object

    Attributes:
        id: int: The id of the reference
        obj: object: The object that the reference points to
    """

    def __init__(self, index: int, obj: object):
        """
        Create a new reference
        Args:
            index: int: The index of the reference
            obj: object: The object that the reference points to
        """
        self.id = index
        self.obj = obj


class CoreExporter:
    """
    An exporter that keeps track of references to objects that need to be sent

    Attributes:
        _ref_count: int: The number of references that have been created.
            Acts as an id for the next reference
        _references: dict[object, Reference]: A map of objects to their
            references
        _new_references: list[int]: A list of new references that have been
            created
        _new_objects: list[object]: A list of new objects that have been
            created
        _used_references: set[int]: A set of references that have been used
        _revision: int: The current revision. Used to ensure that revisions are
            correctly ordered
        _ref_lock: threading.Lock: A lock to ensure that references are created
            atomically
        _rev_lock: threading.Lock: A lock to ensure that revisions are
            correctly ordered

    """

    def __init__(self):
        self._ref_count: int = 0
        self._references: dict[object, Reference] = {}
        self._new_references: list[int] = []
        self._new_objects: list[object] = []
        self._used_references: set[int] = set()
        self._revision: int = 0
        # ref lock is for atomic reference creation
        # whereas rev lock ensures revisions are correctly ordered
        self._ref_lock: threading.Lock = threading.Lock()
        self._rev_lock: threading.Lock = threading.Lock()
        pass

    def reference(self, obj: object) -> Reference:
        """
        Get a reference to an object

        Args:
            obj: The object to get a reference to

        Returns:
            Reference: The reference to the object
        """
        if obj not in self._references:
            new_ref_id = self._ref_count
            self._ref_count += 1
            self._references[obj] = Reference(new_ref_id, obj)
            self._new_references.append(new_ref_id)
            self._new_objects.append(obj)
        self._used_references.add(self._references[obj].id)
        return self._references[obj]

    def get_new_exporter(self) -> Exporter:
        """
        Get a new exporter with a new revision

        Returns:
            Exporter: The new exporter
        """
        with self._rev_lock:
            revision = self._revision
            self._revision += 1
            return Exporter(self, revision)

    def references(self) -> tuple[list[Any], list[int], list[int]]:
        """
        Creates a tuple of the new objects, new references, and removed
            references

        Returns:
            tuple[list[Any], list[int], list[int]]: The tuple of new objects,
                new references, and removed references

        """
        removed_references = []
        for obj, ref in list(self._references.items()):
            if ref.id not in self._used_references:
                removed_references.append(ref.id)
                self._references.pop(obj)

        new_references = self._new_references
        new_objects = self._new_objects
        self._new_objects = []
        self._new_references = []
        self._used_references = set()

        return new_objects, new_references, removed_references

    def acquire(self):
        """
        Acquire the reference lock
        """
        self._ref_lock.acquire()

    def release(self):
        """
        Release the reference lock
        """
        self._ref_lock.release()


class Exporter:
    """
    An exporter that keeps track of references to objects that need to be sent

    Attributes:
        core_exporter: CoreExporter: The core exporter that this exporter
        uses to keep track of references. This is shared across all exporters
        but should be locked when used
        _revision: int: The revision of this exporter
    """

    def __init__(
        self,
        core_exporter: CoreExporter = None,
        revision: int = None,
    ):
        self.core_exporter = core_exporter
        self._revision = revision

        pass

    def reference(self, obj: object) -> Reference:
        """
        Get a reference to an object

        Args:
            obj: The object to get a reference to

        Returns:
            Reference: The reference to the object
        """
        return self.core_exporter.reference(obj)

    def references(self) -> tuple[list[Any], list[int], list[int]]:
        """

        Returns:

        """
        return self.core_exporter.references()

    def acquire(self):
        """
        Acquire the reference lock
        """
        self.core_exporter.acquire()

    def release(self):
        """
        Release the reference lock
        """
        self.core_exporter.release()

    def revision(self):
        """
        Get the revision of this exporter
        Returns:
            int: The revision of this exporter
        """
        return self._revision
