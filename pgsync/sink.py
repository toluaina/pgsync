"""Sink abstraction — the neutral write-target interface that ``Sync`` drives.

Historically PGSync wrote only to Elasticsearch/OpenSearch through
:class:`pgsync.search_client.SearchClient`.  ``Sync`` already funnels *every*
write through a single sink object, built by the overridable
``Sync.make_search_client()`` hook, so the coupling to a search engine lives
entirely behind that object.

This module makes that contract explicit.  ``Sync`` only ever touches the
handful of members enumerated below; a sink for any other target (e.g.
ClickHouse) can be dropped in via ``make_search_client()`` by implementing the
two required methods and inheriting safe defaults for the rest.
``SearchClient`` is the reference implementation and now subclasses ``Sink``.

The surface ``Sync`` uses (verified against ``sync.py``):

===================  =========================================================
member               role
===================  =========================================================
``bulk``             write choke point — every document flows through here
``_create_setting``  target-schema setup (index mapping / CREATE TABLE / ...)
``prepare_action``   let the sink annotate an action doc before ``bulk``
``_search``          look up doc ids to delete (search-engine specific)
``delete_by_query``  delete ids across shards when routing is unavailable
``refresh``          make writes visible (search-engine specific; no-op else)
``search``           ad-hoc query passthrough
``teardown``         drop the target (index / table)
``close``            release the connection
``name``             human label for logging
``doc_count``        running count of successfully written docs
===================  =========================================================

Only ``bulk`` and ``_create_setting`` are abstract; a non-search sink that has
no read-back-from-target delete path (ClickHouse emits tombstones from the WAL
instead) inherits the harmless defaults for the rest.

Search-engine-specific quirks stay behind the sink. ``Sync`` no longer reads any
ES attributes (``is_opensearch``/``major_version``): the ES-<7 ``_type`` decoration
now lives in ``SearchClient.prepare_action``, which the default :meth:`Sink.prepare_action`
leaves as an identity, so a ClickHouse (or any) sink never pretends to be ES.
"""

import abc
import typing as t

if t.TYPE_CHECKING:  # avoid importing node/settings just for a type hint
    from .node import Tree


class Sink(abc.ABC):
    """Neutral write-target contract driven by :class:`pgsync.sync.Sync`.

    Subclass and implement :meth:`bulk` and :meth:`_create_setting`.  Override
    the optional members only where the target supports them.
    """

    # --- attributes Sync reads ----------------------------------------------
    name: str = "Sink"
    doc_count: int = 0

    # --- required ------------------------------------------------------------
    @abc.abstractmethod
    def bulk(
        self,
        index: str,
        actions: t.Iterable[t.Any],
        **kwargs: t.Any,
    ) -> None:
        """Write a batch of documents/rows to the target.

        This is the single choke point every index path flows through.  The
        ``**kwargs`` absorb the search-engine-specific tuning knobs
        (``chunk_size``, ``refresh``, ``raise_on_error`` ...) that non-search
        sinks are free to ignore.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _create_setting(
        self,
        index: str,
        tree: "Tree",
        setting: t.Optional[dict] = None,
        mapping: t.Optional[dict] = None,
        mappings: t.Optional[dict] = None,
        routing: t.Optional[str] = None,
    ) -> None:
        """Create the target schema if required.

        For a search engine this is the index mapping/settings; for a columnar
        sink it is the ``CREATE TABLE`` DDL derived from ``tree``.
        """
        raise NotImplementedError

    # --- optional (safe defaults) -------------------------------------------
    def prepare_action(self, doc: dict) -> dict:
        """Annotate a bulk action document just before it is written.

        The hook where target-specific bulk-API quirks live, keeping them out of
        ``Sync``. The default is the identity — a columnar sink writes the action
        as-is. ``SearchClient`` overrides it to add the legacy Elasticsearch
        ``_type`` field on ES < 7.
        """
        return doc

    def _search(
        self,
        index: str,
        table: str,
        fields: t.Optional[dict] = None,
    ) -> t.Iterator[t.Any]:
        """Yield ids of matching docs to delete.

        Search-engine specific.  Sinks that carry deletes another way (e.g.
        ClickHouse tombstones sourced from the WAL) inherit this empty default.
        """
        return iter(())

    def delete_by_query(self, index: str, ids: t.List[str]) -> None:
        """Delete documents by id without requiring their routing values."""
        raise NotImplementedError

    def refresh(self, indices: t.List[str]) -> None:
        """Make recent writes visible.  No-op where the target has no concept
        of refresh (e.g. ClickHouse)."""
        return None

    def search(self, index: str, body: dict) -> t.Any:
        """Ad-hoc query passthrough.  Optional."""
        raise NotImplementedError

    def teardown(self, index: str) -> None:
        """Drop the target (index/table).  Optional."""
        raise NotImplementedError

    def close(self) -> None:
        """Release the underlying connection.  No-op by default."""
        return None
