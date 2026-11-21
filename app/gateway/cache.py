"""In-memory segmented firmware cache with batched origin fetches.

The cache key is ``(strong ETag, chunk start, chunk end)``: cached bytes are
always bound to a specific firmware version, so segments downloaded before the
origin switched version stay downloadable afterwards.

Chunks are aligned to a fixed grid. When a terminal request spans several
missing chunks, the cache merges each run of *contiguous* missing chunks into
a single upstream HTTP Range request (at most ``max_batch_chunks`` chunks per
request). A batch never crosses a chunk that is already cached or already
being fetched: those chunks are served from the cache or shared with the
in-flight batch instead. All batches of one request are started concurrently,
so one slow upstream range does not delay the others, and concurrent requests
whose gaps overlap share the same in-flight batch — each of them only waits
for the batches that cover its own gaps.

A batch is written into the cache only after the upstream 206 response has
been validated (ETag, Content-Range and exact byte count, enforced by the
``fetch_batch`` callable). A failed batch stores nothing and is forgotten, so
the next request simply retries it; chunks that other batches already
validated and cached stay readable.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.common.firmware import ByteRange

DEFAULT_MAX_BATCH_CHUNKS = 4


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    coalesced: int = 0  # chunk waits that joined another in-flight batch
    origin_range_requests: int = 0  # merged upstream Range requests (batches)
    gone: int = 0  # batches that failed because the version is gone

    def as_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "coalesced": self.coalesced,
            "origin_range_requests": self.origin_range_requests,
            "gone": self.gone,
            "cached_chunks": 0,  # filled by SegmentCache.stats_snapshot()
            "known_versions": 0,
        }


class VersionGone(Exception):
    """Raised when a missing segment cannot be fetched because the origin no
    longer serves the requested ETag (HTTP 410 to the terminal)."""


class SegmentCache:
    def __init__(
        self,
        chunk_size: int,
        max_batch_chunks: int = DEFAULT_MAX_BATCH_CHUNKS,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if max_batch_chunks <= 0:
            raise ValueError("max_batch_chunks must be positive")
        self.chunk_size = chunk_size
        self.max_batch_chunks = max_batch_chunks
        self._chunks: dict[tuple[str, int, int], bytes] = {}
        # ETag -> total representation length learned from the origin.
        self._meta: dict[str, int] = {}
        # chunk key -> in-flight batch task that will populate it.
        self._inflight: dict[tuple[str, int, int], asyncio.Task] = {}
        self._meta_locks: dict[str, asyncio.Lock] = {}
        self.stats = CacheStats()

    # ---- metadata -------------------------------------------------------

    def remember_length(self, etag: str, length: int) -> None:
        self._meta.setdefault(etag, length)

    def known_length(self, etag: str) -> int | None:
        return self._meta.get(etag)

    def chunk_span(self, byte_range: ByteRange) -> list[ByteRange]:
        """Grid-aligned chunks covering ``byte_range`` (inclusive)."""
        first = (byte_range.start // self.chunk_size) * self.chunk_size
        last_start = (byte_range.end // self.chunk_size) * self.chunk_size
        return [
            ByteRange(start, start + self.chunk_size - 1)
            for start in range(first, last_start + 1, self.chunk_size)
        ]

    def meta_lock_for(self, etag: str) -> asyncio.Lock:
        lock = self._meta_locks.get(etag)
        if lock is None:
            lock = asyncio.Lock()
            self._meta_locks[etag] = lock
        return lock

    # ---- batched fetching -----------------------------------------------

    async def _fetch_and_store(
        self,
        etag: str,
        batch: list[tuple[int, tuple[str, int, int]]],
        batch_range: ByteRange,
        fetch_batch,
    ) -> None:
        """Run one merged upstream fetch, then split it into cached chunks.

        On failure nothing is cached and the in-flight markers are dropped,
        so a later request retries the same chunks.
        """
        keys = [key for _, key in batch]
        try:
            data, total_length = await fetch_batch(batch_range)
            self.remember_length(etag, total_length)
            base = batch_range.start
            for chunk_key in keys:
                _, start, end = chunk_key
                self._chunks[chunk_key] = data[start - base:end - base + 1]
        except VersionGone:
            self.stats.gone += 1
            raise
        finally:
            me = asyncio.current_task()
            for key in keys:
                if self._inflight.get(key) is me:
                    del self._inflight[key]

    @staticmethod
    def _retrieved(task: asyncio.Task) -> None:
        # Retrieve the outcome of shared batch tasks so an exception is never
        # reported as "never retrieved" when every waiter went away.
        if not task.cancelled():
            task.exception()

    async def read_range(
        self,
        etag: str,
        byte_range: ByteRange,
        fetch_batch,
    ) -> bytes:
        """Assemble ``byte_range`` from cached chunks, fetching only the gaps.

        ``fetch_batch`` is an async callable ``(batch_range) -> (data, total)``
        that performs one upstream Range request covering the whole interval
        and validates the 206 response (ETag, Content-Range, byte count).
        """
        total = self._meta[etag]
        chunks = [
            ByteRange(c.start, min(c.end, total - 1))
            for c in self.chunk_span(byte_range)
        ]

        # Plan the request. This block contains no ``await``, so the
        # cache/in-flight checks and the batch registration are atomic with
        # respect to other tasks on the event loop.
        wait_for: dict[int, asyncio.Task] = {}  # chunk index -> covering task
        i = 0
        n = len(chunks)
        while i < n:
            chunk = chunks[i]
            key = (etag, chunk.start, chunk.end)
            if key in self._chunks:
                self.stats.hits += 1
                i += 1
                continue
            task = self._inflight.get(key)
            if task is not None:
                self.stats.coalesced += 1
                wait_for[i] = task
                i += 1
                continue
            # Start of a run of contiguous missing, not-yet-fetching chunks.
            # The run ends at a cached/in-flight chunk or at the batch cap.
            batch: list[tuple[int, tuple[str, int, int]]] = []
            while i < n and len(batch) < self.max_batch_chunks:
                c = chunks[i]
                k = (etag, c.start, c.end)
                if k in self._chunks or k in self._inflight:
                    break
                batch.append((i, k))
                i += 1
            batch_range = ByteRange(batch[0][1][1], batch[-1][1][2])
            task = asyncio.create_task(
                self._fetch_and_store(etag, batch, batch_range, fetch_batch)
            )
            task.add_done_callback(self._retrieved)
            for idx, k in batch:
                self._inflight[k] = task
                wait_for[idx] = task
            self.stats.misses += len(batch)
            self.stats.origin_range_requests += 1

        # Wait only for the batches that cover this request's own gaps. The
        # shield keeps a cancelled/disconnected waiter from cancelling a
        # batch that other requests still rely on.
        if wait_for:
            pending = [asyncio.shield(t) for t in set(wait_for.values())]
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

        pieces: list[bytes] = []
        for chunk in chunks:
            data = self._chunks[(etag, chunk.start, chunk.end)]
            lo = max(byte_range.start, chunk.start) - chunk.start
            hi = min(byte_range.end, chunk.start + len(data) - 1) - chunk.start
            pieces.append(data[lo:hi + 1])
        return b"".join(pieces)

    def stats_snapshot(self) -> dict:
        snap = self.stats.as_dict()
        snap["cached_chunks"] = len(self._chunks)
        snap["known_versions"] = len(self._meta)
        snap["chunk_size"] = self.chunk_size
        snap["max_batch_chunks"] = self.max_batch_chunks
        snap["inflight_batches"] = len({id(t) for t in self._inflight.values()})
        return snap
