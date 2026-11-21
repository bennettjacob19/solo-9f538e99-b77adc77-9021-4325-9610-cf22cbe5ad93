"""In-memory segmented firmware cache.

The cache key is ``(strong ETag, chunk start, chunk end)``: cached bytes are
always bound to a specific firmware version, so segments downloaded before the
origin switched version stay downloadable afterwards.

Chunks are aligned to a fixed grid; the gateway only issues an HTTP Range
request for chunks that are not already cached, and concurrent requests for the
same missing chunk share a single upstream fetch.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.common.firmware import ByteRange


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    coalesced: int = 0  # waiters that joined another in-flight fetch
    origin_range_requests: int = 0
    gone: int = 0  # missing segments that could never be completed

    def as_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "coalesced": self.coalesced,
            "origin_range_requests": self.origin_range_requests,
            "gone": self.gone,
            "cached_chunks": 0,  # filled by SegmentCache.stats()
            "known_versions": 0,
        }


class VersionGone(Exception):
    """Raised when a missing segment cannot be fetched because the origin no
    longer serves the requested ETag (HTTP 410 to the terminal)."""


class SegmentCache:
    def __init__(self, chunk_size: int) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.chunk_size = chunk_size
        self._chunks: dict[tuple[str, int, int], bytes] = {}
        # ETag -> total representation length learned from the origin.
        self._meta: dict[str, int] = {}
        self._locks: dict[tuple[str, int, int], asyncio.Lock] = {}
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
            ByteRange(start, min(start + self.chunk_size - 1, self._meta_end_hint(start)))
            for start in range(first, last_start + 1, self.chunk_size)
        ]

    def _meta_end_hint(self, start: int) -> int:
        # The real cap happens in get_or_fetch once metadata is known; this
        # only keeps the dataclass values sane.
        return start + self.chunk_size - 1

    def _lock_for(self, key: tuple[str, int, int]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def meta_lock_for(self, etag: str) -> asyncio.Lock:
        lock = self._meta_locks.get(etag)
        if lock is None:
            lock = asyncio.Lock()
            self._meta_locks[etag] = lock
        return lock

    # ---- data access ----------------------------------------------------

    async def get_or_fetch(
        self,
        etag: str,
        chunk: ByteRange,
        fetch,
    ) -> bytes:
        """Return the cached chunk bytes, fetching them once if missing.

        ``fetch`` is an async callable ``(byte_range) -> (data, total_length)``
        that performs the upstream Range request. The final chunk of a
        representation is capped to the origin-reported length.
        """
        key = (etag, chunk.start, chunk.end)
        cached = self._chunks.get(key)
        if cached is not None:
            self.stats.hits += 1
            return cached

        async with self._lock_for(key):
            cached = self._chunks.get(key)
            if cached is not None:  # another task fetched it while we waited
                self.stats.coalesced += 1
                return cached

            self.stats.misses += 1
            try:
                data, total_length = await fetch(chunk)
            except VersionGone:
                self.stats.gone += 1
                raise
            self.remember_length(etag, total_length)
            # The origin may return a shorter body for the final chunk.
            self._chunks[(etag, chunk.start, chunk.start + len(data) - 1)] = data
            if chunk.end >= total_length - 1 and key[2] != chunk.start + len(data) - 1:
                # Store under the requested key too, so the grid lookup used by
                # chunk_span keeps working for the representation's tail.
                self._chunks[key] = data
            self.stats.origin_range_requests += 1
            return data

    async def read_range(
        self,
        etag: str,
        byte_range: ByteRange,
        fetch,
    ) -> bytes:
        """Assemble ``byte_range`` from cached chunks, fetching only gaps."""
        total = self._meta[etag]
        pieces: list[bytes] = []
        for chunk in self.chunk_span(byte_range):
            chunk = ByteRange(chunk.start, min(chunk.end, total - 1))
            data = await self.get_or_fetch(etag, chunk, fetch)
            lo = max(byte_range.start, chunk.start) - chunk.start
            hi = min(byte_range.end, chunk.start + len(data) - 1) - chunk.start
            pieces.append(data[lo:hi + 1])
        return b"".join(pieces)

    def stats_snapshot(self) -> dict:
        snap = self.stats.as_dict()
        snap["cached_chunks"] = len(self._chunks)
        snap["known_versions"] = len(self._meta)
        snap["chunk_size"] = self.chunk_size
        return snap
