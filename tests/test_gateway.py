"""End-to-end self-tests for the segmented download gateway.

Both apps run in-process over httpx ASGI transports, so the gateway talks to
the real origin implementation through a real (in-memory) HTTP client.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app.common.firmware import (
    FIRMWARE_PATH,
    VERSIONS,
    ByteRange,
    build_firmware,
    strong_etag,
)
from app.gateway.cache import SegmentCache
from app.gateway.main import create_app as create_gateway
from app.origin.main import create_app as create_origin

FIRMWARE_LENGTH = 64 * 1024  # small, non-trivial blob
CHUNK_SIZE = 16 * 1024  # 4 chunks per firmware


@asynccontextmanager
async def build_clients(
    firmware_length: int = FIRMWARE_LENGTH,
    chunk_size: int = CHUNK_SIZE,
    max_batch_chunks: int = 4,
):
    """Wire an origin and a gateway together over in-memory ASGI transports."""
    origin_app = create_origin(firmware_length=firmware_length)
    origin_transport = httpx.ASGITransport(app=origin_app)
    gateway_app = create_gateway(
        origin_base_url="http://origin-test",
        chunk_size=chunk_size,
        max_batch_chunks=max_batch_chunks,
        transport=origin_transport,
    )
    async with (
        LifespanManager(origin_app),
        LifespanManager(gateway_app),
        httpx.AsyncClient(
            transport=origin_transport, base_url="http://origin-test"
        ) as origin,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway_app),
            base_url="http://gateway-test",
        ) as gateway,
    ):
        yield origin, gateway, gateway_app


@pytest.fixture
async def clients():
    async with build_clients() as trio:
        yield trio


async def origin_range_requests(origin) -> int:
    return (await origin.get("/admin/state")).json()["stats"]["range_requests"]


# ----------------------------------------------------------------------
# 1. Metadata: strong ETag and length
# ----------------------------------------------------------------------

async def test_meta_reports_strong_etag_and_length(clients):
    origin, gateway, _ = clients
    resp = await gateway.get("/firmware/meta")
    assert resp.status_code == 200
    meta = resp.json()
    assert meta["length"] == FIRMWARE_LENGTH
    etag = meta["etag"]
    assert etag.startswith('"') and not etag.startswith('"W/') and etag.endswith('"')
    assert etag == strong_etag(VERSIONS[0])

    # The gateway must expose the same headers a plain HEAD on the origin does.
    head = await origin.head(FIRMWARE_PATH)
    assert head.headers["ETag"] == etag
    assert int(head.headers["Content-Length"]) == FIRMWARE_LENGTH
    assert head.headers["Accept-Ranges"] == "bytes"


# ----------------------------------------------------------------------
# 2. Missing segments are fetched with one merged Range; identical
#    re-requests are served entirely from cache (no new origin request)
# ----------------------------------------------------------------------

async def test_range_miss_then_cache_hit(clients):
    origin, gateway, gw_app = clients
    meta = (await gateway.get("/firmware/meta")).json()
    etag = meta["etag"]
    await origin.post("/admin/reset-stats")

    rng = "bytes=100-40000"
    resp = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": rng}
    )
    assert resp.status_code == 206
    body = resp.content
    assert resp.headers["ETag"] == etag
    assert resp.headers["Content-Range"] == f"bytes 100-40000/{FIRMWARE_LENGTH}"
    assert len(body) == 40000 - 100 + 1

    # Grid-aligned chunks touched: starts 0, 16K, 32K -> 3 contiguous missing
    # chunks merged into a single upstream Range request.
    assert await origin_range_requests(origin) == 1
    gw_stats = (await gateway.get("/cache/stats")).json()
    assert gw_stats["origin_range_requests"] == 1
    assert gw_stats["misses"] == 3
    assert gw_stats["hits"] == 0

    # Byte-exactness against the deterministic firmware blob.
    expected = build_firmware(VERSIONS[0], FIRMWARE_LENGTH)[100:40001]
    assert body == expected

    # Second, identical request: fully cache-served.
    resp2 = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": rng}
    )
    assert resp2.status_code == 206
    assert resp2.content == body
    assert resp2.headers["Content-Range"] == resp.headers["Content-Range"]
    assert await origin_range_requests(origin) == 1  # unchanged
    gw_stats2 = (await gateway.get("/cache/stats")).json()
    assert gw_stats2["hits"] >= 3


# ----------------------------------------------------------------------
# 3. Shared segments are reused across different requested intervals
# ----------------------------------------------------------------------

async def test_overlapping_ranges_share_cached_bytes(clients):
    origin, gateway, _ = clients
    meta = (await gateway.get("/firmware/meta")).json()
    etag = meta["etag"]
    await origin.post("/admin/reset-stats")

    first = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert first.status_code == 206
    assert await origin_range_requests(origin) == 1

    # Overlapping request: first chunk cached, only the second one is fetched.
    second = await gateway.get(
        FIRMWARE_PATH,
        headers={
            "If-Match": etag,
            "Range": f"bytes={CHUNK_SIZE // 2}-{2 * CHUNK_SIZE - 1}",
        },
    )
    assert second.status_code == 206
    assert await origin_range_requests(origin) == 2  # only one new Range upstream


# ----------------------------------------------------------------------
# 4. Version switch: old cached segments keep working; missing old segments
#    produce 410 before any body; new version downloads normally
# ----------------------------------------------------------------------

async def test_version_switch_cache_and_410(clients):
    origin, gateway, _ = clients
    meta = (await gateway.get("/firmware/meta")).json()
    old_etag = meta["etag"]

    # Download part of v1 (first two chunks).
    resp = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": old_etag, "Range": f"bytes=0-{2 * CHUNK_SIZE - 1}"},
    )
    assert resp.status_code == 206
    old_part = resp.content

    # Origin switches to v2 (equal length).
    switched = (await origin.post("/admin/switch", json={"version": VERSIONS[1]})).json()
    new_etag = switched["etag"]
    assert new_etag != old_etag
    assert (await gateway.get("/firmware/meta")).json()["etag"] == new_etag

    # 4a. Old-version cached segments are still downloadable.
    cached_resp = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": old_etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert cached_resp.status_code == 206
    assert cached_resp.headers["ETag"] == old_etag
    assert cached_resp.content == old_part[:CHUNK_SIZE]

    # 4b. A missing old-version segment cannot be completed -> 410, no body.
    gone_resp = await gateway.get(
        FIRMWARE_PATH,
        headers={
            "If-Match": old_etag,
            "Range": f"bytes={3 * CHUNK_SIZE}-{4 * CHUNK_SIZE - 1}",
        },
    )
    assert gone_resp.status_code == 410
    assert gone_resp.content == b"" or b"410" in gone_resp.content
    assert gone_resp.headers["ETag"] == old_etag

    # 4c. New version can be downloaded, with its own ETag and bytes.
    new_resp = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": new_etag, "Range": "bytes=0-99"},
    )
    assert new_resp.status_code == 206
    assert new_resp.headers["ETag"] == new_etag
    v2 = build_firmware(VERSIONS[1], FIRMWARE_LENGTH)
    assert new_resp.content == v2[0:100]

    # 4d. The two equal-length versions really differ byte-wise.
    v1 = build_firmware(VERSIONS[0], FIRMWARE_LENGTH)
    assert len(v1) == len(v2) == FIRMWARE_LENGTH
    assert v1 != v2


# ----------------------------------------------------------------------
# 5. Slow origin does not block fully cache-hit requests
# ----------------------------------------------------------------------

async def test_slow_origin_does_not_block_cache_hits(clients):
    origin, gateway, _ = clients
    meta = (await gateway.get("/firmware/meta")).json()
    etag = meta["etag"]

    warm = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert warm.status_code == 206

    # Make every origin Range response slow.
    await origin.post("/admin/delay", json={"delay": 1.0})
    try:
        loop = asyncio.get_running_loop()
        start = loop.time()
        hit = await gateway.get(
            FIRMWARE_PATH,
            headers={"If-Match": etag, "Range": f"bytes=10-{CHUNK_SIZE - 100}"},
        )
        elapsed = loop.time() - start
        assert hit.status_code == 206
        assert elapsed < 0.5  # well under the 1s origin delay

        # Meanwhile a cache miss really does pay the delay.
        start = loop.time()
        miss = await gateway.get(
            FIRMWARE_PATH,
            headers={
                "If-Match": etag,
                "Range": f"bytes={2 * CHUNK_SIZE}-{3 * CHUNK_SIZE - 1}",
            },
        )
        elapsed = loop.time() - start
        assert miss.status_code == 206
        assert elapsed >= 0.9
    finally:
        await origin.post("/admin/delay", json={"delay": 0})


# ----------------------------------------------------------------------
# 6. Concurrent identical misses share one upstream Range request
# ----------------------------------------------------------------------

async def test_concurrent_misses_coalesce(clients):
    origin, gateway, _ = clients
    meta = (await gateway.get("/firmware/meta")).json()
    etag = meta["etag"]
    await origin.post("/admin/reset-stats")
    await origin.post("/admin/delay", json={"delay": 0.3})
    try:
        results = await asyncio.gather(
            *[
                gateway.get(
                    FIRMWARE_PATH,
                    headers={
                        "If-Match": etag,
                        "Range": f"bytes={CHUNK_SIZE}-{2 * CHUNK_SIZE - 1}",
                    },
                )
                for _ in range(5)
            ]
        )
        assert all(r.status_code == 206 for r in results)
        bodies = {r.content for r in results}
        assert len(bodies) == 1
        assert await origin_range_requests(origin) == 1  # single upstream fetch
    finally:
        await origin.post("/admin/delay", json={"delay": 0})


# ----------------------------------------------------------------------
# 7. Validation and error behaviour
# ----------------------------------------------------------------------

async def test_request_validation_errors(clients):
    _, gateway, _ = clients
    meta = (await gateway.get("/firmware/meta")).json()
    etag = meta["etag"]

    # Missing If-Match
    r = await gateway.get(FIRMWARE_PATH, headers={"Range": "bytes=0-10"})
    assert r.status_code == 400
    # Missing Range
    r = await gateway.get(FIRMWARE_PATH, headers={"If-Match": etag})
    assert r.status_code == 400
    # Malformed Range
    r = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": "bytes=abc"}
    )
    assert r.status_code == 400
    # Unsatisfiable range -> 416 with Content-Range: bytes */size
    r = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes={FIRMWARE_LENGTH + 10}-{FIRMWARE_LENGTH + 20}"},
    )
    assert r.status_code == 416
    assert r.headers["Content-Range"] == f"bytes */{FIRMWARE_LENGTH}"
    # Malformed If-Match
    r = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": "no-quotes", "Range": "bytes=0-10"}
    )
    assert r.status_code == 400
    # Multiple ETags in If-Match are not supported -> 400
    r = await gateway.get(
        FIRMWARE_PATH,
        headers={
            "If-Match": f'{etag}, "firmware-v2"',
            "Range": "bytes=0-10",
        },
    )
    assert r.status_code == 400


async def test_wildcard_if_match_uses_current_version(clients):
    origin, gateway, _ = clients
    await origin.post("/admin/switch", json={"version": VERSIONS[1]})
    current = (await gateway.get("/firmware/meta")).json()["etag"]
    r = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": "*", "Range": "bytes=0-10"}
    )
    assert r.status_code == 206
    assert r.headers["ETag"] == current


async def test_suffix_and_open_ended_ranges(clients):
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]

    suffix = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": "bytes=-100"}
    )
    assert suffix.status_code == 206
    assert suffix.headers["Content-Range"] == f"bytes {FIRMWARE_LENGTH - 100}-{FIRMWARE_LENGTH - 1}/{FIRMWARE_LENGTH}"
    assert len(suffix.content) == 100

    open_ended = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes={FIRMWARE_LENGTH - 50}-"},
    )
    assert open_ended.status_code == 206
    assert len(open_ended.content) == 50
    assert open_ended.content == suffix.content[-50:]


async def test_full_firmware_byte_exact(clients):
    from app.common.firmware import etag_version

    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    version = etag_version(etag)
    pieces = []
    step = CHUNK_SIZE
    for start in range(0, FIRMWARE_LENGTH, step):
        end = min(start + step - 1, FIRMWARE_LENGTH - 1)
        r = await gateway.get(
            FIRMWARE_PATH,
            headers={"If-Match": etag, "Range": f"bytes={start}-{end}"},
        )
        assert r.status_code == 206
        pieces.append(r.content)
    downloaded = b"".join(pieces)
    assert downloaded == build_firmware(version, FIRMWARE_LENGTH)


# ----------------------------------------------------------------------
# 8. Origin contract checks (If-Match 412, delay endpoint, equal lengths)
# ----------------------------------------------------------------------

async def test_origin_precondition_and_admin(clients):
    origin, _, _ = clients
    current = strong_etag(VERSIONS[0])
    other = strong_etag(VERSIONS[1])

    ok = await origin.get(
        FIRMWARE_PATH, headers={"If-Match": current, "Range": "bytes=0-9"}
    )
    assert ok.status_code == 206
    fail = await origin.get(
        FIRMWARE_PATH, headers={"If-Match": other, "Range": "bytes=0-9"}
    )
    assert fail.status_code == 412

    bad_switch = await origin.post("/admin/switch", json={"version": "v9"})
    assert bad_switch.status_code == 400


async def test_unknown_stale_etag_returns_410_before_body(clients):
    """An ETag the gateway has never cached must still fail cleanly with 410."""
    origin, gateway, _ = clients
    # Origin is on v1; ask for the v2 ETag with a range, then switch and probe
    # a completely unknown ETag.
    await origin.post("/admin/switch", json={"version": VERSIONS[1]})
    ghost = strong_etag("v9-does-not-exist")
    r = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": ghost, "Range": "bytes=0-99"}
    )
    assert r.status_code == 410
    assert r.headers["ETag"] == ghost
    assert r.content  # JSON error body, and no download payload was streamed


# ----------------------------------------------------------------------
# 9. Batched origin fetches: contiguous missing chunks merge into one Range
#    (capped at max_batch_chunks), never crossing cached/in-flight chunks
# ----------------------------------------------------------------------

SMALL = 8 * 1024


async def test_merge_caps_at_max_batch_chunks():
    """10 contiguous missing chunks -> ceil(10/4) = 3 upstream Range calls."""
    async with build_clients(firmware_length=10 * SMALL, chunk_size=SMALL) as (
        origin,
        gateway,
        _,
    ):
        etag = (await gateway.get("/firmware/meta")).json()["etag"]
        await origin.post("/admin/reset-stats")

        resp = await gateway.get(
            FIRMWARE_PATH,
            headers={"If-Match": etag, "Range": f"bytes=0-{10 * SMALL - 1}"},
        )
        assert resp.status_code == 206
        assert resp.content == build_firmware(VERSIONS[0], 10 * SMALL)
        assert await origin_range_requests(origin) == 3  # batches of 4 + 4 + 2
        stats = (await gateway.get("/cache/stats")).json()
        assert stats["misses"] == 10
        assert stats["origin_range_requests"] == 3


async def test_merge_never_crosses_cached_chunks():
    """A cached chunk splits the missing run into two separate batches."""
    async with build_clients(firmware_length=5 * SMALL, chunk_size=SMALL) as (
        origin,
        gateway,
        _,
    ):
        etag = (await gateway.get("/firmware/meta")).json()["etag"]

        # Warm chunk 2 only.
        warm = await gateway.get(
            FIRMWARE_PATH,
            headers={"If-Match": etag, "Range": f"bytes={2 * SMALL}-{3 * SMALL - 1}"},
        )
        assert warm.status_code == 206
        await origin.post("/admin/reset-stats")

        # Chunks 0-1 form one batch, chunk 2 is a cache hit, chunks 3-4 form
        # a second batch: exactly 2 new upstream Range requests.
        resp = await gateway.get(
            FIRMWARE_PATH,
            headers={"If-Match": etag, "Range": f"bytes=0-{5 * SMALL - 1}"},
        )
        assert resp.status_code == 206
        assert resp.content == build_firmware(VERSIONS[0], 5 * SMALL)
        assert await origin_range_requests(origin) == 2
        stats = (await gateway.get("/cache/stats")).json()
        assert stats["hits"] == 1  # the warm middle chunk
        assert stats["misses"] == 5  # 1 for the warm-up + 4 for the two batches


async def test_concurrent_overlapping_requests_share_batches():
    """Overlapping gaps share in-flight batches; each request waits only for
    the batches covering its own gaps."""
    async with build_clients(firmware_length=5 * SMALL, chunk_size=SMALL) as (
        origin,
        gateway,
        _,
    ):
        etag = (await gateway.get("/firmware/meta")).json()["etag"]
        await origin.post("/admin/reset-stats")
        await origin.post("/admin/delay", json={"delay": 0.3})
        try:
            # A covers chunks 0-1, B covers chunks 1-2. Whichever plans first
            # owns chunk 1's batch; the other joins it and fetches only its
            # remaining chunk -> 2 upstream Range requests, never 3, and the
            # shared chunk is never fetched twice.
            resp_a, resp_b = await asyncio.gather(
                gateway.get(
                    FIRMWARE_PATH,
                    headers={"If-Match": etag, "Range": f"bytes=0-{2 * SMALL - 1}"},
                ),
                gateway.get(
                    FIRMWARE_PATH,
                    headers={
                        "If-Match": etag,
                        "Range": f"bytes={SMALL}-{3 * SMALL - 1}",
                    },
                ),
            )
            assert resp_a.status_code == resp_b.status_code == 206
            blob = build_firmware(VERSIONS[0], 5 * SMALL)
            assert resp_a.content == blob[0 : 2 * SMALL]
            assert resp_b.content == blob[SMALL : 3 * SMALL]
            assert await origin_range_requests(origin) == 2
            stats = (await gateway.get("/cache/stats")).json()
            assert stats["misses"] == 3  # chunk 1 planned by only one request
            assert stats["coalesced"] == 1  # the other request joined it
        finally:
            await origin.post("/admin/delay", json={"delay": 0})


async def test_batches_of_one_request_run_concurrently():
    """A slow batch must not serialize the other batches of the same request:
    5 chunks -> 2 batches, wall time stays close to one origin delay."""
    async with build_clients(firmware_length=5 * SMALL, chunk_size=SMALL) as (
        origin,
        gateway,
        _,
    ):
        etag = (await gateway.get("/firmware/meta")).json()["etag"]
        await origin.post("/admin/delay", json={"delay": 0.4})
        try:
            loop = asyncio.get_running_loop()
            start = loop.time()
            resp = await gateway.get(
                FIRMWARE_PATH,
                headers={"If-Match": etag, "Range": f"bytes=0-{5 * SMALL - 1}"},
            )
            elapsed = loop.time() - start
            assert resp.status_code == 206
            assert resp.content == build_firmware(VERSIONS[0], 5 * SMALL)
            assert await origin_range_requests(origin) == 2  # 4 + 1 chunks
            # Sequential batches would take >= 0.8s; concurrent ones ~0.4s.
            assert elapsed < 0.7
        finally:
            await origin.post("/admin/delay", json={"delay": 0})


# ----------------------------------------------------------------------
# 10. Bad upstream batches: 502 before any body, nothing cached, retry works
# ----------------------------------------------------------------------

async def test_corrupt_content_range_rejected_502_then_retry(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]

    # The next 206 will carry a Content-Range that does not match the batch.
    await origin.post("/admin/corrupt", json={"mode": "range", "count": 1})
    bad = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": "bytes=0-999"}
    )
    assert bad.status_code == 502
    assert b"Content-Range" in bad.content

    # The failed batch was not cached: the retry fetches again and succeeds.
    good = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": "bytes=0-999"}
    )
    assert good.status_code == 206
    assert good.content == build_firmware(VERSIONS[0], FIRMWARE_LENGTH)[:1000]
    stats = (await gateway.get("/cache/stats")).json()
    assert stats["origin_range_requests"] == 2  # one failed + one retried
    assert stats["cached_chunks"] == 1


async def test_corrupt_body_length_rejected_502_then_retry(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]

    await origin.post("/admin/corrupt", json={"mode": "length", "count": 1})
    bad = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert bad.status_code == 502

    good = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert good.status_code == 206
    assert good.content == build_firmware(VERSIONS[0], FIRMWARE_LENGTH)[:CHUNK_SIZE]


async def test_failed_batch_does_not_poison_valid_cached_chunks(clients):
    """Chunks validated and cached by other batches stay readable after a
    sibling batch fails, and the failed chunks are simply re-fetched."""
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]

    # Warm chunk 0.
    warm = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert warm.status_code == 206

    # Chunk 1's batch gets corrupted -> the request fails with 502, but the
    # already cached chunk 0 must remain fully readable without any origin
    # traffic.
    await origin.post("/admin/corrupt", json={"mode": "length", "count": 1})
    bad = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{2 * CHUNK_SIZE - 1}"},
    )
    assert bad.status_code == 502

    await origin.post("/admin/reset-stats")
    cached = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK_SIZE - 1}"},
    )
    assert cached.status_code == 206
    assert cached.content == warm.content
    assert await origin_range_requests(origin) == 0  # served from cache only

    # Retrying the failed chunk now succeeds.
    good = await gateway.get(
        FIRMWARE_PATH,
        headers={"If-Match": etag, "Range": f"bytes=0-{2 * CHUNK_SIZE - 1}"},
    )
    assert good.status_code == 206
    assert good.content == build_firmware(VERSIONS[0], FIRMWARE_LENGTH)[: 2 * CHUNK_SIZE]


async def test_corrupt_foreign_etag_yields_410(clients):
    """A 206 carrying another version's ETag means the requested version is
    gone: 410 before any body, and nothing is cached."""
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]

    await origin.post("/admin/corrupt", json={"mode": "etag", "count": 1})
    resp = await gateway.get(
        FIRMWARE_PATH, headers={"If-Match": etag, "Range": "bytes=0-999"}
    )
    assert resp.status_code == 410
    assert resp.headers["ETag"] == etag
    stats = (await gateway.get("/cache/stats")).json()
    assert stats["cached_chunks"] == 0


# ----------------------------------------------------------------------
# 11. SegmentCache unit behaviour: failed batches are forgotten and retried
# ----------------------------------------------------------------------

async def test_cache_failed_batch_is_forgotten_and_retried():
    cache = SegmentCache(chunk_size=4, max_batch_chunks=2)
    cache.remember_length('"e"', 16)
    calls: list[tuple[int, int]] = []

    class Boom(Exception):
        pass

    async def fetch(batch: ByteRange):
        calls.append((batch.start, batch.end))
        if len(calls) == 1:
            raise Boom("upstream exploded")
        return bytes(range(batch.start, batch.end + 1)), 16

    with pytest.raises(Boom):
        await cache.read_range('"e"', ByteRange(0, 7), fetch)
    assert cache.stats_snapshot()["cached_chunks"] == 0

    # The same read retries the whole gap and now succeeds.
    data = await cache.read_range('"e"', ByteRange(0, 7), fetch)
    assert data == bytes(range(8))
    assert calls == [(0, 7), (0, 7)]  # two chunks merged into one batch, twice


async def test_cache_merges_only_contiguous_missing_chunks():
    cache = SegmentCache(chunk_size=4, max_batch_chunks=4)
    cache.remember_length('"e"', 24)
    # Pre-seed chunk [8-11] so the gap 0-15 splits into two batches.
    cache._chunks[('"e"', 8, 11)] = b"CACHE"
    batches: list[tuple[int, int]] = []

    async def fetch(batch: ByteRange):
        batches.append((batch.start, batch.end))
        return bytes([batch.start // 4]) * batch.length, 24

    data = await cache.read_range('"e"', ByteRange(0, 15), fetch)
    assert batches == [(0, 7), (12, 15)]
    assert data == bytes([0]) * 8 + b"CACHE" + bytes([3]) * 4


async def test_cache_cancelled_waiter_does_not_kill_shared_batch():
    """If one terminal disconnects, the in-flight batch must still serve the
    other requests that joined it."""
    cache = SegmentCache(chunk_size=4, max_batch_chunks=2)
    cache.remember_length('"e"', 8)
    started = asyncio.Event()
    release = asyncio.Event()

    async def fetch(batch: ByteRange):
        started.set()
        await release.wait()
        return b"\x00" * batch.length, 8

    task_a = asyncio.create_task(cache.read_range('"e"', ByteRange(0, 7), fetch))
    await started.wait()
    task_b = asyncio.create_task(cache.read_range('"e"', ByteRange(0, 7), fetch))
    await asyncio.sleep(0)  # let B register on the same in-flight batch

    task_a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task_a

    release.set()
    assert await task_b == b"\x00" * 8  # the shared batch survived A's cancel
    assert cache.stats.coalesced == 2  # both of B's chunks joined the batch
