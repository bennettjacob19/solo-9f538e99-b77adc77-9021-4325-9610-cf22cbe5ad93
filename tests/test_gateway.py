"""End-to-end self-tests for the segmented download gateway.

Both apps run in-process over httpx ASGI transports, so the gateway talks to
the real origin implementation through a real (in-memory) HTTP client.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app.common.firmware import (
    FIRMWARE_PATH,
    VERSIONS,
    build_firmware,
    strong_etag,
)
from app.gateway.main import create_app as create_gateway
from app.origin.main import create_app as create_origin

FIRMWARE_LENGTH = 64 * 1024  # small, non-trivial blob
CHUNK_SIZE = 16 * 1024  # 4 chunks per firmware


@pytest.fixture
async def clients():
    origin_app = create_origin(firmware_length=FIRMWARE_LENGTH)
    origin_transport = httpx.ASGITransport(app=origin_app)
    gateway_app = create_gateway(
        origin_base_url="http://origin-test",
        chunk_size=CHUNK_SIZE,
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
# 2. Missing segments are fetched with Range; identical re-requests are served
#    entirely from cache (no new origin request)
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

    # Grid-aligned chunks touched: starts 0, 16K, 32K -> 3 origin Range calls.
    origin_stats = (await origin.get("/admin/state")).json()["stats"]
    assert origin_stats["range_requests"] == 3
    gw_stats = (await gateway.get("/cache/stats")).json()
    assert gw_stats["origin_range_requests"] == 3
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
    origin_stats2 = (await origin.get("/admin/state")).json()["stats"]
    assert origin_stats2["range_requests"] == 3  # unchanged
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
    assert (await origin.get("/admin/state")).json()["stats"]["range_requests"] == 1

    # Overlapping request: first chunk cached, only the second one is fetched.
    second = await gateway.get(
        FIRMWARE_PATH,
        headers={
            "If-Match": etag,
            "Range": f"bytes={CHUNK_SIZE // 2}-{2 * CHUNK_SIZE - 1}",
        },
    )
    assert second.status_code == 206
    stats = (await origin.get("/admin/state")).json()["stats"]
    assert stats["range_requests"] == 2  # only one new Range upstream


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
        stats = (await origin.get("/admin/state")).json()["stats"]
        assert stats["range_requests"] == 1  # single upstream fetch
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
