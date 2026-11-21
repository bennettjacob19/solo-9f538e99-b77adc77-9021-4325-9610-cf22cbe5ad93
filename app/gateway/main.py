"""FastAPI download gateway with segmented, version-aware caching.

Flow for a terminal download request:

1. The terminal learns the current firmware's strong ETag and length from
   ``GET /firmware/meta`` (visible on the ``/docs`` page).
2. It requests a byte interval with ``If-Match: <ETag>`` plus ``Range``.
3. The gateway answers 206 from cached segments, issuing HTTP Range requests
   to the origin only for missing chunks. If a missing chunk can no longer be
   completed for that ETag (the origin switched version), the gateway answers
   **410 before any body bytes are sent**, so the terminal can re-fetch the
   current version metadata.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response

from app.common.firmware import (
    DEFAULT_FIRMWARE_LENGTH,
    FIRMWARE_PATH,
    ByteRange,
    MalformedPrecondition,
    RangeNotSatisfiable,
    content_range_header,
    parse_if_match,
    parse_range_header,
)
from app.gateway.cache import SegmentCache, VersionGone

ORIGIN_BASE_URL = os.getenv("ORIGIN_BASE_URL", "http://origin:8080")
CHUNK_SIZE = int(os.getenv("GATEWAY_CHUNK_SIZE", str(256 * 1024)))
REQUEST_TIMEOUT = float(os.getenv("GATEWAY_TIMEOUT", "30"))


class UpstreamError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def create_app(
    origin_base_url: str = ORIGIN_BASE_URL,
    chunk_size: int = CHUNK_SIZE,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    cache = SegmentCache(chunk_size=chunk_size)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(
            base_url=origin_base_url,
            timeout=REQUEST_TIMEOUT,
            transport=transport,
        )
        app.state.http = client
        app.state.cache = cache
        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(
        title="Firmware download gateway",
        version="1.0.0",
        description=(
            "Segmented firmware download gateway. Call `GET /firmware/meta` "
            "for the current strong ETag and length, then request byte "
            "intervals with `If-Match` + `Range`."
        ),
        lifespan=lifespan,
    )

    async def fetch_current_meta() -> tuple[str, int]:
        """Fresh metadata straight from the origin (never cached)."""
        client: httpx.AsyncClient = app.state.http
        resp = await client.head(FIRMWARE_PATH)
        if resp.status_code != 200:
            raise UpstreamError(502, f"origin HEAD failed: {resp.status_code}")
        etag = resp.headers.get("ETag")
        length_text = resp.headers.get("Content-Length")
        if not etag or not length_text:
            raise UpstreamError(502, "origin HEAD missing ETag/Content-Length")
        cache.remember_length(etag, int(length_text))
        return etag, int(length_text)

    async def ensure_length(etag: str) -> int:
        length = cache.known_length(etag)
        if length is not None:
            return length
        # Unknown ETag: ask the origin once, protected by a per-ETag lock.
        async with cache.meta_lock_for(etag):
            length = cache.known_length(etag)
            if length is not None:
                return length
            client: httpx.AsyncClient = app.state.http
            resp = await client.get(
                FIRMWARE_PATH,
                headers={"If-Match": etag, "Range": "bytes=0-0"},
            )
            if resp.status_code == 412:
                raise VersionGone(f"origin does not serve {etag} anymore")
            if resp.status_code not in (200, 206):
                raise UpstreamError(502, f"origin probe failed: {resp.status_code}")
            if resp.headers.get("ETag") != etag:
                # Defensive: origin answered a different representation.
                raise VersionGone(f"ETag mismatch probing {etag}")
            if resp.status_code == 206:
                cr = resp.headers.get("Content-Range", "")
                total = int(cr.rsplit("/", 1)[1])
            else:
                total = int(resp.headers["Content-Length"])
            cache.remember_length(etag, total)
            # Keep the probe's payload if useful (first byte of the blob).
            return total

    async def origin_fetch(etag: str, chunk: ByteRange) -> tuple[bytes, int]:
        """Perform exactly one upstream Range request for one missing chunk."""
        client: httpx.AsyncClient = app.state.http
        resp = await client.get(
            FIRMWARE_PATH,
            headers={
                "If-Match": etag,
                "Range": f"bytes={chunk.start}-{chunk.end}",
            },
        )
        if resp.status_code == 412:
            raise VersionGone(f"origin no longer serves {etag}")
        if resp.status_code == 416:
            raise UpstreamError(502, "origin reported range not satisfiable")
        if resp.status_code != 206:
            raise UpstreamError(502, f"unexpected origin status {resp.status_code}")
        if resp.headers.get("ETag") != etag:
            raise VersionGone(f"origin returned wrong ETag for {etag}")
        content_range = resp.headers.get("Content-Range", "")
        try:
            span, total_text = content_range.split("/", 1)
            start_text, end_text = span.replace("bytes ", "").split("-")
            start, end, total = int(start_text), int(end_text), int(total_text)
        except (ValueError, IndexError):
            raise UpstreamError(502, "origin sent malformed Content-Range")
        if start != chunk.start or end > chunk.end or len(resp.content) != end - start + 1:
            raise UpstreamError(502, "origin range does not match the request")
        return resp.content, total

    @app.get("/", include_in_schema=False)
    def root() -> dict:
        return {
            "service": "firmware-gateway",
            "origin": origin_base_url,
            "chunk_size": cache.chunk_size,
            "docs": "/docs",
            "meta_endpoint": "/firmware/meta",
        }

    @app.get("/health")
    async def health() -> Response:
        try:
            client: httpx.AsyncClient = app.state.http
            resp = await client.get("/health")
            upstream = "ok" if resp.status_code == 200 else "degraded"
        except httpx.HTTPError:
            upstream = "unreachable"
        return Response(
            content=f'{{"status":"ok","origin":"{upstream}"}}',
            media_type="application/json",
        )

    @app.get("/firmware/meta", tags=["firmware"])
    async def firmware_meta() -> dict:
        """Current firmware metadata: strong ETag, length and download path."""
        etag, length = await fetch_current_meta()
        return {
            "etag": etag,
            "length": length,
            "firmware_path": FIRMWARE_PATH,
            "usage": "send If-Match: <etag> and a Range header to the firmware path",
        }

    @app.get(FIRMWARE_PATH, tags=["firmware"])
    async def download(request: Request) -> Response:
        if_match_raw = request.headers.get("if-match")
        try:
            if_match = parse_if_match(if_match_raw)
        except MalformedPrecondition as exc:
            return Response(
                content=f'{{"detail":"malformed If-Match: {exc}"}}',
                status_code=400,
                media_type="application/json",
            )
        if if_match is None:
            return Response(
                content='{"detail":"If-Match header with a strong ETag is required"}',
                status_code=400,
                media_type="application/json",
            )

        range_header = request.headers.get("range")
        if range_header is None:
            return Response(
                content='{"detail":"Range header is required (segmented download only)"}',
                status_code=400,
                media_type="application/json",
            )

        # Resolve the requested ETag ("*" means whatever the origin serves now).
        if if_match == ["*"]:
            etag, _ = await fetch_current_meta()
        elif len(if_match) == 1:
            etag = if_match[0]  # a single strong ETag is the supported case
        else:
            return Response(
                content='{"detail":"exactly one ETag (or *) is supported in If-Match"}',
                status_code=400,
                media_type="application/json",
            )

        # All failure checks happen before any body bytes are produced.
        try:
            length = await ensure_length(etag)
        except VersionGone:
            return Response(
                content=(
                    '{"detail":410,"message":"requested firmware version is no '
                    'longer available; re-fetch /firmware/meta"}'
                ),
                status_code=410,
                media_type="application/json",
                headers={"ETag": etag},
            )
        except UpstreamError as exc:
            return Response(
                content=f'{{"detail":"upstream error: {exc.detail}"}}',
                status_code=exc.status_code,
                media_type="application/json",
            )

        try:
            byte_range = parse_range_header(range_header, length)
        except RangeNotSatisfiable as exc:
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{exc.size}", "ETag": etag},
            )
        if byte_range is None:
            return Response(
                content='{"detail":"unsupported or malformed Range header"}',
                status_code=400,
                media_type="application/json",
            )

        async def fetch(chunk: ByteRange) -> tuple[bytes, int]:
            return await origin_fetch(etag, chunk)

        try:
            body = await cache.read_range(etag, byte_range, fetch)
        except VersionGone:
            return Response(
                content=(
                    '{"detail":410,"message":"missing segment cannot be completed '
                    'after origin version switch; re-fetch /firmware/meta"}'
                ),
                status_code=410,
                media_type="application/json",
                headers={"ETag": etag},
            )
        except UpstreamError as exc:
            return Response(
                content=f'{{"detail":"upstream error: {exc.detail}"}}',
                status_code=exc.status_code,
                media_type="application/json",
            )

        return Response(
            content=body,
            status_code=206,
            headers={
                "ETag": etag,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(body)),
                "Content-Range": content_range_header(byte_range, length),
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-cache",
            },
        )

    @app.get("/cache/stats", tags=["observability"])
    def cache_stats() -> dict:
        return cache.stats_snapshot()

    return app


app = create_app()
