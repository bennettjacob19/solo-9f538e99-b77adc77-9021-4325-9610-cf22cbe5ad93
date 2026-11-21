"""Local HTTP origin server.

Serves the same firmware in two equal-length versions. An operator can switch
the active version and add a delay to Range responses so that the gateway's
cache and slow-回源 behaviour can be exercised.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, Response

from app.common.firmware import (
    DEFAULT_FIRMWARE_LENGTH,
    FIRMWARE_PATH,
    VERSIONS,
    ByteRange,
    MalformedPrecondition,
    RangeNotSatisfiable,
    build_firmware,
    content_range_header,
    etag_version,
    parse_if_match,
    parse_range_header,
    strong_etag,
)


@dataclass
class OriginState:
    active_version: str = VERSIONS[0]
    range_delay: float = 0.0  # seconds, applied to every 206 response
    # Statistics used by the gateway self-tests.
    range_requests: int = 0
    full_requests: int = 0
    precondition_failures: int = 0
    switched_at: float = field(default_factory=time.time)

    def firmware(self, version: str) -> bytes:
        return _FIRMWARES[version]


# Populated by the app factory so the length is configurable.
_FIRMWARES: dict[str, bytes] = {}


def create_app(firmware_length: int | None = None) -> FastAPI:
    if firmware_length is None:
        firmware_length = int(os.getenv("FIRMWARE_LENGTH", str(DEFAULT_FIRMWARE_LENGTH)))
    _FIRMWARES.clear()
    for v in VERSIONS:
        _FIRMWARES[v] = build_firmware(v, firmware_length)
    state = OriginState()

    app = FastAPI(
        title="Firmware origin server",
        version="1.0.0",
        description=(
            "Serves two equal-length firmware versions. Use "
            "`/admin/state` to switch the active version and to add a "
            "delay to Range responses."
        ),
    )
    app.state.origin = state

    def current_etag() -> str:
        return strong_etag(state.active_version)

    @app.get("/", include_in_schema=False)
    def root() -> dict:
        return {
            "service": "firmware-origin",
            "versions": list(VERSIONS),
            "active_version": state.active_version,
            "firmware_path": FIRMWARE_PATH,
            "length": len(_FIRMWARES[state.active_version]),
        }

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/admin/state", tags=["admin"])
    def get_state() -> dict:
        return {
            "active_version": state.active_version,
            "etag": current_etag(),
            "range_delay": state.range_delay,
            "length": len(_FIRMWARES[state.active_version]),
            "stats": {
                "range_requests": state.range_requests,
                "full_requests": state.full_requests,
                "precondition_failures": state.precondition_failures,
            },
            "switched_at": state.switched_at,
        }

    @app.post("/admin/switch", tags=["admin"])
    async def switch_version(request: Request) -> dict:
        body = await request.json()
        version = body.get("version")
        if version not in VERSIONS:
            return Response(
                content=f'{{"detail":"version must be one of {VERSIONS}"}}',
                status_code=400,
                media_type="application/json",
            )
        if version != state.active_version:
            state.active_version = version
            state.switched_at = time.time()
        return {
            "active_version": state.active_version,
            "etag": current_etag(),
            "switched_at": state.switched_at,
        }

    @app.post("/admin/delay", tags=["admin"])
    async def set_delay(request: Request) -> dict:
        body = await request.json()
        delay = float(body.get("delay", 0.0))
        if delay < 0:
            return Response(
                content='{"detail":"delay must be >= 0"}',
                status_code=400,
                media_type="application/json",
            )
        state.range_delay = delay
        return {"range_delay": state.range_delay}

    @app.post("/admin/reset-stats", tags=["admin"])
    def reset_stats() -> dict:
        state.range_requests = 0
        state.full_requests = 0
        state.precondition_failures = 0
        return {"reset": True}

    def _precondition_fails(if_match: str | None, etag: str) -> bool:
        """True when the origin must answer 412 for this If-Match."""
        try:
            tags = parse_if_match(if_match)
        except MalformedPrecondition:
            raise
        if tags is None:
            return False
        if tags == ["*"]:
            return False
        return etag not in tags

    def _serve(version: str, request: Request, head: bool) -> Response:
        etag = strong_etag(version)
        try:
            if _precondition_fails(request.headers.get("if-match"), etag):
                state.precondition_failures += 1
                return Response(
                    status_code=412,
                    headers={"ETag": etag},
                )
        except MalformedPrecondition as exc:
            return Response(
                content=f'{{"detail":"{exc}"}}',
                status_code=400,
                media_type="application/json",
            )

        data = _FIRMWARES[version]
        size = len(data)
        range_header = request.headers.get("range")
        try:
            byte_range = parse_range_header(range_header, size)
        except RangeNotSatisfiable as exc:
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{exc.size}", "ETag": etag},
            )

        common_headers = {
            "ETag": etag,
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        }

        if byte_range is None:
            # No (valid) Range header: serve the whole representation.
            state.full_requests += 1
            return Response(
                content=b"" if head else data,
                status_code=200,
                headers={
                    **common_headers,
                    "Content-Length": str(size),
                    "Content-Type": "application/octet-stream",
                },
            )

        state.range_requests += 1
        body = data[byte_range.start:byte_range.end + 1]
        return Response(
            content=b"" if head else body,
            status_code=206,
            headers={
                **common_headers,
                "Content-Length": str(byte_range.length),
                "Content-Range": content_range_header(byte_range, size),
                "Content-Type": "application/octet-stream",
                "X-Origin-Range-Request": "1",
            },
        )

    @app.head(FIRMWARE_PATH, include_in_schema=False)
    async def head_firmware(request: Request) -> Response:
        return _serve(state.active_version, request, head=True)

    @app.get(FIRMWARE_PATH, tags=["firmware"])
    async def get_firmware(request: Request) -> Response:
        response = _serve(state.active_version, request, head=False)
        # Simulate a slow origin only for partial-content responses.
        if response.status_code == 206 and state.range_delay:
            await asyncio.sleep(state.range_delay)
        return response

    @app.get("/firmware/by-version/{version}", include_in_schema=False)
    async def get_specific_version(version: str, request: Request) -> Response:
        """Debug endpoint: fetch a blob of a non-active version directly."""
        if version not in _FIRMWARES:
            return Response(content='{"detail":"unknown version"}', status_code=404)
        return _serve(version, request, head=False)

    # Surface the strong ETag on the OpenAPI page description as well.
    app.description += (
        f"\n\nCurrent firmware ETag: `{current_etag()}` "
        f"(refresh `/admin/state` after a switch)."
    )
    return app


app = create_app()
