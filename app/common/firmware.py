"""Shared helpers for the origin server and the download gateway.

The demo "firmware" is a deterministic binary blob so that the two equal-length
versions are reproducible inside any container without shipping binary files.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

DEFAULT_FIRMWARE_LENGTH = 1 * 1024 * 1024  # 1 MiB per version
VERSIONS = ("v1", "v2")
FIRMWARE_PATH = "/firmware.bin"


def build_firmware(version: str, length: int) -> bytes:
    """Return a deterministic, version-specific byte string of ``length``."""
    rng = random.Random(f"firmware::{version}")
    return rng.randbytes(length)


def strong_etag(version: str) -> str:
    """A strong ETag (no ``W/`` weak prefix), per RFC 9110."""
    return f'"firmware-{version}"'


def etag_version(etag: str) -> str | None:
    """Extract the version embedded in one of our strong ETags."""
    prefix, suffix = '"firmware-', '"'
    if etag.startswith(prefix) and etag.endswith(suffix) and len(etag) > len(prefix) + 1:
        return etag[len(prefix):-len(suffix)]
    return None


@dataclass(frozen=True)
class ByteRange:
    """An inclusive byte interval ``[start, end]``."""

    start: int
    end: int

    @property
    def length(self) -> int:
        return self.end - self.start + 1


def parse_range_header(header: str | None, size: int) -> ByteRange | None:
    """Parse a single ``bytes=`` range.

    Returns ``None`` for a syntactically invalid header (the caller answers
    400). A syntactically valid but unsatisfiable range raises
    :class:`RangeNotSatisfiable` (the caller answers 416).
    """
    if header is None:
        return None
    spec = header.strip()
    if not spec.startswith("bytes="):
        return None
    spec = spec[len("bytes="):].strip()
    # Multipart/byteranges are deliberately not supported by this demo.
    if "," in spec or not spec:
        return None
    if spec.startswith("-"):  # suffix range: bytes=-N => last N bytes
        suffix = spec[1:].strip()
        if not suffix.isdigit():
            return None
        n = int(suffix)
        if n == 0:
            raise RangeNotSatisfiable(size)
        start = max(0, size - n)
        return ByteRange(start, size - 1)
    if spec.endswith("-"):  # open-ended range: bytes=N-
        start_text = spec[:-1].strip()
        if not start_text.isdigit():
            return None
        start = int(start_text)
        if start >= size:
            raise RangeNotSatisfiable(size)
        return ByteRange(start, size - 1)
    parts = spec.split("-", 1)
    if len(parts) != 2 or not parts[0].strip().isdigit() or not parts[1].strip().isdigit():
        return None
    start, end = int(parts[0]), int(parts[1])
    if start > end:
        return None
    if start >= size:
        raise RangeNotSatisfiable(size)
    return ByteRange(start, min(end, size - 1))


class RangeNotSatisfiable(Exception):
    def __init__(self, size: int) -> None:
        super().__init__(f"range not satisfiable for size {size}")
        self.size = size


def parse_if_match(header: str | None) -> list[str] | None:
    """Parse an ``If-Match`` header into a list of raw ETag values.

    ``*`` is returned as the single-element list ``["*"]``.
    ``None`` means the header was absent. A malformed header raises
    :class:`MalformedPrecondition` (the caller answers 400).
    """
    if header is None:
        return None
    raw = header.strip()
    if not raw:
        raise MalformedPrecondition("empty If-Match")
    if raw == "*":
        return ["*"]
    tags: list[str] = []
    i = 0
    while i < len(raw):
        while i < len(raw) and raw[i] in " \t":
            i += 1
        if i >= len(raw) or raw[i] != '"':
            raise MalformedPrecondition("If-Match entries must be quoted strings")
        j = i + 1
        while j < len(raw) and raw[j] != '"':
            if raw[j] == "\\":  # skip escaped character
                j += 1
            j += 1
        if j >= len(raw):
            raise MalformedPrecondition("unterminated quoted-string in If-Match")
        tags.append(raw[i:j + 1])
        i = j + 1
        while i < len(raw) and raw[i] in " \t":
            i += 1
        if i < len(raw):
            if raw[i] != ",":
                raise MalformedPrecondition("expected comma between If-Match entries")
            i += 1
    if not tags:
        raise MalformedPrecondition("empty If-Match")
    return tags


class MalformedPrecondition(Exception):
    pass


def content_range_header(byte_range: ByteRange, size: int) -> str:
    return f"bytes {byte_range.start}-{byte_range.end}/{size}"
