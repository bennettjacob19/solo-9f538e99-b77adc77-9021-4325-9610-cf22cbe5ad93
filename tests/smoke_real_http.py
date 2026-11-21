"""Real-process smoke test (run while both uvicorn servers are up).

Exercises the whole acceptance flow over TCP:
  meta -> range miss -> range hit -> switch -> old cached OK ->
  old missing 410 -> new version 206 -> slow origin vs cache hit.
"""

import json
import time
import urllib.request

GW = "http://127.0.0.1:18000"
ORIGIN = "http://127.0.0.1:18080"
PATH = "/firmware.bin"
failures = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


def request(url, method="GET", headers=None, data=None):
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=data)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()


# 1. metadata on the gateway
status, headers, body = request(GW + "/firmware/meta")
meta = json.loads(body)
etag, length = meta["etag"], meta["length"]
check("meta 200 + strong etag", status == 200 and etag.startswith('"firmware-'), str(meta))
check("firmware length 1 MiB", length == 1048576)

# 2. first range request -> 206, correct Content-Range and ETag
status, headers, body = request(
    GW + PATH, headers={"If-Match": etag, "Range": "bytes=100-300000"}
)
check("first range 206", status == 206)
check("content-range matches request", headers.get("content-range") == f"bytes 100-300000/{length}", headers.get("content-range"))
check("etag matches body version", headers.get("etag") == etag)
check("body length matches interval", len(body) == 299901)
first_body = body

# 3. identical request served from cache; origin stats must not move
_, _, before = request(ORIGIN + "/admin/reset-stats", method="POST", data=b"{}")
time.sleep(0.1)
status, headers, body = request(
    GW + PATH, headers={"If-Match": etag, "Range": "bytes=100-300000"}
)
check("cached range identical", status == 206 and body == first_body)
_, _, stats_body = request(ORIGIN + "/admin/state")
origin_range = json.loads(stats_body)["stats"]["range_requests"]
check("no origin request on full cache hit", origin_range == 0, f"origin range_requests={origin_range}")

# 4. switch origin to v2
status, _, body = request(ORIGIN + "/admin/switch", method="POST", data=json.dumps({"version": "v2"}).encode())
new_etag = json.loads(body)["etag"]
check("switched to v2", status == 200 and new_etag != etag, body.decode())

# 5. gateway meta reflects new version
_, _, body = request(GW + "/firmware/meta")
check("gateway meta shows new etag", json.loads(body)["etag"] == new_etag)

# 6. old cached segment still downloadable with old etag
status, headers, body = request(
    GW + PATH, headers={"If-Match": etag, "Range": "bytes=100-200"}
)
check("old cached segment still 206", status == 206 and headers.get("etag") == etag)
check("old bytes unchanged", body == first_body[:101])

# 7. missing old segment -> 410 before body
status, headers, body = request(
    GW + PATH, headers={"If-Match": etag, "Range": "bytes=900000-900100"}
)
check("missing old segment -> 410", status == 410, f"status={status}")

# 8. new version range works and bytes differ from v1
status, headers, v2_body = request(
    GW + PATH, headers={"If-Match": new_etag, "Range": "bytes=0-1023"}
)
check("new version 206", status == 206 and headers.get("etag") == new_etag)
check("v2 bytes differ from v1 cached bytes", v2_body != first_body[:1024])

# 9. slow origin: cache hits stay fast while a miss waits
request(ORIGIN + "/admin/delay", method="POST", data=json.dumps({"delay": "1.5"}).encode())
t0 = time.monotonic()
status, _, body = request(GW + PATH, headers={"If-Match": new_etag, "Range": "bytes=0-100"})
hit_elapsed = time.monotonic() - t0
check("cache hit fast under slow origin", status == 206 and hit_elapsed < 0.5, f"{hit_elapsed:.2f}s")

t0 = time.monotonic()
status, _, body = request(GW + PATH, headers={"If-Match": new_etag, "Range": "bytes=524288-524387"})
miss_elapsed = time.monotonic() - t0
check("cache miss waits for slow origin", status == 206 and miss_elapsed >= 1.4, f"{miss_elapsed:.2f}s")
request(ORIGIN + "/admin/delay", method="POST", data=json.dumps({"delay": "0"}).encode())

# 10. validation: 400 without If-Match, 416 beyond end
status, _, _ = request(GW + PATH, headers={"Range": "bytes=0-10"})
check("missing If-Match -> 400", status == 400)
status, headers, _ = request(GW + PATH, headers={"If-Match": new_etag, "Range": f"bytes={length + 5}-{length + 9}"})
check("unsatisfiable range -> 416", status == 416 and headers.get("content-range") == f"bytes */{length}")

# 11. gateway cache stats endpoint
_, _, body = request(GW + "/cache/stats")
stats = json.loads(body)
check("cache stats present", {"hits", "misses", "origin_range_requests", "cached_chunks"} <= set(stats), str(stats))

print()
if failures:
    print(f"SMOKE FAILED: {failures}")
    raise SystemExit(1)
print("SMOKE OK: all acceptance checks passed")
