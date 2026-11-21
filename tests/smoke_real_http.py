"""Real-process smoke test (run while both uvicorn servers are up).

Exercises the whole acceptance flow over TCP:
  meta -> merged range fetch -> cache hit -> concurrent overlapping requests
  sharing in-flight batches -> corrupt batch 502 + retry -> switch ->
  old cached OK -> old missing 410 -> slow origin vs cache hit ->
  batch cap (8 chunks -> 2 merged Range requests) -> validation.
"""

import json
import threading
import time
import urllib.request
import urllib.error

GW = "http://127.0.0.1:18000"
ORIGIN = "http://127.0.0.1:18080"
PATH = "/firmware.bin"
LENGTH = 2097152  # 2 MiB, 8 chunks of 256 KiB
CHUNK = 262144
failures = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


def request(url, method="GET", headers=None, data=None):
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=data)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()


def get_range(etag, start, end):
    return request(GW + PATH, headers={"If-Match": etag, "Range": f"bytes={start}-{end}"})


def origin_range_requests():
    _, _, body = request(ORIGIN + "/admin/state")
    return json.loads(body)["stats"]["range_requests"]


def gateway_stats():
    _, _, body = request(GW + "/cache/stats")
    return json.loads(body)


# 1. metadata on the gateway
status, headers, body = request(GW + "/firmware/meta")
meta = json.loads(body)
etag, length = meta["etag"], meta["length"]
check("meta 200 + strong etag", status == 200 and etag.startswith('"firmware-'), str(meta))
check("firmware length 2 MiB", length == LENGTH, f"length={length}")

# Ground truth for byte comparisons, straight from the origin.
status, _, truth_v1 = request(ORIGIN + "/firmware/by-version/v1")
check("origin debug blob v1", status == 200 and len(truth_v1) == LENGTH)

# 2. first range request spans 2 chunks -> merged into ONE upstream Range
request(ORIGIN + "/admin/reset-stats", method="POST", data=b"{}")
status, headers, body = get_range(etag, 100, 300000)
check("first range 206", status == 206)
check("content-range matches request", headers.get("content-range") == f"bytes 100-300000/{length}", headers.get("content-range"))
check("etag matches body version", headers.get("etag") == etag)
check("body length matches interval", len(body) == 299901)
check("body bytes match origin", body == truth_v1[100:300001])
n = origin_range_requests()
check("2 contiguous missing chunks merged into 1 Range", n == 1, f"origin range_requests={n}")
first_body = body

# 3. identical request served from cache; origin stats must not move
status, headers, body = get_range(etag, 100, 300000)
check("cached range identical", status == 206 and body == first_body)
n = origin_range_requests()
check("no origin request on full cache hit", n == 1, f"origin range_requests={n}")

# 4. concurrent overlapping requests share in-flight batches.
#    X needs chunk 2; Y (started while X's batch is in flight) needs chunks
#    2-3. Y must join X's batch for chunk 2 and fetch only chunk 3 itself:
#    exactly 2 upstream Range requests, and both bodies stay byte-exact.
request(ORIGIN + "/admin/delay", method="POST", data=json.dumps({"delay": 0.6}).encode())
request(ORIGIN + "/admin/reset-stats", method="POST", data=b"{}")
results = {}


def worker(name, start, end):
    results[name] = get_range(etag, start, end)


tx = threading.Thread(target=worker, args=("x", 2 * CHUNK, 3 * CHUNK - 1))
tx.start()
time.sleep(0.15)  # let X register its batch before Y plans its own
ty = threading.Thread(target=worker, args=("y", 2 * CHUNK, 4 * CHUNK - 1))
ty.start()
tx.join()
ty.join()
request(ORIGIN + "/admin/delay", method="POST", data=json.dumps({"delay": 0}).encode())
sx, hx, bx = results["x"]
sy, hy, by = results["y"]
check("concurrent X 206 + bytes", sx == 206 and bx == truth_v1[2 * CHUNK:3 * CHUNK])
check("concurrent Y 206 + bytes", sy == 206 and by == truth_v1[2 * CHUNK:4 * CHUNK])
n = origin_range_requests()
check("overlapping gap fetched once (2 batches total)", n == 2, f"origin range_requests={n}")
check("gateway coalesced the shared chunk", gateway_stats()["coalesced"] >= 1, str(gateway_stats()))

# 5. corrupt upstream batch -> 502 before any body; nothing cached; retry OK
request(ORIGIN + "/admin/corrupt", method="POST", data=json.dumps({"mode": "length", "count": 1}).encode())
status, _, body = get_range(etag, 4 * CHUNK, 4 * CHUNK + 99)
check("truncated batch -> 502", status == 502, f"status={status}")
status, _, body = get_range(etag, 4 * CHUNK, 4 * CHUNK + 99)
check("retry after 502 succeeds", status == 206 and body == truth_v1[4 * CHUNK:4 * CHUNK + 100], f"status={status}")

request(ORIGIN + "/admin/corrupt", method="POST", data=json.dumps({"mode": "range", "count": 1}).encode())
status, _, body = get_range(etag, 5 * CHUNK, 5 * CHUNK + 99)
check("wrong Content-Range -> 502", status == 502, f"status={status}")
status, _, body = get_range(etag, 5 * CHUNK, 5 * CHUNK + 99)
check("retry after bad range succeeds", status == 206 and body == truth_v1[5 * CHUNK:5 * CHUNK + 100], f"status={status}")

# 6. switch origin to v2
status, _, body = request(ORIGIN + "/admin/switch", method="POST", data=json.dumps({"version": "v2"}).encode())
new_etag = json.loads(body)["etag"]
check("switched to v2", status == 200 and new_etag != etag, body.decode())

# 7. gateway meta reflects new version
_, _, body = request(GW + "/firmware/meta")
check("gateway meta shows new etag", json.loads(body)["etag"] == new_etag)

# 8. old cached segment still downloadable with old etag
status, headers, body = get_range(etag, 100, 200)
check("old cached segment still 206", status == 206 and headers.get("etag") == etag)
check("old bytes unchanged", body == first_body[:101])

# 9. missing old segment -> 410 before body (chunk 6 of v1 was never cached)
status, headers, body = get_range(etag, 6 * CHUNK, 6 * CHUNK + 100)
check("missing old segment -> 410", status == 410, f"status={status}")

# 10. slow origin: a miss waits, the subsequent cache hit stays fast
request(ORIGIN + "/admin/delay", method="POST", data=json.dumps({"delay": 1.5}).encode())
t0 = time.monotonic()
status, _, body = get_range(new_etag, 0, 100)
miss_elapsed = time.monotonic() - t0
check("cache miss waits for slow origin", status == 206 and miss_elapsed >= 1.4, f"{miss_elapsed:.2f}s")
t0 = time.monotonic()
status, _, body = get_range(new_etag, 0, 100)
hit_elapsed = time.monotonic() - t0
check("cache hit fast under slow origin", status == 206 and hit_elapsed < 0.5, f"{hit_elapsed:.2f}s")
request(ORIGIN + "/admin/delay", method="POST", data=json.dumps({"delay": 0}).encode())

# 11. batch cap: v2 chunks 1-7 are missing (7 contiguous) -> ceil(7/4) = 2
#     merged upstream Range requests for one terminal request.
request(ORIGIN + "/admin/reset-stats", method="POST", data=b"{}")
t0 = time.monotonic()
status, headers, body = get_range(new_etag, 0, LENGTH - 1)
check("full v2 firmware 206", status == 206 and len(body) == LENGTH, f"status={status}")
n = origin_range_requests()
check("7 contiguous chunks fetched as 2 batches (cap 4)", n == 2, f"origin range_requests={n}")
status, _, truth_v2 = request(ORIGIN + "/firmware/by-version/v2")
check("full v2 body byte-exact", body == truth_v2)
check("v2 bytes differ from v1", body != truth_v1)

# 12. full re-request is served from cache only
request(ORIGIN + "/admin/reset-stats", method="POST", data=b"{}")
status, _, body2 = get_range(new_etag, 0, LENGTH - 1)
check("full re-request from cache", status == 206 and body2 == body and origin_range_requests() == 0)

# 13. validation: 400 without If-Match, 416 beyond end
status, _, _ = request(GW + PATH, headers={"Range": "bytes=0-10"})
check("missing If-Match -> 400", status == 400)
status, headers, _ = get_range(new_etag, length + 5, length + 9)
check("unsatisfiable range -> 416", status == 416 and headers.get("content-range") == f"bytes */{length}")

# 14. gateway cache stats endpoint
stats = gateway_stats()
check("cache stats present", {"hits", "misses", "coalesced", "origin_range_requests", "cached_chunks", "max_batch_chunks"} <= set(stats), str(stats))
check("batch cap exposed as 4", stats.get("max_batch_chunks") == 4, str(stats.get("max_batch_chunks")))

print()
if failures:
    print(f"SMOKE FAILED: {failures}")
    raise SystemExit(1)
print("SMOKE OK: all acceptance checks passed")
