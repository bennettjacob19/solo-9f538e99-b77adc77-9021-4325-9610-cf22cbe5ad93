# 固件分段下载网关（FastAPI）

现场终端分段下载固件，多个区间请求可共用已取回的字节。网关只在缺段时向源站
发起 HTTP Range 请求，并把**同一强 ETag 下连续的缺块合并成一次 Range（单次
最多 4 块）**，慢源站下一次请求不必逐块等回包。源站换版后，旧版本已缓存区间
仍可下载，缺段补不齐时在发送下载正文前返回 **410 Gone**，终端可重新获取当前
版本。

## 架构

```
终端 (分段 Range + If-Match)
        │
        ▼
┌─────────────────────────────┐
│ gateway  (FastAPI, :8000)   │  内存分段缓存，键 = (强 ETag, 网格起点, 网格终点)
│  /firmware/meta  /docs      │  连续缺块合并回源(≤4块/次)，不跨已缓存/在途块；
└──────────────┬──────────────┘  并发请求共享在途批次，各批次并行回源
               │ HTTP Range + If-Match
               ▼
┌─────────────────────────────┐
│ origin   (FastAPI, :8080)   │  同一固件两个等长版本 v1/v2，可换版、可延迟、
│  /admin/switch /admin/delay │  可注入损坏回包 (/admin/corrupt)
└─────────────────────────────┘
```

- 固件为确定性生成的二进制（同版本字节固定，两版本等长且内容不同），无需附带
  二进制文件，容器内即可复现。
- 缓存以 **强 ETag 标识版本**：`"firmware-v1"`、`"firmware-v2"`，天然隔离不同
  版本的分段。
- 分段按固定网格对齐（默认 256 KiB，`GATEWAY_CHUNK_SIZE` 可调）。请求区间覆盖
  哪些网格块，就只对缺失的网格块发 Range；已缓存的块直接内存读取。
- **缺块合并**：同一请求里连续的缺块合并为一次回源，单次最多
  `GATEWAY_MAX_BATCH_CHUNKS`（默认 4）块；合并遇到已缓存或正在回源的块即断开，
  绝不跨过它们。
- **并发共享**：并发请求发现缺块已在回源途中时，直接共享该批次结果，各自只等待
  覆盖自己缺块的批次；同一请求的多个批次并行发出，某一块回源慢不阻塞其余缺块。
- **校验后入缓存**：源站的 206 回包须与本批的 ETag、Content-Range、预期字节数
  完全吻合，才拆成块写入缓存；失败批次不入缓存、下次自动重试，其他已校验入
  缓存的块不受影响。回包范围/长度有误回 **502**（先于任何下载正文）。

## 用 docker compose 启动

```bash
docker compose up --build
```

- 网关：http://localhost:8000/docs
- 源站：http://localhost:8080/docs

## 终端下载流程

1. 在网关 `/docs` 或 `GET /firmware/meta` 查询当前固件的 **强 ETag 和长度**：

   ```bash
   curl -s http://localhost:8000/firmware/meta
   # {"etag":"\"firmware-v1\"","length":2097152,...}
   ```

2. 带 ETag 请求字节区间（`If-Match` + `Range`）：

   ```bash
   curl -D - -o part.bin \
     -H 'If-Match: "firmware-v1"' \
     -H 'Range: bytes=100-300000' \
     http://localhost:8000/firmware.bin
   # HTTP/1.1 206 Partial Content
   # ETag: "firmware-v1"
   # Content-Range: bytes 100-300000/2097152
   ```

   - 成功 **206**：正文与 `Content-Range` 严格对应请求区间，`ETag` 与正文同版。
   - 源站换版导致缺段补不齐：**410 Gone**（先于任何下载正文返回），终端应重新
     调用 `/firmware/meta` 取新版。
   - 源站回包范围或长度有误：**502 Bad Gateway**（先于任何下载正文返回），
     该批不入缓存，终端重试即可。
   - 已缓存的旧版区间：换版后仍返回 206（带旧 ETag）。
   - 区间越界：416 + `Content-Range: bytes */<长度>`；缺 `If-Match`/`Range` 或
     格式错误：400。

## 源站管理接口

| 接口 | 作用 |
| --- | --- |
| `POST /admin/switch` `{"version":"v2"}` | 切换当前版本（v1/v2，等长） |
| `POST /admin/delay` `{"delay":1.5}` | 给每个 206 区间响应加延迟（秒），验证慢回源 |
| `POST /admin/corrupt` `{"mode":"length","count":1}` | 让接下来 count 个 206 回包损坏：`range`=Content-Range 不符、`length`=正文截断、`etag`=外版 ETag，验证网关 502/410 与重试 |
| `GET  /admin/state` | 当前版本、ETag、延迟、损坏注入与回源统计 |
| `POST /admin/reset-stats` | 清零统计 |

### 慢回源不影响缓存命中

源站加延迟后，完全命中缓存的请求不回源、立即返回；只有真正缺段的请求等待，
且同一请求的多个缺段批次并行回源。可观察网关 `GET /cache/stats` 的
`hits/misses/coalesced/origin_range_requests`（回源次数按合并后的批次计）。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ORIGIN_BASE_URL` | `http://origin:8080` | 源站地址 |
| `GATEWAY_CHUNK_SIZE` | `262144` | 缓存/回源网格块大小（字节） |
| `GATEWAY_MAX_BATCH_CHUNKS` | `4` | 单次回源最多合并的连续缺块数 |
| `GATEWAY_TIMEOUT` | `30` | 回源 HTTP 超时（秒） |
| `FIRMWARE_LENGTH` | `1048576` | 每个固件版本的长度（两版本等长；compose 中设为 2097152 以演示 8 块合并上限） |

## 本地开发与自测

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pip install pytest pytest-asyncio asgi-lifespan

# 1) 进程内集成测试（httpx ASGI，22 个用例）
python -I -m pytest

# 2) 真实 uvicorn 进程的端到端冒烟
FIRMWARE_LENGTH=2097152 uvicorn app.origin.main:app  --port 18080 &
ORIGIN_BASE_URL=http://127.0.0.1:18080 \
  uvicorn app.gateway.main:app --port 18000 &
python -I tests/smoke_real_http.py
```

冒烟脚本覆盖：合并回源（2 块 1 次、7 块 2 次）、并发重叠共享在途批次、
损坏回包 502 与重试、换版 410、慢回源下缓存命中不受影响、全量字节校验。

## 目录

```
app/common/firmware.py   强 ETag、Range/If-Match 解析、固件生成
app/origin/main.py       本地源站：两等长版本、换版、区间延迟、损坏注入、If-Match 412
app/gateway/cache.py     分段缓存：网格对齐、连续缺块合并(≤4)、在途批次共享、失败可重试
app/gateway/main.py      网关：/firmware/meta、206/410/502/416/400、回源仅合并 Range
tests/                   pytest 集成测试 + 真实 HTTP 冒烟脚本
docker-compose.yml       origin + gateway 两服务
```

## 设计说明与边界

- 网关仅支持分段下载（必须带 `Range`）；`If-Match: *` 表示取源站当前版本。
- 缓存为进程内内存，重启清空；多副本部署时各副本独立（演示范围内未引入共享缓存）。
- 不支持 multipart/byteranges 与多 ETag 的 `If-Match`，分别返回 400。
- 源站始终用强 ETag（无 `W/` 前缀），保证字节级版本一致性。
- 批次按网格块边界对齐合并；终端请求的区间与网格无关，收齐后按原请求区间回 206。
