"""End-to-end concurrency / cooldown / stop test using the real Crawler
against a fake upstream server (no external processes required).

Run with the project venv:
  python -I tests/test_crawler_contract.py
"""

import asyncio
import sys
import time

sys.path.insert(0, "/workspace/collector")

from app.crawler import MAX_IN_FLIGHT, Crawler  # noqa: E402


class FakeUpstream:
    def __init__(self):
        self.inflight = 0
        self.max_inflight = 0
        self.total_hits = 0
        self.lock = asyncio.Lock()
        # First two requests after this gate are answered with 429.
        self.reject_next = 0
        self.retry_after = 3

    async def handle(self, scope, receive, send):
        path = scope["path"]
        if path == "/health":
            await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"ok"})
            return

        async with self.lock:
            self.inflight += 1
            self.total_hits += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
        assert self.inflight <= MAX_IN_FLIGHT, f"concurrency violated: {self.inflight} in flight"

        await asyncio.sleep(0.15)  # keep requests overlapping

        try:
            if self.reject_next > 0:
                self.reject_next -= 1
                await send({
                    "type": "http.response.start",
                    "status": 429,
                    "headers": [(b"retry-after", str(self.retry_after).encode()),
                                (b"content-type", b"text/html")],
                })
                await send({"type": "http.response.body", "body": b"<html>429</html>"})
                return

            if path.startswith("/tenders") or path.startswith("/awards"):
                body = self._list_html(path)
            elif path.startswith("/notice/"):
                code = path.rsplit("/", 1)[-1]
                body = self._detail_html(code)
            else:
                body = "<html>not found</html>"
                await send({"type": "http.response.start", "status": 404,
                            "headers": [(b"content-type", b"text/html")]})
                await send({"type": "http.response.body", "body": body.encode()})
                return

            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", b"text/html; charset=utf-8")]})
            await send({"type": "http.response.body", "body": body.encode()})
        finally:
            async with self.lock:
                self.inflight -= 1

    @staticmethod
    def _list_html(path: str) -> str:
        section = "tender" if path.startswith("/tenders") else "award"
        rows = "".join(
            f'<tr class="announcement"><td class="code">ZB-2026-{n:03d}</td>'
            f'<td><a href="/notice/ZB-2026-{n:03d}">公告{n}</a></td></tr>'
            for n in range(1, 5)
        )
        return (
            '<html><body><table id="announcement-list"><tbody>'
            + rows
            + f'</tbody></table><nav class="pager" data-total-pages="2"></nav>'
            + f'<a href="/{section}s?page=2">2</a></body></html>'
        )

    @staticmethod
    def _detail_html(code: str) -> str:
        return f"""<html><body><h1 id="detail-title">标题 {code}</h1>
        <table class="detail-table">
        <tr><th>公告编号</th><td>{code}</td></tr>
        <tr><th>采购单位</th><td>测试单位</td></tr>
        <tr><th>截止日期</th><td>2026-12-31 17:00</td></tr>
        </table></body></html>"""


async def main() -> None:
    upstream = FakeUpstream()
    port = 18099
    ready = asyncio.Event()

    async def _pipe(reader, writer):  # noqa: ANN001
        # Minimal HTTP/1.1 server sufficient for httpx GETs.
        while True:
            line = await reader.readline()
            if not line:
                break
            headers = {}
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
                k, _, v = h.partition(b":")
                headers[k.strip().lower()] = v.strip()
            cl = int(headers.get(b"content-length", b"0"))
            if cl:
                await reader.readexactly(cl)

            collected: dict = {}

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(message):
                if message["type"] == "http.response.start":
                    collected["status"] = message["status"]
                    collected["headers"] = message["headers"]
                elif message["type"] == "http.response.body":
                    collected["body"] = message.get("body", b"")
                    writer.write(b"HTTP/1.1 " + str(collected["status"]).encode() + b" OK\r\n")
                    for k, v in collected["headers"]:
                        writer.write(k + b": " + v + b"\r\n")
                    writer.write(b"content-length: " + str(len(collected["body"])).encode() + b"\r\n\r\n")
                    writer.write(collected["body"])
                    await writer.drain()
                    writer.close()

            scope = {"type": "http", "path": line.split(b" ")[1].decode().split("?")[0],
                     "headers": [], "method": "GET"}
            await upstream.handle(scope, receive, send)
            return

    async def fixed_server():
        srv = await asyncio.start_server(_pipe, "127.0.0.1", port)
        ready.set()
        async with srv:
            await srv.serve_forever()

    srv_task = asyncio.create_task(fixed_server())
    await ready.wait()

    crawler = Crawler(
        base_url=f"http://127.0.0.1:{port}",
        sections={"招标公告": "/tenders", "中标公告": "/awards"},
    )

    # ---- contract 1: max two in flight, full round succeeds
    await crawler.start_round()
    task = crawler._round_task
    await task
    snap = crawler.snapshot()
    print(f"[1] status={snap['status']} max_inflight={upstream.max_inflight} "
          f"records={snap['records_count']} hits={upstream.total_hits}")
    assert snap["status"] == "completed", snap["status"]
    assert upstream.max_inflight <= MAX_IN_FLIGHT
    assert upstream.max_inflight == 2, f"expected overlap of 2, got {upstream.max_inflight}"
    # 2 columns x 2 list pages + unique detail codes (dedup across columns)
    assert snap["records_count"] == 4
    assert not snap["unfinished"]

    # ---- contract 2: 429 triggers site-wide cooldown; retries recover
    upstream.reject_next = 2
    upstream.total_hits = 0
    t0 = time.monotonic()
    await crawler.start_round()
    await crawler._round_task
    elapsed = time.monotonic() - t0
    snap = crawler.snapshot()
    n429 = sum("429" in e for e in snap["events"])
    print(f"[2] status={snap['status']} 429_events={n429} elapsed={elapsed:.1f}s records={snap['records_count']}")
    assert n429 >= 1
    assert elapsed >= upstream.retry_after, "site did not wait for Retry-After"
    assert snap["status"] == "completed", snap["status"]

    # ---- contract 3: stop during cooldown; queued pages never send
    upstream.reject_next = 4
    await crawler.start_round()
    await asyncio.sleep(0.6)  # let the first 429s and cooldown start
    hits_at_stop = upstream.total_hits
    await crawler.stop_round()
    snap = crawler.snapshot()
    print(f"[3] status={snap['status']} hits_at_stop={hits_at_stop} final_hits={upstream.total_hits} "
          f"unfinished={len(snap['unfinished'])}")
    assert snap["status"] == "stopped"
    # Give any wrongly-queued request a chance to leak out.
    await asyncio.sleep(0.5)
    assert upstream.total_hits == hits_at_stop, "requests were sent after stop"
    assert len(snap["unfinished"]) > 0

    # ---- contract 4: a fresh round after stop works (no stale writes)
    upstream.reject_next = 0
    rid = await crawler.start_round()
    await crawler._round_task
    snap = crawler.snapshot()
    print(f"[4] new_round={rid} status={snap['status']} records={snap['records_count']}")
    assert snap["round_id"] == rid
    assert snap["status"] == "completed"
    assert snap["records_count"] == 4
    assert all(rid[:8] in e or "429" not in e for e in snap["events"])

    srv_task.cancel()
    print("\nALL CONTRACT TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
