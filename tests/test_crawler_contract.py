"""End-to-end concurrency / cooldown / redirect / stop tests using the real
Crawler against a fake upstream server (no external processes required).

The fake upstream speaks just enough HTTP/1.1 (including chunked streaming)
to simulate slow-transmit and stalled 429 error bodies, same-site redirect
hops and redirect loops, and it records the arrival time of every request
plus the send time of every 429 so cooldown compliance can be asserted from
the server's point of view.

Run with the project venv:
  python -I tests/test_crawler_contract.py
"""

import asyncio
import sys
import time
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, "/workspace/collector")

from app.crawler import MAX_IN_FLIGHT, MAX_REDIRECT_HOPS, Crawler  # noqa: E402


class _Responder:
    """Minimal HTTP/1.1 response writer (content-length or chunked)."""

    def __init__(self, writer):
        self.writer = writer
        self.chunked = False

    async def start(self, status, headers):
        names = {k.lower() for k, _ in headers}
        self.chunked = b"content-length" not in names
        out = [b"HTTP/1.1 " + str(status).encode() + b" R\r\n"]
        for k, v in headers:
            out.append(k + b": " + v + b"\r\n")
        if self.chunked:
            out.append(b"transfer-encoding: chunked\r\n")
        out.append(b"\r\n")
        self.writer.write(b"".join(out))
        await self.writer.drain()

    async def body(self, data=b"", more=False):
        if self.chunked:
            if data:
                self.writer.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
            if not more:
                self.writer.write(b"0\r\n\r\n")
        elif data:
            self.writer.write(data)
        await self.writer.drain()


class FakeUpstream:
    def __init__(self):
        self.inflight = 0
        self.max_inflight = 0
        self.total_hits = 0
        self.lock = asyncio.Lock()
        # First N requests after arming are answered with 429.
        self.reject_next = 0
        self.retry_after = 3
        # 429 failure modes.
        self.slow_429 = 0.0       # seconds spent dribbling the 429 body
        self.stalled_429 = False  # 429 headers sent, body never completes
        # Redirect behaviour.
        self.redirect_hops = 0    # 302 hops before serving content
        self.loop_paths = set()   # paths that redirect forever
        # Server-side request-time records.
        self.arrivals = []        # (t_monotonic, target) of every request
        self.t_429 = []           # times 429 response headers were sent

    async def handle(self, target, responder: _Responder, reader):
        parsed = urlsplit(target)
        path = parsed.path

        async with self.lock:
            self.inflight += 1
            self.total_hits += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            self.arrivals.append((time.monotonic(), target))
        assert self.inflight <= MAX_IN_FLIGHT, f"concurrency violated: {self.inflight} in flight"

        try:
            try:
                hop = int(parse_qs(parsed.query).get("hop", ["0"])[0])
            except ValueError:
                hop = 0

            if path in self.loop_paths or hop < self.redirect_hops:
                sep = "&" if parsed.query else ""
                location = f"{path}?{parsed.query}{sep}hop={hop + 1}"
                await responder.start(
                    302, [(b"location", location.encode()), (b"content-length", b"0")]
                )
                await responder.body(b"")
                return

            await asyncio.sleep(0.15)  # keep requests overlapping

            if self.reject_next > 0:
                self.reject_next -= 1
                # Chunked 429: headers (with Retry-After) go out immediately.
                await responder.start(
                    429,
                    [(b"retry-after", str(self.retry_after).encode()),
                     (b"content-type", b"text/html")],
                )
                self.t_429.append(time.monotonic())
                if self.stalled_429:
                    await responder.body(b"<html>429 partial", more=True)
                    # The stream stalls here; it unblocks only when the
                    # client closes the connection (EOF on the reader).
                    try:
                        await asyncio.wait_for(reader.read(1), timeout=10)
                    except asyncio.TimeoutError:
                        pass
                    return
                if self.slow_429 > 0:
                    await responder.body(b"<html>429 ", more=True)
                    await asyncio.sleep(self.slow_429)
                    await responder.body(b"slow error body</html>")
                    return
                await responder.body(b"<html>429</html>")
                return

            if path.startswith("/tenders") or path.startswith("/awards"):
                body = self._list_html(path)
            elif path.startswith("/notice/"):
                code = path.rsplit("/", 1)[-1]
                body = self._detail_html(code)
            else:
                await responder.start(404, [(b"content-type", b"text/html"),
                                            (b"content-length", b"22")])
                await responder.body(b"<html>not found</html>")
                return

            data = body.encode()
            await responder.start(200, [(b"content-type", b"text/html; charset=utf-8"),
                                        (b"content-length", str(len(data)).encode())])
            await responder.body(data)
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
            + '</tbody></table><nav class="pager" data-total-pages="2"></nav>'
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


async def _pipe(upstream, reader, writer):
    try:
        request_line = await reader.readline()
        if not request_line:
            return
        target = request_line.split(b" ")[1].decode()
        while True:  # drain request headers
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
        await upstream.handle(target, _Responder(writer), reader)
    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


def _assert_cooldown_respected(upstream, slack=0.3):
    """Every request arriving after a 429 must have waited Retry-After."""
    for t429 in upstream.t_429:
        later = [t for (t, _) in upstream.arrivals if t > t429 + 0.05]
        for t in later:
            assert t - t429 >= upstream.retry_after - slack, (
                f"request arrived {t - t429:.2f}s after a 429 "
                f"(Retry-After={upstream.retry_after}s)"
            )


async def main() -> None:
    upstream = FakeUpstream()
    port = 18099
    ready = asyncio.Event()

    async def fixed_server():
        srv = await asyncio.start_server(
            lambda r, w: _pipe(upstream, r, w), "127.0.0.1", port
        )
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
    await crawler._round_task
    snap = crawler.snapshot()
    print(f"[1] status={snap['status']} max_inflight={upstream.max_inflight} "
          f"records={snap['records_count']} hits={upstream.total_hits}")
    assert snap["status"] == "completed", snap["status"]
    assert upstream.max_inflight <= MAX_IN_FLIGHT
    assert upstream.max_inflight == 2, f"expected overlap of 2, got {upstream.max_inflight}"
    # 2 columns x 2 list pages + unique detail codes (dedup across columns)
    assert snap["records_count"] == 4
    assert not snap["unfinished"]
    assert len(snap["request_log"]) == upstream.total_hits

    # ---- contract 2: 429 triggers site-wide cooldown; retries recover
    upstream.reject_next = 2
    upstream.retry_after = 3
    t0 = time.monotonic()
    await crawler.start_round()
    await crawler._round_task
    elapsed = time.monotonic() - t0
    snap = crawler.snapshot()
    n429 = sum(1 for e in snap["request_log"] if e["status"] == 429)
    print(f"[2] status={snap['status']} 429s={n429} elapsed={elapsed:.1f}s records={snap['records_count']}")
    assert n429 >= 1
    assert elapsed >= upstream.retry_after, "site did not wait for Retry-After"
    assert snap["status"] == "completed", snap["status"]
    _assert_cooldown_respected(upstream)

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

    # ---- contract 5: slow-dribbled 429 body must not delay the cooldown
    upstream.arrivals.clear()
    upstream.t_429.clear()
    upstream.reject_next = 1
    upstream.retry_after = 2
    upstream.slow_429 = 1.5  # error body takes 1.5s to trickle in
    await crawler.start_round()
    await crawler._round_task
    snap = crawler.snapshot()
    assert snap["status"] == "completed", snap["status"]
    assert len(upstream.t_429) == 1
    t429 = upstream.t_429[0]
    later = [t for (t, _) in upstream.arrivals if t > t429 + 0.05]
    assert later, "no requests followed the 429"
    gap = min(later) - t429
    e429 = [e for e in snap["request_log"] if e["status"] == 429]
    print(f"[5] slow-429 gap_to_next_request={gap:.2f}s "
          f"(retry_after=2s, body_dribble=1.5s) 429_log_duration={e429[0]['duration_ms']}ms")
    # Cooldown ran from the 429 *headers*: the next request leaves after
    # ~Retry-After, not after Retry-After + the 1.5s body dribble.
    assert gap >= upstream.retry_after - 0.3
    assert gap < upstream.retry_after + 1.0, "cooldown started only after the slow body"
    # The 429 log entry measures header arrival, not full-body time.
    assert e429 and e429[0]["duration_ms"] < 1000
    _assert_cooldown_respected(upstream)

    # ---- contract 6: stalled 429 body (never completes) must not hang the round
    upstream.arrivals.clear()
    upstream.t_429.clear()
    upstream.slow_429 = 0.0
    upstream.stalled_429 = True
    upstream.reject_next = 1
    upstream.retry_after = 2
    t0 = time.monotonic()
    await crawler.start_round()
    await crawler._round_task
    elapsed = time.monotonic() - t0
    snap = crawler.snapshot()
    print(f"[6] stalled-429 status={snap['status']} elapsed={elapsed:.1f}s "
          f"(client timeout is 15s)")
    assert snap["status"] == "completed", snap["status"]
    # The crawler closed the error response at the headers; it never waited
    # for the stalled body or the 15s request timeout.
    assert elapsed < 10, f"stalled 429 body delayed the round ({elapsed:.1f}s)"
    _assert_cooldown_respected(upstream)
    upstream.stalled_429 = False

    # ---- contract 7: redirect hops interleaved with cooldowns obey the gate
    upstream.arrivals.clear()
    upstream.t_429.clear()
    upstream.redirect_hops = 1   # every page is reached via one relative 302
    upstream.reject_next = 3     # some 429s land mid-chain
    upstream.retry_after = 2
    await crawler.start_round()
    await crawler._round_task
    snap = crawler.snapshot()
    n_redirect = sum(1 for e in snap["request_log"] if e["status"] == 302)
    print(f"[7] status={snap['status']} records={snap['records_count']} "
          f"redirects={n_redirect} 429s={len(upstream.t_429)} hits={len(upstream.arrivals)}")
    assert snap["status"] == "completed", snap["status"]
    assert snap["records_count"] == 4
    assert n_redirect > 0, "redirect hops were not exercised"
    assert any("hop=" in t for (_, t) in upstream.arrivals)
    assert len(upstream.t_429) >= 1
    _assert_cooldown_respected(upstream)
    upstream.redirect_hops = 0

    # ---- contract 8: redirect loop -> unfinished after exactly 3 attempts;
    #      redirect hops never reset the per-page retry counter
    upstream.arrivals.clear()
    upstream.t_429.clear()
    upstream.loop_paths = {"/notice/ZB-2026-001"}
    await crawler.start_round()
    await crawler._round_task
    snap = crawler.snapshot()
    loop_hits = sum(1 for (_, t) in upstream.arrivals
                    if t.startswith("/notice/ZB-2026-001"))
    entry = [u for u in snap["unfinished"] if u["page"] == "ZB-2026-001"]
    print(f"[8] status={snap['status']} records={snap['records_count']} "
          f"loop_hits={loop_hits} attempts={entry[0]['attempts'] if entry else '-'}")
    assert snap["status"] == "partial", snap["status"]
    assert snap["records_count"] == 3
    assert len(entry) == 1
    assert entry[0]["attempts"] == 3, "redirects must not reset the retry count"
    assert "redirect" in entry[0]["reason"].lower()
    assert loop_hits == 3 * (MAX_REDIRECT_HOPS + 1)
    upstream.loop_paths = set()

    srv_task.cancel()
    print("\nALL CONTRACT TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
