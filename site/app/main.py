"""Mock public procurement site.

Features used to exercise the collector:

* two fixed columns (/tenders, /awards) with paginated lists and detail pages;
* a sliding-window rate limiter returning HTTP 429 with an integer
  Retry-After header, switchable at runtime via /admin;
* 429 failure-mode switches: a slow-transmit error body (dribbled over a
  configurable number of milliseconds) and a stalled error body (headers
  sent, body never completes) — a correct collector starts its cooldown
  from the response headers and closes such responses immediately;
* a same-site redirect switch: every content page first answers N relative
  302 hops, so redirect chains interleave with rate-limit cooldowns;
* an artificial latency switch;
* a site-side request-time log (wall clock + path + status) shown on /admin,
  so an operator can verify that no request arrives during a cooldown;
* one intentionally malformed detail page (see data.BROKEN_DETAIL_CODE) to
  demonstrate parse-failure handling and the unfinished-page report.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from .data import ANNOUNCEMENTS, BROKEN_DETAIL_CODE, PAGE_SIZE, find_by_code, section_rows

DEFAULT_RATE_LIMIT = int(os.getenv("SITE_RATE_LIMIT", "12"))
DEFAULT_WINDOW = float(os.getenv("SITE_RATE_WINDOW", "10"))
DEFAULT_LATENCY_MS = int(os.getenv("SITE_LATENCY_MS", "150"))
RETRY_AFTER_SECONDS = int(os.getenv("SITE_RETRY_AFTER", "5"))
DEFAULT_SLOW_429_MS = int(os.getenv("SITE_SLOW_429_MS", "0"))
DEFAULT_STALLED_429 = os.getenv("SITE_STALLED_429", "false").lower() == "true"
DEFAULT_REDIRECT_HOPS = int(os.getenv("SITE_REDIRECT_HOPS", "0"))
REQUEST_LOG_LIMIT = 200


class SlidingWindowLimiter:
    def __init__(self, limit: int, window_seconds: float):
        self.limit = limit
        self.window = window_seconds
        self.hits: deque[float] = deque()

    def allow(self) -> bool:
        now = time.monotonic()
        while self.hits and now - self.hits[0] >= self.window:
            self.hits.popleft()
        if len(self.hits) >= self.limit:
            return False
        self.hits.append(now)
        return True

    def reset(self) -> None:
        self.hits.clear()


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["limiter_enabled"] = os.getenv("SITE_RATE_ENABLED", "true").lower() == "true"
    state["latency_ms"] = DEFAULT_LATENCY_MS
    state["slow_429_ms"] = DEFAULT_SLOW_429_MS
    state["stalled_429"] = DEFAULT_STALLED_429
    state["redirect_hops"] = DEFAULT_REDIRECT_HOPS
    state["limiter"] = SlidingWindowLimiter(DEFAULT_RATE_LIMIT, DEFAULT_WINDOW)
    state["request_log"] = deque(maxlen=REQUEST_LOG_LIMIT)
    yield


app = FastAPI(title="Mock Procurement Site", lifespan=lifespan)


def _record_request(request: Request, status: int) -> None:
    target = request.url.path
    if request.url.query:
        target += "?" + request.url.query
    state["request_log"].append(
        {
            "at": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "path": target,
            "status": status,
        }
    )


def _too_many_requests_response():
    """429 with an integer Retry-After; the body may dribble or stall."""
    headers = {"Retry-After": str(RETRY_AFTER_SECONDS)}
    body = (
        "<html><body><h1>429 Too Many Requests</h1>"
        f"<p>Retry-After: {RETRY_AFTER_SECONDS}s — slow_body="
        f"{state.get('slow_429_ms', 0)}ms stalled={state.get('stalled_429', False)}</p>"
        "</body></html>"
    ).encode()

    if state.get("stalled_429"):
        # Headers (with Retry-After) go out immediately, one body chunk
        # follows, then the stream stalls and never completes. Starlette's
        # StreamingResponse cancels the generator when the client closes
        # the connection, so a correct collector unblocks this at once.
        async def stalled_body():
            yield b"<html><body><h1>429 Too Many Requests</h1>"
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise
            yield b"</body></html>"

        return StreamingResponse(
            stalled_body(), status_code=429, headers=headers, media_type="text/html"
        )

    slow_ms = state.get("slow_429_ms", 0)
    if slow_ms > 0:
        # Headers go out immediately; the error body dribbles in slowly.
        async def dribble_body():
            half = len(body) // 2
            yield body[:half]
            await asyncio.sleep(slow_ms / 1000.0)
            yield body[half:]

        return StreamingResponse(
            dribble_body(), status_code=429, headers=headers, media_type="text/html"
        )

    return HTMLResponse(body, status_code=429, headers=headers)


def _redirect_hop_response(request: Request, next_hop: int) -> RedirectResponse:
    # Relative Location: stays on the same site both directly and through
    # the collector's /site/ proxy.
    params = dict(request.query_params)
    params["_hop"] = str(next_hop)
    return RedirectResponse(
        url=f"{request.url.path}?{urlencode(params)}", status_code=302
    )


@app.middleware("http")
async def traffic_controls(request: Request, call_next):
    path = request.url.path
    if path.startswith("/admin") or path == "/healthz":
        return await call_next(request)

    latency_ms = state.get("latency_ms", 0)
    if latency_ms > 0:
        await asyncio.sleep(latency_ms / 1000.0)

    limiter: SlidingWindowLimiter = state["limiter"]
    if state.get("limiter_enabled") and not limiter.allow():
        _record_request(request, 429)
        return _too_many_requests_response()

    # Same-site redirect hops: every content page is first reached through N
    # relative 302s. Each hop is a real request, so it also consumes the
    # rate-limit window — redirect chains and 429 cooldowns interleave.
    hops = state.get("redirect_hops", 0)
    if hops > 0:
        try:
            current_hop = int(request.query_params.get("_hop", "0"))
        except ValueError:
            current_hop = 0
        if current_hop < hops:
            _record_request(request, 302)
            return _redirect_hop_response(request, current_hop + 1)

    response = await call_next(request)
    _record_request(request, response.status_code)
    return response


# ----------------------------------------------------------------- HTML pages
def _page_shell(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"/>
<title>{title} - 公共采购站（模拟）</title>
<style>
body {{ font-family: "PingFang SC", "Microsoft YaHei", sans-serif; margin: 0; background: #f1f5f9; }}
header {{ background: #1e3a8a; color: #fff; padding: 16px 28px; }}
nav a {{ color: #bfdbfe; margin-right: 18px; text-decoration: none; }}
main {{ max-width: 980px; margin: 24px auto; background: #fff; padding: 24px 30px;
        border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,.08); }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border-bottom: 1px solid #e2e8f0; padding: 10px 8px; text-align: left; }}
th {{ background: #f8fafc; }}
.code {{ font-family: monospace; }}
.pager a {{ display: inline-block; margin-right: 8px; padding: 4px 10px;
            border: 1px solid #cbd5e1; border-radius: 6px; text-decoration: none; color: #1e3a8a; }}
.pager a.current {{ background: #1e3a8a; color: #fff; }}
.detail-table th {{ width: 130px; background: #f8fafc; }}
.admin label {{ display: block; margin: 12px 0 4px; font-weight: 600; }}
.admin input {{ padding: 8px; width: 260px; border: 1px solid #cbd5e1; border-radius: 6px; }}
.admin input[type=checkbox] {{ width: auto; }}
button {{ margin-top: 18px; padding: 10px 26px; background: #1e3a8a; color: #fff;
          border: none; border-radius: 8px; cursor: pointer; }}
.notice {{ background: #fef3c7; border: 1px solid #f59e0b; padding: 10px 14px;
          border-radius: 8px; margin-bottom: 16px; }}
.reqlog {{ font-family: monospace; font-size: 12px; }}
.reqlog td.s429 {{ color: #b91c1c; font-weight: 700; }}
.reqlog td.s302 {{ color: #a16207; font-weight: 700; }}
</style></head><body>
<header><strong>公共采购站（本地模拟）</strong>
<nav><a href="/tenders">招标公告</a><a href="/awards">中标公告</a><a href="/admin">限流/延迟开关</a></nav>
</header><main>{body}</main></body></html>"""


def _render_list(section: str, section_title: str, page: int) -> HTMLResponse:
    rows = section_rows(section)
    total_pages = max(1, math.ceil(len(rows) / PAGE_SIZE))
    page = max(1, min(page, total_pages))
    window_rows = rows[(page - 1) * PAGE_SIZE : page * PAGE_SIZE]

    trs = "".join(
        f"""<tr class="announcement">
        <td class="code">{a.code}</td>
        <td><a href="/notice/{a.code}">{a.title}</a></td>
        <td>{a.unit}</td><td>{a.deadline}</td></tr>"""
        for a in window_rows
    )
    page_links = "".join(
        f'<a class="page-link {"current" if n == page else ""}" '
        f'href="/{section}?page={n}">{n}</a>'
        for n in range(1, total_pages + 1)
    )
    body = f"""<h1>{section_title}</h1>
<table id="announcement-list">
<thead><tr><th>公告编号</th><th>标题</th><th>采购单位</th><th>截止日期</th></tr></thead>
<tbody>{trs}</tbody></table>
<nav class="pager" data-total-pages="{total_pages}" style="margin-top:18px">{page_links}</nav>"""
    return HTMLResponse(_page_shell(section_title, body))


@app.get("/", response_class=HTMLResponse)
async def home():
    body = """<h1>欢迎访问公共采购站（本地模拟）</h1>
    <p>本站用于采集器联调，提供两个固定栏目：</p>
    <ul><li><a href="/tenders">招标公告（/tenders）</a></li>
    <li><a href="/awards">中标公告（/awards）</a></li></ul>
    <p><a href="/admin">限流 / 延迟 / 429 故障 / 跳转开关管理页</a></p>"""
    return HTMLResponse(_page_shell("首页", body))


@app.get("/tenders", response_class=HTMLResponse)
async def tenders(page: int = 1):
    return _render_list("tender", "招标公告", page)


@app.get("/awards", response_class=HTMLResponse)
async def awards(page: int = 1):
    return _render_list("award", "中标公告", page)


@app.get("/notice/{code}", response_class=HTMLResponse)
async def notice_detail(code: str):
    item = find_by_code(code)
    if item is None:
        return HTMLResponse(_page_shell("未找到", "<h1>404 公告不存在</h1>"), status_code=404)

    # Deliberately malformed page: missing procurement unit and deadline rows.
    if code == BROKEN_DETAIL_CODE:
        body = f"""<h1 id="detail-title">{item.title}</h1>
<table class="detail-table">
<tr><th>公告编号</th><td>{item.code}</td></tr>
<tr><th>栏目</th><td>{item.section}</td></tr>
</table>
<p class="notice">⚠ 测试用异常页：缺少“采购单位”和“截止日期”字段，采集器解析应失败。</p>"""
        return HTMLResponse(_page_shell("公告详情", body))

    body = f"""<h1 id="detail-title">{item.title}</h1>
<table class="detail-table">
<tr><th>公告编号</th><td>{item.code}</td></tr>
<tr><th>采购单位</th><td>{item.unit}</td></tr>
<tr><th>截止日期</th><td>{item.deadline}</td></tr>
<tr><th>所属栏目</th><td>{item.section}</td></tr>
</table>
<p><a href="/{item.section}s">返回列表</a> ｜ 数据集总数：{len(ANNOUNCEMENTS)}</p>"""
    return HTMLResponse(_page_shell("公告详情", body))


# ------------------------------------------------------------------ admin API
@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    limiter: SlidingWindowLimiter = state["limiter"]
    checked = "checked" if state.get("limiter_enabled") else ""
    stalled_checked = "checked" if state.get("stalled_429") else ""
    log_rows = "".join(
        f'<tr><td>{e["at"]}</td><td>{e["path"]}</td>'
        f'<td class="s{e["status"]}">{e["status"]}</td></tr>'
        for e in list(state["request_log"])[-40:][::-1]
    ) or '<tr><td colspan="3">暂无请求</td></tr>'
    # Form actions are relative so they work both directly (:8080) and
    # through the collector's same-origin proxy (/site/admin).
    body = f"""<h1>限流 / 延迟 / 429 故障 / 跳转开关</h1>
<div class="notice">修改后立即生效。触发 429 时响应头 Retry-After={RETRY_AFTER_SECONDS} 秒，
采集器应在收到 429 <b>响应头</b>时即开始全站冷却并关闭该错误响应（不等待慢传/断流的正文），
冷却期间两个栏目的重试与同站跳转的每一跳都不得发出新请求。</div>
<form method="post" action="admin/config" class="admin">
  <label><input type="checkbox" name="limiter_enabled" {checked}/> 启用限流（滑动窗口）</label>
  <label>窗口内最大请求数</label>
  <input type="number" name="limit" min="1" value="{limiter.limit}"/>
  <label>窗口大小（秒）</label>
  <input type="number" name="window" step="0.5" min="1" value="{limiter.window}"/>
  <label>每请求人工延迟（毫秒，0 表示关闭）</label>
  <input type="number" name="latency_ms" min="0" value="{state.get('latency_ms', 0)}"/>
  <label>429 正文慢传（毫秒，0 表示关闭；响应头仍立即返回）</label>
  <input type="number" name="slow_429_ms" min="0" step="100" value="{state.get('slow_429_ms', 0)}"/>
  <label><input type="checkbox" name="stalled_429" {stalled_checked}/> 429 断流（响应头发出后正文永不传完）</label>
  <label>同站跳转次数（每个内容页先相对 302 跳转 N 次，0 表示关闭）</label>
  <input type="number" name="redirect_hops" min="0" max="5" value="{state.get('redirect_hops', 0)}"/>
  <br/><button type="submit">保存并重置计数</button>
</form>
<form method="post" action="admin/reset" style="margin-top:8px">
  <button type="submit" style="background:#475569">仅重置限流计数</button>
</form>
<p class="notice" style="margin-top:18px">异常详情页：<a href="/notice/{BROKEN_DETAIL_CODE}">
/notice/{BROKEN_DETAIL_CODE}</a>（缺字段，用于验证采集器三次尝试后列入未完成页面）</p>
<h2>请求时间记录（站点侧，最新在前，仅显示最近 40 条）</h2>
<table class="reqlog"><thead><tr><th>时间(UTC)</th><th>路径</th><th>状态</th></tr></thead>
<tbody>{log_rows}</tbody></table>"""
    return HTMLResponse(_page_shell("管理", body))


@app.post("/admin/config")
async def admin_config(
    limiter_enabled: str | None = Form(default=None),
    limit: int = Form(...),
    window: float = Form(...),
    latency_ms: int = Form(...),
    slow_429_ms: int = Form(default=0),
    stalled_429: str | None = Form(default=None),
    redirect_hops: int = Form(default=0),
):
    limiter: SlidingWindowLimiter = state["limiter"]
    limiter.limit = max(1, limit)
    limiter.window = max(1.0, window)
    limiter.reset()
    state["limiter_enabled"] = limiter_enabled == "on"
    state["latency_ms"] = max(0, latency_ms)
    state["slow_429_ms"] = max(0, slow_429_ms)
    state["stalled_429"] = stalled_429 == "on"
    state["redirect_hops"] = min(5, max(0, redirect_hops))
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/reset")
async def admin_reset():
    state["limiter"].reset()
    return RedirectResponse(url="/admin", status_code=303)


@app.get("/admin/log")
async def admin_log():
    """Site-side request-time log as JSON (newest last)."""
    return {"retry_after_seconds": RETRY_AFTER_SECONDS, "requests": list(state["request_log"])}


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "announcements": len(ANNOUNCEMENTS)}
