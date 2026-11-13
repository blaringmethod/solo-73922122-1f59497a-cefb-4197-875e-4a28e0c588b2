"""Mock public procurement site.

Features used to exercise the collector:

* two fixed columns (/tenders, /awards) with paginated lists and detail pages;
* a sliding-window rate limiter returning HTTP 429 with an integer
  Retry-After header, switchable at runtime via /admin;
* an artificial latency switch;
* one intentionally malformed detail page (see data.BROKEN_DETAIL_CODE) to
  demonstrate parse-failure handling and the unfinished-page report.
"""

from __future__ import annotations

import math
import os
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from .data import ANNOUNCEMENTS, BROKEN_DETAIL_CODE, PAGE_SIZE, find_by_code, section_rows

DEFAULT_RATE_LIMIT = int(os.getenv("SITE_RATE_LIMIT", "12"))
DEFAULT_WINDOW = float(os.getenv("SITE_RATE_WINDOW", "10"))
DEFAULT_LATENCY_MS = int(os.getenv("SITE_LATENCY_MS", "150"))
RETRY_AFTER_SECONDS = int(os.getenv("SITE_RETRY_AFTER", "5"))


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
    state["limiter"] = SlidingWindowLimiter(DEFAULT_RATE_LIMIT, DEFAULT_WINDOW)
    yield


app = FastAPI(title="Mock Procurement Site", lifespan=lifespan)


@app.middleware("http")
async def latency_and_limit(request: Request, call_next):
    path = request.url.path
    if path.startswith("/admin") or path == "/healthz":
        return await call_next(request)

    latency_ms = state.get("latency_ms", 0)
    if latency_ms > 0:
        import asyncio

        await asyncio.sleep(latency_ms / 1000.0)

    limiter: SlidingWindowLimiter = state["limiter"]
    if state.get("limiter_enabled") and not limiter.allow():
        return HTMLResponse(
            "<html><body><h1>429 Too Many Requests</h1></body></html>",
            status_code=429,
            headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
        )
    return await call_next(request)


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
button {{ margin-top: 18px; padding: 10px 26px; background: #1e3a8a; color: #fff;
          border: none; border-radius: 8px; cursor: pointer; }}
.notice {{ background: #fef3c7; border: 1px solid #f59e0b; padding: 10px 14px;
          border-radius: 8px; margin-bottom: 16px; }}
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
    <p><a href="/admin">限流 / 延迟开关管理页</a></p>"""
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
    body = f"""<h1>限流 / 延迟开关</h1>
<div class="notice">修改后立即生效。触发 429 时响应头 Retry-After={RETRY_AFTER_SECONDS} 秒，
采集器将对全站两个栏目同时冷却。</div>
<form method="post" action="/admin/config" class="admin">
  <label><input type="checkbox" name="limiter_enabled" {checked}/> 启用限流（滑动窗口）</label>
  <label>窗口内最大请求数</label>
  <input type="number" name="limit" min="1" value="{limiter.limit}"/>
  <label>窗口大小（秒）</label>
  <input type="number" name="window" step="0.5" min="1" value="{limiter.window}"/>
  <label>每请求人工延迟（毫秒，0 表示关闭）</label>
  <input type="number" name="latency_ms" min="0" value="{state.get('latency_ms', 0)}"/>
  <br/><button type="submit">保存并重置计数</button>
</form>
<form method="post" action="/admin/reset" style="margin-top:8px">
  <button type="submit" style="background:#475569">仅重置限流计数</button>
</form>
<p class="notice" style="margin-top:18px">异常详情页：<a href="/notice/{BROKEN_DETAIL_CODE}">
/notice/{BROKEN_DETAIL_CODE}</a>（缺字段，用于验证采集器三次尝试后列入未完成页面）</p>"""
    return HTMLResponse(_page_shell("管理", body))


@app.post("/admin/config")
async def admin_config(
    limiter_enabled: str | None = Form(default=None),
    limit: int = Form(...),
    window: float = Form(...),
    latency_ms: int = Form(...),
):
    limiter: SlidingWindowLimiter = state["limiter"]
    limiter.limit = max(1, limit)
    limiter.window = max(1.0, window)
    limiter.reset()
    state["limiter_enabled"] = limiter_enabled == "on"
    state["latency_ms"] = max(0, latency_ms)
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/reset")
async def admin_reset():
    state["limiter"].reset()
    return RedirectResponse(url="/admin", status_code=303)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "announcements": len(ANNOUNCEMENTS)}
