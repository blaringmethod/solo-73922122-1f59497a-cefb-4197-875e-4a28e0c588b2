# 公共采购公告采集系统

面向招标数据员的公告采集交付件：FastAPI 采集器 + 带限流/延迟开关的本地模拟采购站，
Docker Compose 一键启动，浏览器即可启动采集、查看实时进度、停止任务并下载 JSON。

## 目录结构

```
.
├── docker-compose.yml          # 两服务编排：procurement-site + collector
├── collector/                  # 采集器（FastAPI + httpx + BeautifulSoup）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # API、JSON 下载、采购站同源代理、静态页挂载
│       ├── crawler.py          # 采集引擎：并发闸门、429 全站冷却、停止回收
│       ├── parser.py           # BeautifulSoup 列表/详情解析（失败抛 ParseError）
│       └── static/index.html   # 浏览器控制台（单文件，原生 JS）
├── site/                       # 本地模拟公共采购站
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 栏目/详情页、滑动窗口限流、延迟开关、管理页
│       └── data.py             # 确定性内存数据集（含跨栏目重复编号与 1 个坏详情页）
└── tests/                      # 契约测试与解析器单测（标准库 + venv 依赖）
```

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集控制台 | http://localhost:8000/ | 启动/停止/进度/事件/下载 JSON |
| 采购站（同源代理入口） | http://localhost:8000/site/ | 浏览器入口，经采集器反代到站点 |
| 采购站限流管理 | http://localhost:8000/site/admin | 限流阈值、窗口、延迟毫秒开关 |
| 采购站直连 | http://localhost:8080/ | 仅排障时使用 |

## 采集规则与实现对照

| 需求 | 实现 |
| --- | --- |
| 固定站点两个栏目，遍历分页列表及详情 | `Crawler` 配置 `/tenders`、`/awards`；首页解析 `data-total-pages` 后入队其余列表页，再逐条入队详情页 |
| 按公告编号去重 | 集合 `_seen_codes` 跨栏目去重；同编号只采一次详情，跳过计数在分栏目进度中可见 |
| 提取标题、采购单位、截止日期 | `parser.parse_detail_page`，缺任一字段即 `ParseError` |
| 可下载 JSON | `GET /api/records/download`，附件文件名带轮次 ID |
| 单进程同时只采一轮 | 单 uvicorn worker + `round_lock`；重复启动返回 `409` |
| 最多两个请求在途 | 全站共享 `asyncio.Semaphore(2)`，两个栏目共用 |
| 429 按 Retry-After 整数秒全站暂停 | `_extend_cooldown` 维护单一冷却钟，`_pass_gate` 拦截所有后续请求（含另一栏目与重试）；后续 429 只延长不缩短；已在途请求允许收尾 |
| 每页最多尝试三次 | 列表/详情统一 `_get_parsed_page`，传输错误、4xx/5xx、解析失败均消耗尝试次数 |
| 耗尽/解析失败须列出未完成页，不能报整轮成功 | 失败页进入 `unfinished`；轮次结束状态为 `partial` 而非 `completed` |
| 停止阻止排队/冷却请求，关闭在途请求并回收连接后才显示已停止 | `stop_round` 先置停止位，再 `aclose()` 关闭在途请求与连接池，`await` 轮次协程收尾后状态才变 `stopped`，随后重建客户端允许新轮 |
| 旧响应不能写入新轮 | 每轮生成 `round_id`，冷却唤醒与回写前校验；新轮重建客户端、状态与记录容器 |

轮次状态机：`idle → running → completed | partial | stopped`（停止过程中短暂为 `stopping`）。

## 模拟采购站说明

- 数据集：36 条招标公告（6 个列表页）+ 24 条中标公告（4 个列表页），
  中标公告复用前 24 个招标编号，用于验证跨栏目去重；
- `ZB-2026-010` 详情页故意缺少“采购单位/截止日期”，用于验证三次解析失败后
  进入未完成页面列表、整轮标记为 `partial`；
- 限流为滑动窗口，默认 10 秒窗口 12 个请求，超限返回 `429` 与整数 `Retry-After: 5`；
- `/admin` 可运行时切换限流开关、窗口容量、窗口秒数与每请求延迟毫秒，
  保存即重置计数，也可单独重置计数。

## 推荐演示流程

1. `docker compose up --build`，打开 http://localhost:8000/ ；
2. 直接点“启动一轮采集”：观察到 `partial`——35 条记录、1 个坏详情页在未完成列表中；
3. 在 `/site/admin` 把窗口容量调到 3：再启动一轮，观察控制台“全站冷却剩余”倒计时，
   冷却期间两个栏目均无新请求完成；
4. 冷却进行中点“停止”：状态在 1 秒内变为 `stopped`，排队/冷却中的页面进入未完成列表；
5. 立即再启动一轮：状态与记录完全重置，证明旧轮响应不会写入新轮；
6. 点“下载 JSON”获取结构化结果（含未完成页清单）。

## 本地测试（无需 Docker）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r collector/requirements.txt
.venv/bin/python -I tests/test_parser.py
.venv/bin/python -I tests/test_crawler_contract.py
```

契约测试用内存假上游验证四件事：在途请求峰值恰为 2、429 后整站等待不少于
Retry-After 且重试可恢复、冷却中停止后零请求泄漏、停止后新轮干净无旧数据。
