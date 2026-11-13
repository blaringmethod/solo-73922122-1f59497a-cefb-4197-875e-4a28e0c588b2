"""Crawler engine.

Concurrency and rate-limit contract:

* Only one collection round may run at a time (``round_lock``).
* At most two HTTP requests are in flight at any moment, across both columns
  (a single shared ``asyncio.Semaphore(2)``).
* A 429 with a ``Retry-After`` integer header pauses the *whole site*: every
  later request, including the retry itself and requests of the other column,
  waits in :meth:`_pass_gate`. Further 429s only extend the pause. Requests
  already in flight are allowed to finish.
* Every page (list or detail) is attempted at most three times. Exhausted
  retries and permanently unparseable pages are recorded as unfinished, so a
  round is never reported as successful in that case.
* Stop blocks queued/cooling requests from being sent, closes in-flight
  requests and the connection pool, and only then flips the state to stopped
  and permits a new round. A round id guards against a late response from the
  old round being written into the new one.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import httpx

from . import parser
from .parser import ListParseResult, ParseError

MAX_ATTEMPTS = 3
MAX_IN_FLIGHT = 2
GATE_POLL_SECONDS = 0.25
EVENT_LOG_LIMIT = 200
DEFAULT_RETRY_AFTER_SECONDS = 5


class RoundStopped(Exception):
    """Raised inside a round when cooperative stop has been requested."""


@dataclass
class SectionProgress:
    list_total_pages: int = 0
    list_completed: int = 0
    list_failed: int = 0
    detail_enqueued: int = 0
    detail_completed: int = 0
    detail_failed: int = 0
    duplicated_skipped: int = 0


@dataclass
class UnfinishedPage:
    section: str
    page_type: str  # "list" or "detail"
    page: str  # list page number, or announcement code for detail pages
    url: str
    reason: str
    attempts: int


@dataclass
class RoundState:
    round_id: str | None = None
    status: str = "idle"  # idle | running | stopping | stopped | completed | partial
    base_url: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    stop_requested: bool = False
    pages_enqueued: int = 0
    pages_completed: int = 0
    pages_failed: int = 0
    records_count: int = 0
    cooldown_remaining_seconds: float = 0.0
    sections: dict[str, SectionProgress] = field(default_factory=dict)
    unfinished: list[UnfinishedPage] = field(default_factory=list)
    events: list[str] = field(default_factory=list)


class Crawler:
    def __init__(self, base_url: str, sections: dict[str, str], timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        # section display name -> list path
        self.sections = sections
        self.timeout = timeout

        self._round_lock = asyncio.Lock()
        self._round_task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

        # Per-round runtime flags (reset when a round starts).
        self._round_id: str | None = None
        self._stop_event = asyncio.Event()
        self._inflight = asyncio.Semaphore(MAX_IN_FLIGHT)

        # Site-wide 429 cooldown (one clock for the whole crawler).
        self._cooldown_until = 0.0

        self.state = RoundState(
            base_url=self.base_url,
            sections={name: SectionProgress() for name in sections},
        )
        self._records: dict[str, dict] = {}
        self._seen_codes: set[str] = set()

    # ------------------------------------------------------------------ state
    def snapshot(self) -> dict:
        now = time.monotonic()
        remaining = max(0.0, self._cooldown_until - now)
        self.state.cooldown_remaining_seconds = round(remaining, 1)
        return asdict(self.state)

    def records(self) -> list[dict]:
        return list(self._records.values())

    def _log(self, message: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.state.events.append(f"{stamp} {message}")
        del self.state.events[:-EVENT_LOG_LIMIT]

    # ------------------------------------------------------------- lifecycle
    async def start_round(self) -> str:
        async with self._round_lock:
            if self._round_task is not None and not self._round_task.done():
                raise RuntimeError("a collection round is already running")

            round_id = uuid.uuid4().hex[:12]
            self._round_id = round_id
            self._stop_event = asyncio.Event()
            self._inflight = asyncio.Semaphore(MAX_IN_FLIGHT)
            self._cooldown_until = 0.0
            self._records: dict[str, dict] = {}
            self._seen_codes: set[str] = set()

            self.state = RoundState(
                round_id=round_id,
                status="running",
                base_url=self.base_url,
                started_at=datetime.now(timezone.utc).isoformat(),
                sections={name: SectionProgress() for name in self.sections},
            )
            # Limits created before this round do not apply to the new one:
            # a fresh client also guarantees a fresh connection pool.
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "procurement-collector/1.0"},
                follow_redirects=True,
            )
            self._log(f"round {round_id} started; columns={list(self.sections)}")
            self._round_task = asyncio.create_task(self._run_round(round_id))
            return round_id

    async def stop_round(self) -> None:
        async with self._round_lock:
            task = self._round_task
            client = self._client
            if task is None or task.done():
                return

            self._stop_event.set()
            self.state.stop_requested = True
            self.state.status = "stopping"
            self._log("stop requested: queued/cooling requests will not be sent")

            # Close in-flight requests and release the connection pool. The
            # round coroutine catches the resulting errors and finishes.
            if client is not None:
                try:
                    await client.aclose()
                except Exception:  # pragma: no cover - defensive
                    pass

            # Wait until the round coroutine has fully wound down.
            try:
                await task
            except asyncio.CancelledError:  # pragma: no cover - defensive
                raise
            except Exception:
                pass

            # Rebuild a closed client so the next round owns a clean pool.
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout),
                headers={"User-Agent": "procurement-collector/1.0"},
                follow_redirects=True,
            )
            self._log("in-flight requests closed and connections recycled; round stopped")

    # ----------------------------------------------------------------- round
    async def _run_round(self, round_id: str) -> None:
        # Mutable holder so both the coordinator and workers can track the
        # number of jobs queued or being processed.
        pending = {"count": 0}
        queue: asyncio.Queue = asyncio.Queue()
        for name, path in self.sections.items():
            queue.put_nowait(
                {"section": name, "kind": "list", "page": 1, "url": path, "attempts": 0}
            )
            pending["count"] += 1
            self.state.pages_enqueued += 1

        workers = [
            asyncio.create_task(self._worker(round_id, queue, i, pending))
            for i in range(MAX_IN_FLIGHT)
        ]

        try:
            # Coordinator: finish once every job (and every job it spawned)
            # has been settled. Stop interrupts the wait promptly.
            while pending["count"] > 0 and not self._stop_event.is_set():
                await asyncio.sleep(0.1)
            for w in workers:
                queue.put_nowait(None)
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            for w in workers:
                if not w.done():
                    w.cancel()
            for w in workers:
                try:
                    await w
                except asyncio.CancelledError:
                    pass
            # Any job still queued when the round ends is unfinished.
            while not queue.empty():
                leftover = queue.get_nowait()
                if isinstance(leftover, dict):
                    self._mark_unfinished(
                        leftover, "round stopped before request was sent", leftover["attempts"]
                    )
            # Close the client used by this round (idempotent if stopped).
            if self._client is not None:
                try:
                    await self._client.aclose()
                except Exception:
                    pass

        if round_id != self._round_id:
            # A new round superseded this one; do not touch its state.
            return

        self.state.finished_at = datetime.now(timezone.utc).isoformat()
        if self._stop_event.is_set():
            self.state.status = "stopped"
        elif self.state.pages_failed:
            self.state.status = "partial"
        else:
            self.state.status = "completed"
        self.state.records_count = len(self._records)
        self._log(
            f"round ended: status={self.state.status}, "
            f"records={self.state.records_count}, unfinished={len(self.state.unfinished)}"
        )

    async def _worker(
        self, round_id: str, queue: asyncio.Queue, worker_no: int, pending: dict
    ) -> None:
        while True:
            job = await queue.get()
            try:
                if job is None:
                    return
                if self._stop_event.is_set():
                    self._mark_unfinished(job, "round stopped before request was sent", job["attempts"])
                    pending["count"] -= 1
                    return
                try:
                    if job["kind"] == "list":
                        await self._handle_list(round_id, job, queue, pending)
                    else:
                        await self._handle_detail(round_id, job, pending)
                except RoundStopped:
                    # Stop while waiting/cooling: no further request is sent.
                    self._mark_unfinished(job, "round stopped during cooldown/queue wait", job["attempts"])
                    pending["count"] -= 1
                    return
            finally:
                queue.task_done()

    # -------------------------------------------------------------- handlers
    async def _handle_list(
        self, round_id: str, job: dict, queue: asyncio.Queue, pending: dict
    ) -> None:
        section = job["section"]
        progress = self.state.sections[section]
        try:
            result: ListParseResult = await self._get_parsed_page(
                round_id, job, parser.parse_list_page
            )
        except RoundStopped:
            raise
        except Exception as exc:
            progress.list_failed += 1
            self.state.pages_failed += 1
            self._mark_unfinished(job, str(exc), MAX_ATTEMPTS)
            self._log(f"[{section}] list page {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return

        progress.list_completed += 1
        self.state.pages_completed += 1

        spawned = 0
        if job["page"] == 1:
            progress.list_total_pages = result.total_pages
            for page_no in range(2, result.total_pages + 1):
                new_job = {
                    "section": section,
                    "kind": "list",
                    "page": page_no,
                    "url": self._list_url(section, page_no),
                    "attempts": 0,
                }
                queue.put_nowait(new_job)
                spawned += 1
                self.state.pages_enqueued += 1

        for item in result.items:
            if item.code in self._seen_codes:
                progress.duplicated_skipped += 1
                continue
            self._seen_codes.add(item.code)
            queue.put_nowait(
                {
                    "section": section,
                    "kind": "detail",
                    "page": item.code,
                    "url": item.url,
                    "attempts": 0,
                }
            )
            spawned += 1
            progress.detail_enqueued += 1
            self.state.pages_enqueued += 1

        # This job is settled; the jobs it spawned keep the round alive.
        pending["count"] += spawned - 1

    async def _handle_detail(self, round_id: str, job: dict, pending: dict) -> None:
        section = job["section"]
        progress = self.state.sections[section]
        try:
            detail = await self._get_parsed_page(round_id, job, parser.parse_detail_page)
        except RoundStopped:
            raise
        except Exception as exc:
            progress.detail_failed += 1
            self.state.pages_failed += 1
            self._mark_unfinished(job, str(exc), MAX_ATTEMPTS)
            self._log(f"[{section}] detail {job['page']} unfinished: {exc}")
            pending["count"] -= 1
            return

        progress.detail_completed += 1
        self.state.pages_completed += 1
        self._records[detail["code"]] = {
            "announcement_code": detail["code"],
            "title": detail["title"],
            "procurement_unit": detail["unit"],
            "deadline": detail["deadline"],
            "section": section,
            "detail_url": self._absolute(job["url"]),
        }
        self.state.records_count = len(self._records)
        pending["count"] -= 1

    # ------------------------------------------------------------- transport
    async def _get_parsed_page(self, round_id: str, job: dict, parse_func):
        """Fetch one URL with up to three attempts, then parse it.

        Transport errors and HTTP error statuses (including 429 after cooldown)
        consume attempts; a page whose HTML never parses within three attempts
        surfaces the last :class:`ParseError`.
        """
        last_error: Exception | None = None
        while job["attempts"] < MAX_ATTEMPTS:
            job["attempts"] += 1
            await self._pass_gate(round_id)  # may raise RoundStopped
            if round_id != self._round_id:
                raise RoundStopped("round superseded before request")
            try:
                async with self._inflight:
                    assert self._client is not None
                    response = await self._client.get(job["url"])
            except httpx.RequestError as exc:
                last_error = exc
                if self._stop_event.is_set():
                    raise RoundStopped("in-flight request closed by stop")
                self._log(
                    f"request error ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}: {exc}"
                )
                continue

            # The request itself has finished: 429 only gates *later* traffic.
            if response.status_code == 429:
                wait_seconds = self._retry_after_seconds(response)
                self._extend_cooldown(wait_seconds)
                last_error = httpx.HTTPStatusError(
                    f"429 Too Many Requests; site-wide cooldown {wait_seconds}s",
                    request=response.request,
                    response=response,
                )
                self._log(
                    f"429 on {job['url']}; whole site pauses {wait_seconds}s "
                    f"(attempt {job['attempts']}/{MAX_ATTEMPTS})"
                )
                continue

            if response.status_code >= 400:
                last_error = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                self._log(
                    f"HTTP {response.status_code} ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}"
                )
                continue

            try:
                return parse_func(response.text)
            except ParseError as exc:
                last_error = exc
                self._log(
                    f"parse error ({job['attempts']}/{MAX_ATTEMPTS}) {job['url']}: {exc}"
                )
                continue

        raise last_error if last_error else ParseError("unknown failure")

    async def _pass_gate(self, round_id: str) -> None:
        """Block while stop is requested or a site-wide cooldown is active."""
        while True:
            if self._stop_event.is_set() or round_id != self._round_id:
                raise RoundStopped("stop requested before request could be sent")
            remaining = self._cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.sleep(min(GATE_POLL_SECONDS, remaining))
            except asyncio.CancelledError:  # pragma: no cover - defensive
                raise

    def _extend_cooldown(self, seconds: int) -> None:
        # Later 429s can only extend the existing wait.
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + seconds)

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> int:
        raw = response.headers.get("Retry-After", "").strip()
        if raw.isdigit():
            return max(0, int(raw))
        return DEFAULT_RETRY_AFTER_SECONDS

    # ---------------------------------------------------------------- helpers
    def _list_url(self, section: str, page: int) -> str:
        path = self.sections[section]
        separator = "&" if "?" in path else "?"
        return f"{path}{separator}page={page}"

    def _absolute(self, url: str) -> str:
        if url.startswith(("http://", "https://")):
            return url
        return f"{self.base_url}/{url.lstrip('/')}"

    def _mark_unfinished(self, job: dict, reason: str, attempts: int) -> None:
        page_type = job["kind"]
        self.state.unfinished.append(
            UnfinishedPage(
                section=job["section"],
                page_type=page_type,
                page=str(job["page"]),
                url=self._absolute(job["url"]),
                reason=reason,
                attempts=attempts,
            )
        )
