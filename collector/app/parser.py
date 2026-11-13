"""HTML parsing for the procurement site.

All parsers raise :class:`ParseError` when the expected structure is missing,
so the crawler can count the attempt and eventually list the page as unfinished.
"""

from dataclasses import dataclass

from bs4 import BeautifulSoup


class ParseError(Exception):
    """Raised when a page cannot be parsed into the expected structure."""


@dataclass
class ListItem:
    code: str
    title: str
    url: str


@dataclass
class ListParseResult:
    total_pages: int
    items: list[ListItem]


def _soup(html: str) -> BeautifulSoup:
    if not html or not html.strip():
        raise ParseError("empty response body")
    return BeautifulSoup(html, "html.parser")


def parse_list_page(html: str) -> ListParseResult:
    soup = _soup(html)
    container = soup.find(id="announcement-list")
    if container is None:
        raise ParseError("announcement list container (#announcement-list) not found")

    rows = container.select("tr.announcement, li.announcement")
    items: list[ListItem] = []
    for row in rows:
        link = row.find("a", href=True)
        code_tag = row.find(class_="code")
        if link is None or code_tag is None:
            raise ParseError("malformed announcement row: missing code or link")
        code = code_tag.get_text(strip=True)
        if not code:
            raise ParseError("malformed announcement row: empty announcement code")
        items.append(
            ListItem(code=code, title=link.get_text(strip=True), url=link["href"].strip())
        )

    if not items:
        raise ParseError("announcement list contains no rows")

    total_pages = 1
    pager = soup.find("nav", class_="pager")
    if pager is not None:
        raw_total = pager.get("data-total-pages")
        if raw_total and str(raw_total).isdigit():
            total_pages = int(raw_total)
        else:
            page_numbers = [
                int(tag.get_text(strip=True))
                for tag in pager.select("a.page-link")
                if tag.get_text(strip=True).isdigit()
            ]
            if page_numbers:
                total_pages = max(page_numbers)

    return ListParseResult(total_pages=max(1, total_pages), items=items)


def parse_detail_page(html: str) -> dict:
    soup = _soup(html)
    title_tag = soup.find(id="detail-title")
    if title_tag is None or not title_tag.get_text(strip=True):
        raise ParseError("detail page missing title (#detail-title)")

    field_map = {}
    for row in soup.select("table.detail-table tr"):
        th = row.find("th")
        td = row.find("td")
        if th is not None and td is not None:
            field_map[th.get_text(strip=True)] = td.get_text(strip=True)

    code = field_map.get("公告编号")
    unit = field_map.get("采购单位")
    deadline = field_map.get("截止日期")
    missing = [name for name, value in (("公告编号", code), ("采购单位", unit), ("截止日期", deadline)) if not value]
    if missing:
        raise ParseError(f"detail page missing required field(s): {', '.join(missing)}")

    return {
        "code": code,
        "title": title_tag.get_text(strip=True),
        "unit": unit,
        "deadline": deadline,
    }
