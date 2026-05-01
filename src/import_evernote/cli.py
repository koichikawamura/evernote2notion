"""
import-evernote: migrate Evernote HTML exports into a Notion database.

Usage:
    import-evernote --export-dir <PATH> --parent-page-id <PAGE_ID>

See README for full setup instructions.
"""

import argparse
import hashlib
import json
import logging
import email.header
import mimetypes
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

import httpx
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError

# ── Configuration ──────────────────────────────────────────────────────────────

MAX_BLOCKS_PER_REQUEST = 100  # Notion API limit per call
MAX_RICH_TEXT_LEN = 2000      # Notion API limit per rich-text item
MAX_TITLE_LEN = 2000          # Notion title property limit
REQUEST_DELAY = 0.45          # ~2.2 req/sec, safely under Notion's 3/sec limit

# Code block languages supported by Notion (anything else → "plain text")
_NOTION_LANGUAGES = frozenset([
    "abap", "arduino", "bash", "basic", "c", "clojure", "coffeescript",
    "c++", "c#", "css", "dart", "diff", "docker", "elixir", "elm", "erlang",
    "flow", "fortran", "f#", "gherkin", "glsl", "go", "graphql", "groovy",
    "haskell", "html", "java", "javascript", "json", "julia", "kotlin",
    "latex", "less", "lisp", "livescript", "lua", "makefile", "markdown",
    "markup", "matlab", "mermaid", "nix", "objective-c", "ocaml", "pascal",
    "perl", "php", "plain text", "powershell", "prolog", "protobuf", "python",
    "r", "reason", "ruby", "rust", "sass", "scala", "scheme", "scss",
    "shell", "sql", "swift", "toml", "typescript", "vb.net", "verilog",
    "vhdl", "visual basic", "webassembly", "xml", "yaml",
    "java/c/c++/c#",
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("import_evernote.log"),
    ],
)
logger = logging.getLogger(__name__)

# ── File Upload ────────────────────────────────────────────────────────────────

_FILE_UPLOAD_VERSION = "2026-03-11"  # Notion API version that supports file uploads

_IMAGE_MIMES = frozenset(["image/jpeg", "image/png", "image/gif", "image/webp",
                           "image/svg+xml", "image/avif", "image/bmp", "image/tiff"])
_VIDEO_MIMES = frozenset(["video/mp4", "video/quicktime", "video/x-msvideo",
                           "video/webm", "video/x-matroska"])
_AUDIO_MIMES = frozenset(["audio/mpeg", "audio/wav", "audio/ogg", "audio/mp4",
                           "audio/flac", "audio/aac"])


def _sniff_mime(path: Path) -> str:
    """Guess MIME type via file extension, then magic bytes for extensionless files."""
    mime, _ = mimetypes.guess_type(str(path))
    if mime:
        return mime
    try:
        with open(path, "rb") as f:
            h = f.read(12)
        if h[:2] == b"\xff\xd8":
            return "image/jpeg"
        if h[:8] == b"\x89PNG\r\n\x1a\n":
            return "image/png"
        if h[:6] in (b"GIF87a", b"GIF89a"):
            return "image/gif"
        if h[:4] == b"%PDF":
            return "application/pdf"
        if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
            return "image/webp"
    except OSError:
        pass
    return "application/octet-stream"


def _upload_local_file(
    token: str, filepath: Path, cache: dict[str, tuple[str, str]]
) -> Optional[tuple[str, str]]:
    """Upload a local file to Notion.

    Returns (file_upload_id, content_type) on success, None if the file is
    missing or the upload fails.  Results are cached by resolved path.
    """
    key = str(filepath.resolve())
    if key in cache:
        return cache[key]
    if not filepath.exists():
        return None

    content_type = _sniff_mime(filepath)

    # Notion requires a filename with an extension; derive one from the MIME type
    # for extensionless files (common in Evernote exports).
    _MIME_EXT = {
        "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
        "image/webp": ".webp", "image/svg+xml": ".svg", "image/bmp": ".bmp",
        "application/pdf": ".pdf",
    }
    filename = filepath.name
    if not filepath.suffix and content_type in _MIME_EXT:
        filename = filename + _MIME_EXT[content_type]
    if len(filename.encode()) > 900:
        filename = filepath.stem[:200] + filepath.suffix

    hdrs = {"Authorization": f"Bearer {token}", "Notion-Version": _FILE_UPLOAD_VERSION}

    for attempt in range(3):
        try:
            with httpx.Client(timeout=120) as hc:
                r = hc.post(
                    "https://api.notion.com/v1/file_uploads",
                    headers=hdrs,
                    json={"mode": "single_part", "filename": filename,
                          "content_type": content_type},
                )
                r.raise_for_status()
                upload_id: str = r.json()["id"]

                with open(filepath, "rb") as fh:
                    r = hc.post(
                        f"https://api.notion.com/v1/file_uploads/{upload_id}/send",
                        headers=hdrs,
                        files={"file": (filename, fh, content_type)},
                    )
                r.raise_for_status()
            break
        except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            if attempt == 2:
                raise
            wait = 2 ** attempt
            logger.warning(f"Upload attempt {attempt + 1} failed for {filepath.name}: {exc}; retrying in {wait}s")
            time.sleep(wait)

    result = (upload_id, content_type)
    cache[key] = result
    return result


def _upload_block(upload_id: str, content_type: str) -> dict:
    """Build a Notion block that references an uploaded file."""
    obj: dict = {"type": "file_upload", "file_upload": {"id": upload_id}}
    if content_type in _IMAGE_MIMES or content_type.startswith("image/"):
        return {"object": "block", "type": "image", "image": obj}
    if content_type == "application/pdf":
        return {"object": "block", "type": "pdf", "pdf": obj}
    if content_type in _VIDEO_MIMES or content_type.startswith("video/"):
        return {"object": "block", "type": "video", "video": obj}
    if content_type in _AUDIO_MIMES or content_type.startswith("audio/"):
        return {"object": "block", "type": "audio", "audio": obj}
    return {"object": "block", "type": "file", "file": obj}


# ── Date Utilities ─────────────────────────────────────────────────────────────

_DATE_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$")


def _parse_date(s: str) -> Optional[str]:
    """Convert '20131003T072951Z' → '2013-10-03T07:29:51.000+00:00'."""
    m = _DATE_RE.match(s or "")
    if not m:
        return None
    y, mo, d, h, mi, sec = m.groups()
    return f"{y}-{mo}-{d}T{h}:{mi}:{sec}.000+00:00"


# Matches a MIME encoded-word with ? delimiters stripped to plain chars:
#   =cp932Q=82=C5...=  →  =?cp932?Q?=82=C5...?=
_STRIPPED_MIME_RE = re.compile(
    r"=([A-Za-z0-9][\w-]*)([BQbq])((?:[^=\s]|=[0-9A-Fa-f]{2})+)=(?=\s|$)"
)


def _decode_mime(s: str) -> str:
    """Decode RFC 2047 MIME encoded-words in a string.

    Handles both the standard form  =?charset?Q?text?=
    and the ?-stripped filename form  =charsetQtext=  that Evernote produces
    when saving files to disk on some platforms.
    """
    if not s:
        return s
    # Standard encoded-words (present in meta title content)
    if "=?" in s:
        try:
            return str(email.header.make_header(email.header.decode_header(s)))
        except Exception:
            pass
    # Stripped form (present in filename stems)
    if _STRIPPED_MIME_RE.search(s):
        reconstructed = _STRIPPED_MIME_RE.sub(
            lambda m: f"=?{m.group(1)}?{m.group(2)}?{m.group(3)}?=", s
        )
        try:
            return str(email.header.make_header(email.header.decode_header(reconstructed)))
        except Exception:
            pass
    return s


# ── Rich-text Helpers ──────────────────────────────────────────────────────────

_SKIP_TAGS = frozenset([
    "style", "script", "svg", "icons", "meta", "note-attributes",
    "head", "input", "use", "path", "g", "defs", "circle", "rect",
    "line", "polygon", "symbol",
])


def _make_rt(content: str, state: dict) -> dict:
    """Build a single Notion rich-text item."""
    item: dict[str, Any] = {
        "type": "text",
        "text": {"content": content},
        "annotations": {
            "bold": bool(state.get("bold")),
            "italic": bool(state.get("italic")),
            "underline": bool(state.get("underline")),
            "strikethrough": bool(state.get("strikethrough")),
            "code": bool(state.get("code")),
            "color": "default",
        },
    }
    if state.get("url"):
        item["text"]["link"] = {"url": state["url"]}
    return item


def _cap_rt(items: list[dict]) -> list[dict]:
    """Split items exceeding MAX_RICH_TEXT_LEN and cap list at 100 items."""
    result: list[dict] = []
    for item in items:
        if item.get("type") != "text":
            result.append(item)
            continue
        text: str = item["text"]["content"]
        while len(text) > MAX_RICH_TEXT_LEN:
            part = {**item, "text": {**item["text"], "content": text[:MAX_RICH_TEXT_LEN]}}
            result.append(part)
            text = text[MAX_RICH_TEXT_LEN:]
        if text:
            result.append({**item, "text": {**item["text"], "content": text}})
    return result[:100]


def get_rich_text(element: Any, state: Optional[dict] = None) -> list[dict]:
    """Recursively extract Notion rich-text objects from an HTML element.

    Inline formatting (bold, italic, links, etc.) is preserved.
    Block-level elements are treated as transparent containers here;
    block structure is handled by element_to_blocks().
    """
    if state is None:
        state = {}

    if isinstance(element, Comment):
        return []

    if isinstance(element, NavigableString):
        text = str(element)
        return [_make_rt(text, state)] if text else []

    if not isinstance(element, Tag):
        return []

    tag = element.name.lower() if element.name else ""

    if not tag or tag in _SKIP_TAGS:
        return []

    new_state = dict(state)

    if tag in ("b", "strong"):
        new_state["bold"] = True
    elif tag in ("i", "em"):
        new_state["italic"] = True
    elif tag in ("u", "ins"):
        new_state["underline"] = True
    elif tag in ("s", "del", "strike"):
        new_state["strikethrough"] = True
    elif tag == "code":
        new_state["code"] = True
    elif tag == "a":
        href = re.sub(r'[\x00-\x20\x7f]', '', (element.get("href") or ""))
        if (href and len(href) <= 2000 and "url" not in new_state
                and href.startswith(("http://", "https://"))):
            new_state["url"] = href
    elif tag == "br":
        return [_make_rt("\n", state)]
    elif tag in ("ul", "ol", "table"):
        # Can't represent structure inline – fall back to plain text
        return [_make_rt(element.get_text(" ", strip=True), new_state)]

    result: list[dict] = []
    for child in element.children:
        result.extend(get_rich_text(child, new_state))
    return result


# ── Block Conversion ───────────────────────────────────────────────────────────

_BLOCK_CHILDREN_TAGS = frozenset([
    "div", "p", "ul", "ol", "table", "pre", "blockquote", "hr",
    "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "img",
])


def _para(rt: list[dict]) -> dict:
    return {"object": "block", "type": "paragraph",
            "paragraph": {"rich_text": _cap_rt(rt)}}


def _heading(level: int, rt: list[dict]) -> dict:
    t = f"heading_{min(level, 3)}"
    return {"object": "block", "type": t, t: {"rich_text": _cap_rt(rt)}}


def element_to_blocks(elem: Any) -> list[dict]:
    """Convert one HTML element to zero or more Notion block dicts."""
    if isinstance(elem, Comment):
        return []

    if isinstance(elem, NavigableString):
        text = str(elem).strip()
        return [_para([_make_rt(text, {})])] if text else []

    if not isinstance(elem, Tag):
        return []

    tag = elem.name.lower() if elem.name else ""

    if not tag or tag in _SKIP_TAGS or tag == "svg":
        return []

    # ── Headings ─────────────────────────────────────────────────────
    h_map = {"h2": 1, "h3": 2, "h4": 3, "h5": 3, "h6": 3}
    # h1 with class "noteTitle" is the note title, already consumed by parser
    if tag in h_map:
        rt = get_rich_text(elem)
        return [_heading(h_map[tag], rt)] if rt else []

    # ── Horizontal rule ───────────────────────────────────────────────
    if tag == "hr":
        return [{"object": "block", "type": "divider", "divider": {}}]

    # ── Code block ───────────────────────────────────────────────────
    if tag == "pre":
        code_text = elem.get_text()
        lang = "plain text"
        code_child = elem.find("code")
        if code_child:
            for cls in (code_child.get("class") or []):
                candidate = cls[9:] if cls.startswith("language-") else cls
                if candidate.lower() in _NOTION_LANGUAGES:
                    lang = candidate.lower()
                    break
        blocks: list[dict] = []
        for i in range(0, max(1, len(code_text)), MAX_RICH_TEXT_LEN):
            chunk = code_text[i : i + MAX_RICH_TEXT_LEN]
            blocks.append({"object": "block", "type": "code",
                           "code": {"rich_text": [_make_rt(chunk, {})], "language": lang}})
        return blocks

    # ── Blockquote ───────────────────────────────────────────────────
    if tag == "blockquote":
        rt = get_rich_text(elem)
        return [{"object": "block", "type": "quote",
                 "quote": {"rich_text": _cap_rt(rt)}}] if rt else []

    # ── Image ────────────────────────────────────────────────────────
    if tag == "img":
        upload_id = elem.get("data-notion-upload-id")
        if upload_id:
            ct = elem.get("data-notion-content-type", "image/jpeg")
            return [_upload_block(upload_id, ct)]

        src = (elem.get("src") or "").strip()
        if src.startswith(("http://", "https://")) and len(src) <= 2000:
            return [{"object": "block", "type": "image",
                     "image": {"type": "external", "external": {"url": src}}}]
        if src:
            alt = (elem.get("alt") or elem.get("data-filename") or src[:200]).strip()
            return [_para([_make_rt(f"[Image: {alt}]", {})])]
        return []

    # ── Unordered list ───────────────────────────────────────────────
    if tag == "ul":
        return [b for li in elem.find_all("li", recursive=False)
                for b in _li_blocks(li, "bulleted")]

    # ── Ordered list ─────────────────────────────────────────────────
    if tag == "ol":
        return [b for li in elem.find_all("li", recursive=False)
                for b in _li_blocks(li, "numbered")]

    # ── Table ────────────────────────────────────────────────────────
    if tag == "table":
        return _table_blocks(elem)

    # Skip bare table structural elements at top level
    if tag in ("thead", "tbody", "tfoot", "tr", "td", "th", "caption"):
        return []

    # ── Div / paragraph (the common Evernote content wrapper) ────────
    if tag in ("div", "p", "section", "article", "span", "main", "aside",
               "header", "footer", "nav"):
        # Non-image attachment (PDF, audio, docx…) referenced by data-resource-hash
        if elem.get("data-resource-hash"):
            if elem.get("data-notion-upload-id"):
                upload_id = elem["data-notion-upload-id"]
                ct = elem.get("data-notion-content-type", "application/octet-stream")
                return [_upload_block(upload_id, ct)]
            else:
                inner = elem.find(attrs={"data-type": True})
                dtype = inner.get("data-type", "file") if inner else "file"
                return [_para([_make_rt(f"[Attachment: {dtype}]", {})])]

        # Evernote todo checkbox inside a list item
        checkbox = elem.find("input", {"type": "checkbox"})
        if checkbox:
            checked = checkbox.has_attr("checked")
            for container in elem.find_all(class_="list-bullet-todo-container"):
                container.decompose()
            rt = get_rich_text(elem)
            return [{"object": "block", "type": "to_do",
                     "to_do": {"rich_text": _cap_rt(rt) or [_make_rt("", {})],
                               "checked": checked}}]

        # If any direct child is a structural block, recurse
        if any(
            isinstance(c, Tag) and c.name and c.name.lower() in _BLOCK_CHILDREN_TAGS
            for c in elem.children
        ):
            result: list[dict] = []
            for child in elem.children:
                result.extend(element_to_blocks(child))
            return result

        # Leaf element – extract rich text
        rt = get_rich_text(elem)
        return [_para(rt)] if rt else []

    # ── Fallback: treat as inline container ──────────────────────────
    rt = get_rich_text(elem)
    return [_para(rt)] if rt else []


_MAX_LIST_DEPTH = 2   # Notion API: list items at depth > 2 cannot have children
_MAX_LIST_CHILDREN = 100  # Notion API: max children per list item


def _li_blocks(li: Tag, list_type: str, depth: int = 1) -> list[dict]:
    """Convert a <li> element to one Notion block (bulleted/numbered/to_do).

    depth tracks nesting from the page level (1 = direct child of the page).
    Notion forbids children on list items beyond _MAX_LIST_DEPTH.
    """
    checkbox = li.find("input", {"type": "checkbox"})
    is_todo = checkbox is not None
    checked = is_todo and checkbox.has_attr("checked")

    rt_parts: list[dict] = []
    nested_blocks: list[dict] = []

    for child in li.children:
        if isinstance(child, Tag):
            ctag = child.name.lower()
            if ctag in ("ul", "ol"):
                nested_type = "bulleted" if ctag == "ul" else "numbered"
                for nested_li in child.find_all("li", recursive=False):
                    nested_blocks.extend(_li_blocks(nested_li, nested_type, depth + 1))
            elif ctag == "input":
                pass  # skip the checkbox input element
            elif "list-bullet-todo-container" in (child.get("class") or []):
                pass  # skip the Evernote todo checkbox container div
            else:
                rt_parts.extend(get_rich_text(child))
        elif isinstance(child, NavigableString):
            text = str(child)
            if text.strip():
                rt_parts.append(_make_rt(text, {}))

    rt = _cap_rt(rt_parts) or [_make_rt("", {})]

    # Notion caps children count and forbids children beyond max depth
    if nested_blocks and depth < _MAX_LIST_DEPTH:
        nested_blocks = nested_blocks[:_MAX_LIST_CHILDREN]
    else:
        nested_blocks = []

    if is_todo:
        block: dict = {"object": "block", "type": "to_do",
                       "to_do": {"rich_text": rt, "checked": checked}}
        if nested_blocks:
            block["to_do"]["children"] = nested_blocks
    else:
        btype = f"{list_type}_list_item"
        block = {"object": "block", "type": btype, btype: {"rich_text": rt}}
        if nested_blocks:
            block[btype]["children"] = nested_blocks

    return [block]


def _table_blocks(table: Tag) -> list[dict]:
    """Convert an HTML <table> to a Notion table block (with rows as children)."""
    rows = table.find_all("tr")
    if not rows:
        return []

    all_rows: list[list[list[dict]]] = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        row_cells = [
            (_cap_rt(get_rich_text(cell)) or [_make_rt("", {})])
            for cell in cells
        ]
        if row_cells:
            all_rows.append(row_cells)

    if not all_rows:
        return []

    table_width = max(len(r) for r in all_rows)
    if table_width == 0:
        return []

    # Notion caps table width at 100 columns; over-wide tables are layout artifacts
    if table_width > 100:
        text = table.get_text(" ", strip=True)
        return [_para([_make_rt(text, {})])] if text else []

    for row in all_rows:
        while len(row) < table_width:
            row.append([_make_rt("", {})])

    has_header = bool(rows[0].find("th"))

    table_row_blocks = [
        {"object": "block", "type": "table_row", "table_row": {"cells": row}}
        for row in all_rows
    ]

    return [{
        "object": "block",
        "type": "table",
        "table": {
            "table_width": table_width,
            "has_column_header": has_header,
            "has_row_header": False,
            "children": table_row_blocks,
        },
    }]


# ── Evernote HTML Parser ───────────────────────────────────────────────────────

def parse_evernote_html(
    filepath: Path,
    upload_fn: Optional[Callable[[Path], Optional[tuple[str, str]]]] = None,
) -> dict:
    """Parse an Evernote HTML export file; return a note dict with Notion blocks.

    If upload_fn is provided it is called for each local asset path and should
    return (file_upload_id, content_type) or None.  Successful uploads are
    stored in data-notion-upload-id / data-notion-content-type attributes on
    the img element so that element_to_blocks can use them.
    """
    with open(filepath, encoding="utf-8", errors="replace") as f:
        html = f.read()

    soup = BeautifulSoup(html, "lxml")

    if upload_fn is not None:
        base_dir = filepath.parent
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src or src.startswith(("http://", "https://")):
                continue
            local_path = base_dir / src
            result = upload_fn(local_path)
            if result:
                upload_id, content_type = result
                img["data-notion-upload-id"] = upload_id
                img["data-notion-content-type"] = content_type

        # Build MD5→Path map for non-image attachments in the companion files/ dir
        files_dir = base_dir / (filepath.stem + " files")
        hash_to_path: dict[str, Path] = {}
        if files_dir.is_dir():
            for asset in files_dir.iterdir():
                if asset.is_file():
                    try:
                        md5 = hashlib.md5(asset.read_bytes()).hexdigest()
                        hash_to_path[md5] = asset
                    except OSError:
                        pass

        # Upload non-image attachments referenced by data-resource-hash on <div>s
        for div in soup.find_all("div", attrs={"data-resource-hash": True}):
            inner = div.find(attrs={"data-type": True})
            if not inner:
                continue  # images are already handled via <img src>
            resource_hash = div.get("data-resource-hash", "")
            asset_path = hash_to_path.get(resource_hash)
            if asset_path:
                result = upload_fn(asset_path)
                if result:
                    upload_id, actual_ct = result
                    div["data-notion-upload-id"] = upload_id
                    div["data-notion-content-type"] = actual_ct

    note: dict[str, Any] = {
        "title": _decode_mime(filepath.stem),  # fallback if meta title is missing
        "created": None,
        "updated": None,
        "tags": [],
        "source": None,
        "source_url": None,
        "year": filepath.parent.name,
        "filepath": str(filepath),
        "blocks": [],
    }

    for meta in soup.find_all("meta", itemprop=True):
        prop = meta.get("itemprop", "")
        val = (meta.get("content") or "").strip()
        if prop == "title" and val:
            note["title"] = _decode_mime(val)
        elif prop == "created":
            note["created"] = _parse_date(val)
        elif prop == "updated":
            note["updated"] = _parse_date(val)
        elif prop == "tag" and val:
            note["tags"].append(val)
        elif prop == "source" and val:
            note["source"] = val
        elif prop == "source-url" and val:
            note["source_url"] = val

    # Content lives between the noteTitle <h1> and the <style id="chs"> closer
    title_h1 = soup.find("h1", class_="noteTitle")
    if title_h1 is None:
        return note

    content_elems: list[Any] = []
    for sibling in title_h1.next_siblings:
        if (
            isinstance(sibling, Tag)
            and sibling.name == "style"
            and sibling.get("id") == "chs"
        ):
            break
        content_elems.append(sibling)

    blocks: list[dict] = []
    for elem in content_elems:
        blocks.extend(element_to_blocks(elem))

    note["blocks"] = blocks
    return note


# ── Notion API Wrapper ─────────────────────────────────────────────────────────

def _api(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a Notion API function with rate-limiting and retries."""
    for attempt in range(6):
        try:
            time.sleep(REQUEST_DELAY)
            return fn(*args, **kwargs)
        except HTTPResponseError as exc:
            code = getattr(exc, "code", "") or ""
            status = getattr(exc, "status", 0) or 0
            if code == "rate_limited" or status == 429:
                wait = 2 ** attempt
                logger.warning(f"Rate limited; retrying in {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            elif attempt < 3 and status in (500, 502, 503):
                time.sleep(2 ** attempt)
            elif attempt < 1 and status == 403:
                # One retry for transient 403 (permission propagation lag after page creation)
                logger.warning(f"403 Forbidden (transient?); retrying in 5s")
                time.sleep(5)
            else:
                raise
    raise RuntimeError("Exceeded max retries for Notion API call")


_REQUIRED_PROPERTIES: dict = {
    "Year":       {"select": {}},
    "Created":    {"date": {}},
    "Updated":    {"date": {}},
    "Tags":       {"multi_select": {}},
    "Source URL": {"url": {}},
    "Source":     {"select": {}},
}


def _db_request(client: Client, method: str, path: str, body: dict) -> dict:
    """Make a raw Notion API request, bypassing notion-client's pick() filtering.

    notion-client 3.0.0 omits 'properties' from the allowed kwargs in both
    databases.create and databases.update, so we call client.request() directly.
    """
    return _api(client.request, path=path, method=method, body=body)


def create_database(client: Client, parent_page_id: str) -> str:
    """Create the Evernote Notes database under parent_page_id; return db ID."""
    db = _db_request(client, "POST", "databases", {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": "Evernote Notes"}}],
        "properties": {"Name": {"title": {}}, **_REQUIRED_PROPERTIES},
    })
    db_id: str = db["id"]
    logger.info(f"Created Notion database: {db_id}")
    return db_id


def ensure_database_properties(client: Client, db_id: str) -> None:
    """Add any required properties missing from the database."""
    db = _api(client.databases.retrieve, db_id)
    existing = set((db.get("properties") or {}).keys())
    missing = {k: v for k, v in _REQUIRED_PROPERTIES.items() if k not in existing}
    if not missing:
        logger.info(f"Database {db_id[:8]}… — all properties present")
        return
    logger.info(f"Adding missing properties: {list(missing.keys())}")
    _db_request(client, "PATCH", f"databases/{db_id}", {"properties": missing})


def _can_write_to_database(client: Client, db_id: str) -> bool:
    """Return True if the integration can create pages in the database."""
    try:
        _db_request(client, "PATCH", f"databases/{db_id}", {})
        return True
    except HTTPResponseError as exc:
        status = getattr(exc, "status", 0) or 0
        if status in (403, 404):
            return False
        raise


def _find_existing_database(client: Client, parent_page_id: str) -> Optional[str]:
    """Search the parent page's children for a database named 'Evernote Notes'.

    Returns the database ID (without hyphens) if found, else None.
    """
    try:
        cursor = None
        while True:
            kwargs: dict = {"block_id": parent_page_id}
            if cursor:
                kwargs["start_cursor"] = cursor
            resp = _api(client.blocks.children.list, **kwargs)
            for block in resp.get("results", []):
                if (block.get("type") == "child_database"
                        and block.get("child_database", {}).get("title") == "Evernote Notes"):
                    return block["id"].replace("-", "")
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
    except Exception as exc:
        logger.warning(f"Could not search parent page for existing database: {exc}")
    return None


def _page_properties(note: dict) -> dict:
    """Build the Notion page properties dict for a note."""
    props: dict = {
        "Name": {"title": [{"type": "text",
                             "text": {"content": note["title"][:MAX_TITLE_LEN]}}]},
    }
    if note.get("year"):
        props["Year"] = {"select": {"name": note["year"]}}
    if note.get("created"):
        props["Created"] = {"date": {"start": note["created"]}}
    if note.get("updated"):
        props["Updated"] = {"date": {"start": note["updated"]}}
    if note.get("tags"):
        props["Tags"] = {"multi_select": [{"name": t[:100]} for t in note["tags"]]}
    if note.get("source_url"):
        props["Source URL"] = {"url": note["source_url"][:2000]}
    if note.get("source"):
        props["Source"] = {"select": {"name": note["source"][:100]}}
    return props


_MAX_TABLE_ROWS_BATCH = 10  # rows per append call for large tables (limits payload size)


def _append_blocks(client: Client, block_id: str, blocks: list[dict]) -> None:
    """Append blocks in batches.

    On 413: halves batch size until the oversized chunk fits.
    On 403: binary-searches for the offending block; replaces it with a
            placeholder paragraph and continues (WAF may block specific content).
    """
    if not blocks:
        return
    batch_size = MAX_BLOCKS_PER_REQUEST
    i = 0
    while i < len(blocks):
        chunk = blocks[i : i + batch_size]
        try:
            _api(client.blocks.children.append, block_id=block_id, children=chunk)
            i += batch_size
        except HTTPResponseError as exc:
            status = getattr(exc, "status", 0) or 0
            if status == 413 and batch_size > 1:
                batch_size = max(1, batch_size // 2)
                logger.warning(f"413 on block append; reducing batch to {batch_size}")
            elif status in (400, 403) and batch_size > 1:
                # Narrow down to isolate the problematic block
                batch_size = max(1, batch_size // 2)
                logger.warning(f"{status} on block append; narrowing to batch_size={batch_size}")
            elif status in (400, 403) and batch_size == 1:
                # Single block is persistently rejected — replace with placeholder
                btype = chunk[0].get("type", "unknown")
                logger.warning(f"Skipping block[{i}] ({btype}): content rejected ({status}); replacing with placeholder")
                placeholder = _para([_make_rt(f"[Block omitted: {btype} rejected by Notion API ({status})]", {})])
                try:
                    _api(client.blocks.children.append, block_id=block_id, children=[placeholder])
                except HTTPResponseError:
                    pass
                i += 1
                batch_size = MAX_BLOCKS_PER_REQUEST
            else:
                raise


def import_note(client: Client, db_id: str, note: dict) -> str:
    """Create a Notion database page for note; stream blocks in batches.

    Always creates the page empty first, then appends blocks so that large
    notes never hit 413 on pages.create.  Tables with many rows use a
    stub-then-append pattern to obtain the table block ID before streaming rows.
    """
    page = _api(
        client.pages.create,
        parent={"database_id": db_id},
        properties=_page_properties(note),
    )
    page_id: str = page["id"]

    pending: list[dict] = []

    def _flush() -> None:
        if not pending:
            return
        _append_blocks(client, page_id, list(pending))
        pending.clear()

    for block in note["blocks"]:
        if block["type"] == "table":
            rows = block.get("table", {}).get("children", [])
            if len(rows) <= _MAX_TABLE_ROWS_BATCH:
                pending.append(block)
            else:
                _flush()
                stub = {**block, "table": {**block["table"], "children": rows[:1]}}
                resp = _api(client.blocks.children.append, block_id=page_id, children=[stub])
                table_id: str = resp["results"][0]["id"]
                _append_blocks(client, table_id, rows[1:])
        else:
            pending.append(block)
            if len(pending) >= MAX_BLOCKS_PER_REQUEST:
                _flush()

    _flush()
    return page_id


# ── Progress Tracking ──────────────────────────────────────────────────────────

def _load_progress(progress_file: Path) -> dict:
    if progress_file.exists():
        with open(progress_file) as f:
            return json.load(f)
    return {"completed": [], "failed": {}}


def _save_progress(p: dict, progress_file: Path) -> None:
    with open(progress_file, "w") as f:
        json.dump(p, f, indent=2, ensure_ascii=False)


# ── Deduplication ─────────────────────────────────────────────────────────────

def _deduplicate(client: Client, db_id: str) -> None:
    """Archive older duplicate pages (same title) in the database.

    For each title that appears more than once, keeps the most recently created
    page and archives all older copies.
    """
    from collections import defaultdict

    logger.info(f"Fetching all pages from database {db_id[:8]}…")
    pages: list[dict] = []
    cursor = None
    while True:
        body: dict = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = _api(client.request, path=f"databases/{db_id}/query", method="POST", body=body)
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
        logger.info(f"  fetched {len(pages)} pages so far…")

    logger.info(f"Total pages fetched: {len(pages)}")

    # Group by title
    by_title: dict[str, list[dict]] = defaultdict(list)
    for page in pages:
        title_parts = (
            page.get("properties", {})
            .get("Name", {})
            .get("title", [])
        )
        title = title_parts[0]["text"]["content"] if title_parts else ""
        by_title[title].append(page)

    duplicates = {t: ps for t, ps in by_title.items() if len(ps) > 1}
    logger.info(f"Titles with duplicates: {len(duplicates)}")

    archived = 0
    for title, group in duplicates.items():
        # Sort newest first by created_time; keep [0], archive the rest
        group.sort(key=lambda p: p["created_time"], reverse=True)
        for old_page in group[1:]:
            page_id = old_page["id"]
            try:
                _api(client.pages.update, page_id=page_id, archived=True)
                archived += 1
                logger.info(f"Archived duplicate: '{title[:60]}' ({page_id})")
            except Exception as exc:
                logger.warning(f"Could not archive {page_id}: {exc}")

    logger.info(f"Done. Archived {archived} duplicate pages.")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--parent-page-id",
        help="Notion page ID that will contain the imported database",
    )
    parser.add_argument(
        "--export-dir",
        default="evernote export",
        help="Path to the Evernote HTML export directory (default: 'evernote export')",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("NOTION_TOKEN"),
        help="Notion API token (default: $NOTION_TOKEN)",
    )
    parser.add_argument("--year", type=int, help="Import only notes from this year")
    parser.add_argument("--limit", type=int, help="Stop after N notes (for testing)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse all notes and report block counts; don't upload",
    )
    parser.add_argument(
        "--deduplicate",
        action="store_true",
        help="Archive older duplicate pages (same title) in the database, keeping newest",
    )
    args = parser.parse_args()

    export_dir = Path(args.export_dir)
    progress_file = Path("import_progress.json")

    if not args.dry_run and not args.deduplicate:
        if not args.token:
            parser.error("--token or $NOTION_TOKEN is required")
        if not args.parent_page_id:
            parser.error("--parent-page-id is required")
        if not export_dir.is_dir():
            parser.error(f"Export directory not found: {export_dir}")

    # ── Gather files ──────────────────────────────────────────────────
    all_files: list[Path] = sorted(export_dir.rglob("*.html"))
    if args.year:
        all_files = [f for f in all_files if f.parent.name == str(args.year)]
    logger.info(f"Found {len(all_files)} HTML files")

    # ── Dry run ───────────────────────────────────────────────────────
    if args.dry_run:
        target = all_files[: args.limit] if args.limit else all_files
        errors = 0
        total_blocks = 0
        for i, path in enumerate(target, 1):
            try:
                note = parse_evernote_html(path)
                nb = len(note["blocks"])
                total_blocks += nb
                logger.info(f"[{i}/{len(target)}] {note['title']} — {nb} blocks")
            except Exception as exc:
                logger.error(f"Parse error {path}: {exc}")
                errors += 1
        logger.info(
            f"Dry run complete — {len(target)} notes, {total_blocks} blocks, "
            f"{errors} errors"
        )
        return

    # ── Live import / deduplicate ─────────────────────────────────────
    # Pin to 2022-06-28: the 2025-09-03 default removes "properties" from the
    # database response and uses "data_sources" instead, breaking our schema setup.
    client = Client(auth=args.token, notion_version="2022-06-28")

    if args.deduplicate:
        progress = _load_progress(progress_file)
        db_id = progress.get("database_id")
        if not db_id:
            parser.error("No database_id in import_progress.json; run import first")
        _deduplicate(client, db_id)
        return

    progress = _load_progress(progress_file)

    # Build upload_fn: wraps _upload_local_file with caching, logging, and a
    # small delay so file-upload calls don't overwhelm the Notion API.
    _upload_cache: dict[str, tuple[str, str]] = {}

    def _make_upload_fn() -> Callable[[Path], Optional[tuple[str, str]]]:
        def upload_fn(local_path: Path) -> Optional[tuple[str, str]]:
            try:
                time.sleep(0.1)
                return _upload_local_file(args.token, local_path, _upload_cache)
            except Exception as exc:
                logger.warning(f"Asset upload failed ({local_path.name}): {exc}")
                return None
        return upload_fn

    upload_fn = _make_upload_fn()

    def _setup_database() -> str:
        """Return the database ID to import into, creating one if necessary."""
        if "database_id" in progress:
            db_id = progress["database_id"]
            logger.info(f"Resuming import into database {db_id}")
            ensure_database_properties(client, db_id)
            if _can_write_to_database(client, db_id):
                return db_id
            logger.warning(f"Integration cannot write to database {db_id[:8]}…; searching for another")

        # Search the parent page for an existing "Evernote Notes" database
        found = _find_existing_database(client, args.parent_page_id)
        if found:
            logger.info(f"Found existing 'Evernote Notes' database: {found}")
            ensure_database_properties(client, found)
            return found

        # Nothing found – create a fresh database
        return create_database(client, args.parent_page_id)

    db_id = _setup_database()
    progress["database_id"] = db_id
    _save_progress(progress, progress_file)

    completed: set[str] = set(progress.get("completed", []))
    attempted = 0
    limit = args.limit or len(all_files)

    for filepath in all_files:
        if attempted >= limit:
            break

        key = str(filepath)
        if key in completed:
            continue

        attempted += 1

        try:
            note = parse_evernote_html(filepath, upload_fn=upload_fn)
            page_id = import_note(client, db_id, note)
            completed.add(key)
            progress["completed"] = list(completed)
            _save_progress(progress, progress_file)
            logger.info(
                f"[{attempted}/{limit}] ✓ {note['title']}"
                f" ({len(note['blocks'])} blocks) → {page_id}"
            )
        except Exception as exc:
            logger.error(f"[{attempted}/{limit}] ✗ {filepath.name}: {exc}")
            progress["completed"] = list(completed)
            _save_progress(progress, progress_file)
            sys.exit(1)

    logger.info(
        f"Done. Attempted: {attempted}. "
        f"Succeeded: {len(completed)}. "
        f"All notes imported successfully."
    )


if __name__ == "__main__":
    main()
