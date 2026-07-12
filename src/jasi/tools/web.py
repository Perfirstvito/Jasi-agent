from __future__ import annotations

import asyncio
import ipaddress
import socket
from typing import Any
from urllib.parse import urljoin, urlparse

import html2text
import httpx
from bs4 import BeautifulSoup

from jasi.runtime.errors import ToolRejected
from jasi.tools.registry import ToolExecutionContext, ToolSpec

_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_MAX_TEXT_CHARS = 30_000
_MAX_REDIRECTS = 5
_DEFAULT_FETCH_TIMEOUT = 30
_MAX_FETCH_TIMEOUT = 60
_DEFAULT_SEARCH_RESULTS = 5
_FAKE_IP_DNS_NETWORK = ipaddress.ip_network("198.18.0.0/15")


def create_web_tools(*, allow_fake_ip_dns: bool = False) -> list[ToolSpec]:
    async def web_fetch(
        arguments: dict[str, Any], _context: ToolExecutionContext
    ) -> dict[str, Any]:
        return await _fetch_url(
            arguments["url"],
            arguments.get("format", "markdown"),
            int(arguments.get("timeout", _DEFAULT_FETCH_TIMEOUT)),
            allow_fake_ip_dns=allow_fake_ip_dns,
        )

    async def web_search(
        arguments: dict[str, Any], _context: ToolExecutionContext
    ) -> dict[str, Any]:
        query = arguments["query"].strip()
        if not query:
            raise ToolRejected("web search query cannot be blank")
        max_results = int(arguments.get("max_results", _DEFAULT_SEARCH_RESULTS))
        region = arguments.get("region", "wt-wt")
        timelimit = arguments.get("timelimit")
        return await asyncio.to_thread(
            _search_web,
            query,
            max_results,
            region,
            timelimit,
        )

    return [
        ToolSpec(
            name="web_search",
            description=(
                "Search the public web for current information. Returns titles, snippets, "
                "and URLs; use web_fetch to inspect a result."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1, "maxLength": 500},
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                        "default": _DEFAULT_SEARCH_RESULTS,
                    },
                    "region": {"type": "string", "minLength": 2, "default": "wt-wt"},
                    "timelimit": {
                        "type": "string",
                        "enum": ["d", "w", "m", "y"],
                    },
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            risk="read-only",
            handler=web_search,
            search_terms=("internet search", "current information", "网页搜索", "联网搜索"),
        ),
        ToolSpec(
            name="web_fetch",
            description=(
                "Fetch one public HTTP or HTTPS URL after SSRF checks. Supports markdown, "
                "plain text, and raw HTML output."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "minLength": 1, "maxLength": 2048},
                    "format": {
                        "type": "string",
                        "enum": ["markdown", "text", "html"],
                        "default": "markdown",
                    },
                    "timeout": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": _MAX_FETCH_TIMEOUT,
                        "default": _DEFAULT_FETCH_TIMEOUT,
                    },
                },
                "required": ["url"],
                "additionalProperties": False,
            },
            risk="read-only",
            handler=web_fetch,
            search_terms=("fetch url", "read webpage", "抓取网页", "读取链接"),
        ),
    ]


async def _fetch_url(
    url: str,
    output_format: str,
    timeout_seconds: int,
    *,
    allow_fake_ip_dns: bool,
) -> dict[str, Any]:
    current_url = url.strip()
    timeout = httpx.Timeout(timeout_seconds)
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            headers={
                "User-Agent": "Jasi/0.1",
                "Accept": "text/html,text/plain,application/json,application/xml;q=0.9,*/*;q=0.1",
            },
        ) as client:
            for redirect_count in range(_MAX_REDIRECTS + 1):
                await _validate_public_url(
                    current_url,
                    allow_fake_ip_dns=allow_fake_ip_dns,
                )
                async with client.stream("GET", current_url) as response:
                    if response.is_redirect:
                        location = response.headers.get("location")
                        if not location:
                            raise ToolRejected("redirect response has no location")
                        if redirect_count >= _MAX_REDIRECTS:
                            raise ToolRejected("too many URL redirects")
                        current_url = urljoin(str(response.url), location)
                        continue
                    content_length = response.headers.get("content-length")
                    if content_length:
                        try:
                            declared_length = int(content_length)
                        except ValueError as exc:
                            raise ToolRejected(
                                "web response has an invalid content length"
                            ) from exc
                        if declared_length < 0:
                            raise ToolRejected("web response has an invalid content length")
                        if declared_length > _MAX_RESPONSE_BYTES:
                            raise ToolRejected("web response is too large")
                    chunks: list[bytes] = []
                    received = 0
                    async for chunk in response.aiter_bytes():
                        received += len(chunk)
                        if received > _MAX_RESPONSE_BYTES:
                            raise ToolRejected("web response is too large")
                        chunks.append(chunk)
                    body = b"".join(chunks)
                    return _render_response(response, body, output_format, url)
    except ToolRejected:
        raise
    except httpx.TimeoutException:
        return {"url": url, "error": "request_timeout"}
    except httpx.HTTPError as exc:
        return {"url": url, "error": "request_failed", "error_type": exc.__class__.__name__}
    raise ToolRejected("web request did not produce a response")


async def _validate_public_url(url: str, *, allow_fake_ip_dns: bool = False) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ToolRejected("URL must use HTTP or HTTPS")
    if parsed.username or parsed.password:
        raise ToolRejected("URL credentials are not allowed")
    host = (parsed.hostname or "").strip().casefold()
    if not host:
        raise ToolRejected("URL has no host")
    if host == "localhost" or host.endswith((".localhost", ".local")):
        raise ToolRejected("local URL targets are not allowed")
    try:
        ipaddress.ip_address(host)
        host_is_ip_literal = True
    except ValueError:
        host_is_ip_literal = False
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise ToolRejected("URL has an invalid port") from exc
    try:
        addresses = await asyncio.to_thread(
            socket.getaddrinfo,
            host,
            port,
            0,
            socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise ToolRejected("URL host could not be resolved") from exc
    if not addresses:
        raise ToolRejected("URL host could not be resolved")
    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        if ip.is_global:
            continue
        if allow_fake_ip_dns and not host_is_ip_literal and ip in _FAKE_IP_DNS_NETWORK:
            continue
        raise ToolRejected("private, local, and reserved URL targets are not allowed")


def _render_response(
    response: httpx.Response,
    body: bytes,
    output_format: str,
    requested_url: str,
) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "").casefold()
    if any(
        marker in content_type
        for marker in ("image/", "audio/", "video/", "application/octet-stream", "application/pdf")
    ):
        raise ToolRejected("binary web responses are not supported")
    encoding = response.encoding or "utf-8"
    decoded = body.decode(encoding, errors="replace")
    is_html = "html" in content_type or "<html" in decoded[:500].casefold()
    if output_format == "html" or not is_html:
        text = decoded
    elif output_format == "text":
        soup = BeautifulSoup(decoded, "html.parser")
        for element in soup(["script", "style", "noscript", "iframe"]):
            element.decompose()
        text = "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())
    else:
        converter = html2text.HTML2Text()
        converter.body_width = 0
        converter.ignore_links = False
        converter.ignore_images = False
        text = converter.handle(decoded).strip()
    truncated = len(text) > _MAX_TEXT_CHARS
    if truncated:
        text = text[:_MAX_TEXT_CHARS]
    return {
        "url": requested_url,
        "final_url": str(response.url),
        "status": response.status_code,
        "content_type": content_type,
        "format": output_format,
        "text": text,
        "truncated": truncated,
    }


def _search_web(
    query: str,
    max_results: int,
    region: str,
    timelimit: str | None,
) -> dict[str, Any]:
    try:
        from ddgs import DDGS

        raw_results = DDGS(timeout=20).text(
            query,
            region=region,
            safesearch="moderate",
            timelimit=timelimit,
            max_results=max_results,
        )
        results = [
            {
                "title": str(item.get("title") or ""),
                "url": str(item.get("href") or item.get("url") or ""),
                "snippet": str(item.get("body") or item.get("snippet") or ""),
            }
            for item in raw_results
        ]
        return {"query": query, "results": results, "count": len(results)}
    except Exception as exc:
        return {
            "query": query,
            "results": [],
            "count": 0,
            "error": "search_unavailable",
            "error_type": exc.__class__.__name__,
        }
