#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from rapidfuzz import fuzz


# ============================================================
# CONFIG
# ============================================================

OUTPUT = Path("listtivi.m3u")
REPORT = Path("stream_report.json")

DALVIK_UA = (
    "Dalvik/2.1.0 "
    "(Linux; U; Android 13; SM-S918B Build/TP1A.220624.014)"
)

REQUEST_TIMEOUT = (10, 20)
STREAM_TIMEOUT = (5, 12)

CHANNEL_API = "https://iptv-org.github.io/api/channels.json"
LOGO_API = "https://iptv-org.github.io/api/logos.json"

SOURCES = [
    {
        "name": "vmttv",
        "url": "https://raw.githubusercontent.com/"
               "vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv",
        "priority": 10,
    },
    {
        "name": "vietanhtv",
        "url": "https://tv.vietanhtv.top/sex/",
        "priority": 20,
    },
    {
        "name": "dltivi",
        "url": "https://raw.githubusercontent.com/"
               "DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl",
        "priority": 30,
    },
    {
        "name": "iptv-org",
        "url": "https://raw.githubusercontent.com/"
               "iptv-org/iptv/refs/heads/master/streams/vn.m3u",
        "priority": 40,
    },
    {
        "name": "easport",
        "url": "https://livesport.s.gy/easport",
        "priority": 50,
    },
]


# ============================================================
# EXCLUSION
# ============================================================

EXCLUDED_GROUPS = {
    "vmttv": {
        "live events",
        "radio",
        "uk radio",
        "israel",
        "hàn quốc",
        "trung quốc",
        "thái lan",
        "cola tv",
        "pháo hoa tv",
    },
    "vietanhtv": {
        "update",
        "dự phòng",
        "fpt",
        "sự kiện 360",
        "rạp phim",
        "radio",
        "socolive",
    },
    "easport": {
        "info",
    },
}

EXCLUDED_CHANNEL_IDS = {
    "iptv-org": {
        "vsbet",
    }
}


# ============================================================
# GROUP NORMALIZATION
# ============================================================

GROUP_ALIASES = {
    "vtv": "VTV",
    "vtv hd": "VTV",
    "vtv1": "VTV",
    "vtv2": "VTV",
    "vtv3": "VTV",
    "vtv4": "VTV",
    "vtv5": "VTV",
    "vtv6": "VTV",
    "vtv can tho": "VTV Địa phương",
    "vtv da nang": "VTV Địa phương",

    "fpt play": "FPT Play",
    "fpt": "FPT Play",

    "tv360": "TV360",
    "mytv": "MyTV",
    "vtvgo": "VTVGo",
    "vieon": "VieON",

    "thể thao": "Thể thao",
    "the thao": "Thể thao",
    "sport": "Thể thao",
    "sports": "Thể thao",

    "tin tức": "Tin tức",
    "tin tuc": "Tin tức",
    "news": "Tin tức",

    "giải trí": "Giải trí",
    "giai tri": "Giải trí",
    "entertainment": "Giải trí",

    "phim": "Phim",
    "movies": "Phim",

    "thiếu nhi": "Thiếu nhi",
    "thieu nhi": "Thiếu nhi",
    "kids": "Thiếu nhi",

    "âm nhạc": "Âm nhạc",
    "am nhac": "Âm nhạc",
    "music": "Âm nhạc",

    "quốc tế": "Quốc tế",
    "quoc te": "Quốc tế",
    "international": "Quốc tế",
}


GROUP_ORDER = [
    "VTV",
    "VTV Địa phương",
    "FPT Play",
    "TV360",
    "MyTV",
    "VTVGo",
    "VieON",
    "Tin tức",
    "Thể thao",
    "Giải trí",
    "Phim",
    "Thiếu nhi",
    "Âm nhạc",
    "Quốc tế",
]


# ============================================================
# HTTP
# ============================================================

SESSION = requests.Session()

SESSION.headers.update({
    "User-Agent": DALVIK_UA,
    "Accept": "*/*",
    "Connection": "keep-alive",
})


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class StreamHealth:
    url: str
    status: int = 0
    content_type: str = ""
    latency: float = 9999.0

    is_hls: bool = False
    playlist_valid: bool = False
    segment_alive: bool = False
    alive: bool = False

    score: float = 0.0
    error: str = ""


@dataclass
class Channel:
    source: str
    source_priority: int

    name: str
    url: str

    attrs: dict[str, str] = field(default_factory=dict)
    extra_lines: list[str] = field(default_factory=list)

    group: str = ""
    tvg_id: str = ""
    tvg_name: str = ""
    logo: str = ""

    health: Optional[StreamHealth] = None

    canonical_id: str = ""
    canonical_name: str = ""


# ============================================================
# NORMALIZATION
# ============================================================

def strip_accents(value: str) -> str:
    value = unicodedata.normalize("NFD", value)
    return "".join(
        c for c in value
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(value: str) -> str:
    if not value:
        return ""

    value = strip_accents(value.lower())

    value = re.sub(
        r"\b(uhd|4k|8k|fullhd|fhd|hd|sd|hevc|h265|h264)\b",
        " ",
        value,
    )

    value = value.replace("&", " and ")

    value = re.sub(r"[^a-z0-9]+", " ", value)

    return " ".join(value.split())


def normalize_group(value: str) -> str:
    original = value.strip()

    if not original:
        return "Khác"

    key = normalize_text(original)

    if key in GROUP_ALIASES:
        return GROUP_ALIASES[key]

    best = None
    best_score = 0

    for alias, canonical in GROUP_ALIASES.items():
        score = fuzz.token_set_ratio(
            key,
            normalize_text(alias),
        )

        if score > best_score:
            best_score = score
            best = canonical

    if best and best_score >= 90:
        return best

    return original


def group_similarity(a: str, b: str) -> float:
    a = normalize_text(a)
    b = normalize_text(b)

    if not a or not b:
        return 0

    return fuzz.token_set_ratio(a, b) / 100.0


def channel_similarity(a: str, b: str) -> float:
    a = normalize_text(a)
    b = normalize_text(b)

    if not a or not b:
        return 0

    return fuzz.token_set_ratio(a, b) / 100.0


# ============================================================
# EXTINF PARSER
# ============================================================

ATTR_RE = re.compile(
    r'([\w-]+)="([^"]*)"'
)


def parse_extinf(line: str) -> tuple[dict[str, str], str]:
    attrs = {}

    for match in ATTR_RE.finditer(line):
        attrs[match.group(1)] = match.group(2)

    if "," in line:
        name = line.split(",", 1)[1].strip()
    else:
        name = ""

    return attrs, name


# ============================================================
# M3U PARSER
# ============================================================

def parse_m3u(
    text: str,
    source_name: str,
    source_priority: int,
) -> tuple[list[Channel], list[str]]:

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    channels: list[Channel] = []
    epgs: list[str] = []

    current_attrs = {}
    current_name = ""
    extra_lines: list[str] = []

    for raw in lines:
        line = raw.strip()

        if not line:
            continue

        # ----------------------------------------------------
        # Global EPG
        # ----------------------------------------------------

        if line.startswith("#EXTM3U"):
            for attr in (
                "url-tvg",
                "x-tvg-url",
                "tvg-url",
            ):
                m = re.search(
                    rf'{attr}="([^"]+)"',
                    line,
                    re.I,
                )

                if m:
                    for url in re.split(
                        r"[,;]\s*",
                        m.group(1),
                    ):
                        if url:
                            epgs.append(url.strip())

            continue

        # ----------------------------------------------------
        # EXTINF
        # ----------------------------------------------------

        if line.startswith("#EXTINF"):
            current_attrs, current_name = parse_extinf(line)
            extra_lines = []
            continue

        # ----------------------------------------------------
        # Preserve ALL metadata
        # ----------------------------------------------------

        if line.startswith("#"):
            if current_attrs:
                extra_lines.append(raw.strip())
            continue

        # ----------------------------------------------------
        # Stream URL
        # ----------------------------------------------------

        if current_attrs or current_name:

            attrs = dict(current_attrs)

            channel = Channel(
                source=source_name,
                source_priority=source_priority,

                name=current_name,
                url=line,

                attrs=attrs,
                extra_lines=list(extra_lines),

                group=attrs.get("group-title", ""),
                tvg_id=attrs.get("tvg-id", ""),
                tvg_name=attrs.get("tvg-name", current_name),
                logo=attrs.get("tvg-logo", ""),
            )

            channels.append(channel)

        current_attrs = {}
        current_name = ""
        extra_lines = []

    return channels, sorted(set(epgs))


# ============================================================
# SOURCE DOWNLOAD
# ============================================================

def fetch_source(url: str) -> str:
    response = SESSION.get(
        url,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )

    response.raise_for_status()

    return response.text


# ============================================================
# IPTV-ORG API
# ============================================================

def load_json(url: str):
    response = SESSION.get(
        url,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    return response.json()


def build_channel_index(data):
    result = {}

    for item in data:
        cid = str(item.get("id", "")).strip()

        if not cid:
            continue

        names = [
            item.get("name", ""),
            *item.get("alt_names", []),
        ]

        result[cid.lower()] = {
            "id": cid,
            "name": item.get("name", ""),
            "names": [x for x in names if x],
            "network": item.get("network"),
            "country": item.get("country"),
            "categories": item.get("categories", []),
        }

    return result


def build_logo_index(data):
    result = {}

    for item in data:
        channel_id = item.get("channel")

        if not channel_id:
            continue

        result.setdefault(channel_id.lower(), []).append(item)

    return result


# ============================================================
# CHANNEL MATCHING
# ============================================================

def match_iptv_channel(
    channel: Channel,
    channel_index: dict,
) -> Optional[dict]:

    # Exact tvg-id
    if channel.tvg_id:
        item = channel_index.get(channel.tvg_id.lower())

        if item:
            return item

    # Normalized id
    normalized_id = normalize_text(channel.tvg_id)

    if normalized_id:
        for cid, item in channel_index.items():
            if normalize_text(cid) == normalized_id:
                return item

    # Name matching
    source_names = [
        channel.name,
        channel.tvg_name,
    ]

    source_names = [
        normalize_text(x)
        for x in source_names
        if x
    ]

    best_item = None
    best_score = 0

    for item in channel_index.values():
        candidates = [
            normalize_text(x)
            for x in item["names"]
            if x
        ]

        for source_name in source_names:
            for candidate in candidates:
                score = fuzz.token_set_ratio(
                    source_name,
                    candidate,
                )

                if score > best_score:
                    best_score = score
                    best_item = item

    if best_item and best_score >= 92:
        return best_item

    return None


# ============================================================
# LOGO
# ============================================================

def choose_logo(
    channel: Channel,
    matched: Optional[dict],
    logo_index: dict,
) -> str:

    # Raw source ALWAYS wins.
    if channel.logo:
        return channel.logo

    if not matched:
        return ""

    candidates = logo_index.get(
        matched["id"].lower(),
        [],
    )

    if not candidates:
        return ""

    def logo_score(item):
        score = 0

        if item.get("in_use"):
            score += 100

        tags = [
            str(x).lower()
            for x in item.get("tags", [])
        ]

        if "horizontal" in tags:
            score += 20

        fmt = str(item.get("format", "")).lower()

        if fmt == "svg":
            score += 10
        elif fmt == "png":
            score += 8
        elif fmt in {"jpg", "jpeg", "webp"}:
            score += 5

        width = item.get("width") or 0
        height = item.get("height") or 0

        if width and height and width >= height:
            score += 5

        return score

    candidates = sorted(
        candidates,
        key=logo_score,
        reverse=True,
    )

    return candidates[0].get("url", "")


# ============================================================
# STREAM VALIDATION
# ============================================================

GOOD_STATUS = {200, 206}

HLS_CONTENT_TYPES = {
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
    "audio/x-mpegurl",
}


def is_hls(url: str, content_type: str) -> bool:
    path = urlparse(url).path.lower()

    if ".m3u8" in path:
        return True

    content_type = content_type.lower()

    return any(
        x in content_type
        for x in HLS_CONTENT_TYPES
    )


def safe_request_headers(channel: Channel) -> dict:
    headers = {
        "User-Agent": DALVIK_UA,
        "Accept": "*/*",
    }

    # Respect source-provided HTTP metadata.
    for key, value in channel.attrs.items():

        key_lower = key.lower()

        if key_lower in {
            "http-referrer",
            "http-referer",
        }:
            headers["Referer"] = value

        elif key_lower in {
            "http-user-agent",
        }:
            headers["User-Agent"] = value

    return headers


def check_url(
    url: str,
    channel: Channel,
) -> StreamHealth:

    health = StreamHealth(url=url)

    headers = safe_request_headers(channel)

    start = time.monotonic()

    try:
        response = SESSION.head(
            url,
            headers=headers,
            timeout=STREAM_TIMEOUT,
            allow_redirects=True,
        )

        health.status = response.status_code
        health.content_type = (
            response.headers.get(
                "Content-Type",
                "",
            ).split(";")[0].strip().lower()
        )

    except Exception as exc:
        health.error = f"HEAD: {exc}"

    health.latency = time.monotonic() - start

    # --------------------------------------------------------
    # HEAD may be blocked by IPTV servers.
    # Do bounded GET instead.
    # --------------------------------------------------------

    if health.status not in GOOD_STATUS:

        try:
            start = time.monotonic()

            response = SESSION.get(
                url,
                headers={
                    **headers,
                    "Range": "bytes=0-65535",
                },
                timeout=STREAM_TIMEOUT,
                allow_redirects=True,
                stream=True,
            )

            health.status = response.status_code

            health.content_type = (
                response.headers.get(
                    "Content-Type",
                    "",
                ).split(";")[0].strip().lower()
            )

            health.latency = time.monotonic() - start

            body = next(
                response.iter_content(
                    chunk_size=65536
                ),
                b"",
            )

            response.close()

            if body:
                health.alive = (
                    health.status in GOOD_STATUS
                )

        except Exception as exc:
            health.error = f"GET: {exc}"

    # --------------------------------------------------------
    # HLS validation
    # --------------------------------------------------------

    health.is_hls = is_hls(
        url,
        health.content_type,
    )

    if health.is_hls and health.status in GOOD_STATUS:

        try:
            response = SESSION.get(
                url,
                headers=headers,
                timeout=STREAM_TIMEOUT,
                allow_redirects=True,
            )

            text = response.text[:128 * 1024]
            response.close()

            if "#EXTM3U" in text:

                health.playlist_valid = True

                media_urls = []

                for line in text.splitlines():

                    line = line.strip()

                    if not line:
                        continue

                    if line.startswith("#"):
                        continue

                    media_urls.append(
                        urljoin(
                            response.url,
                            line,
                        )
                    )

                    if len(media_urls) >= 3:
                        break

                # Validate at least one media segment.
                for media_url in media_urls:

                    try:
                        segment = SESSION.get(
                            media_url,
                            headers={
                                **headers,
                                "Range": "bytes=0-4095",
                            },
                            timeout=STREAM_TIMEOUT,
                            allow_redirects=True,
                            stream=True,
                        )

                        status = segment.status_code
                        segment.close()

                        if status in GOOD_STATUS:
                            health.segment_alive = True
                            break

                    except Exception:
                        continue

        except Exception as exc:
            health.error = f"HLS: {exc}"

    # --------------------------------------------------------
    # Final alive state
    # --------------------------------------------------------

    if health.is_hls:

        health.alive = (
            health.status in GOOD_STATUS
            and health.playlist_valid
            and health.segment_alive
        )

    else:

        health.alive = (
            health.status in GOOD_STATUS
        )

    # --------------------------------------------------------
    # Score
    # --------------------------------------------------------

    score = 0

    if health.status == 200:
        score += 35
    elif health.status == 206:
        score += 32
    elif 200 <= health.status < 300:
        score += 25

    if health.is_hls:
        score += 10

    if health.playlist_valid:
        score += 30

    if health.segment_alive:
        score += 25

    if health.alive:
        score += 20

    # latency bonus
    if health.latency < 1:
        score += 10
    elif health.latency < 2:
        score += 7
    elif health.latency < 4:
        score += 4

    health.score = score

    return health


# ============================================================
# CHANNEL CANONICALIZATION
# ============================================================

def canonicalize_channels(
    channels: list[Channel],
    channel_index: dict,
    logo_index: dict,
):

    for channel in channels:

        matched = match_iptv_channel(
            channel,
            channel_index,
        )

        if matched:

            channel.canonical_id = matched["id"]

            channel.canonical_name = (
                matched["name"]
                or channel.name
            )

            channel.logo = choose_logo(
                channel,
                matched,
                logo_index,
            )

        else:

            channel.canonical_id = (
                channel.tvg_id
                or normalize_text(channel.name)
            )

            channel.canonical_name = (
                channel.tvg_name
                or channel.name
            )

        channel.group = normalize_group(
            channel.group
        )


# ============================================================
# FILTER
# ============================================================

def should_exclude(channel: Channel) -> bool:

    source = channel.source.lower()

    group = normalize_text(channel.group)

    for excluded in EXCLUDED_GROUPS.get(
        source,
        set(),
    ):

        if group == normalize_text(excluded):
            return True

        if fuzz.token_set_ratio(
            group,
            normalize_text(excluded),
        ) >= 94:
            return True

    cid = normalize_text(channel.canonical_id)

    for excluded in EXCLUDED_CHANNEL_IDS.get(
        source,
        set(),
    ):

        if cid == normalize_text(excluded):
            return True

        if normalize_text(channel.tvg_id) == normalize_text(excluded):
            return True

    return False


# ============================================================
# DEDUPLICATION
# ============================================================

def same_channel(a: Channel, b: Channel) -> bool:

    if (
        a.canonical_id
        and b.canonical_id
        and normalize_text(a.canonical_id)
        == normalize_text(b.canonical_id)
    ):
        return True

    name_score = channel_similarity(
        a.canonical_name or a.name,
        b.canonical_name or b.name,
    )

    group_score = group_similarity(
        a.group,
        b.group,
    )

    return (
        name_score >= 0.94
        and group_score >= 0.70
    )


def deduplicate(
    channels: list[Channel],
) -> list[Channel]:

    result: list[Channel] = []

    for channel in channels:

        # ----------------------------------------------------
        # Never run expensive fuzzy matching if exact ID
        # already identifies the channel.
        # ----------------------------------------------------

        merged = False

        for existing in result:

            if same_channel(
                channel,
                existing,
            ):
                merged = True

                candidates = [
                    existing,
                    channel,
                ]

                winner = max(
                    candidates,
                    key=lambda x: (
                        x.health.score
                        if x.health
                        else 0,
                        -x.source_priority,
                    ),
                )

                if winner is not existing:
                    idx = result.index(existing)
                    result[idx] = winner

                break

        if not merged:
            result.append(channel)

    return result


# ============================================================
# STREAM URL DEDUP
# ============================================================

def choose_best_stream(
    channels: list[Channel],
) -> list[Channel]:

    grouped: dict[str, list[Channel]] = {}

    for channel in channels:

        key = (
            normalize_text(channel.canonical_id)
            or normalize_text(channel.canonical_name)
            or normalize_text(channel.name)
        )

        grouped.setdefault(
            key,
            [],
        ).append(channel)

    result = []

    for candidates in grouped.values():

        # Same channel:
        # keep exactly ONE URL.

        unique_urls = {}

        for channel in candidates:

            if channel.url not in unique_urls:
                unique_urls[channel.url] = channel

            else:

                old = unique_urls[channel.url]

                if (
                    channel.source_priority
                    < old.source_priority
                ):
                    unique_urls[channel.url] = channel

        candidates = list(
            unique_urls.values()
        )

        winner = max(
            candidates,
            key=lambda x: (
                1 if x.health and x.health.alive else 0,
                x.health.score if x.health else 0,
                -x.source_priority,
                -(x.health.latency if x.health else 9999),
            ),
        )

        result.append(winner)

    return result


# ============================================================
# SORT
# ============================================================

def group_sort_key(group: str):

    if group in GROUP_ORDER:
        return (
            GROUP_ORDER.index(group),
            group.lower(),
        )

    return (
        999,
        normalize_text(group),
    )


def channel_sort_key(channel: Channel):

    name = normalize_text(
        channel.canonical_name
        or channel.name
    )

    return (
        group_sort_key(channel.group),
        name,
    )


# ============================================================
# OUTPUT
# ============================================================

def build_header(epgs: list[str]) -> str:

    epgs = sorted(set(
        x.strip()
        for x in epgs
        if x.strip()
    ))

    if epgs:
        return (
            '#EXTM3U '
            f'url-tvg="{",".join(epgs)}"'
        )

    return "#EXTM3U"


def build_extinf(channel: Channel) -> str:

    attrs = dict(channel.attrs)

    # Canonical normalized values.
    if channel.canonical_id:
        attrs["tvg-id"] = channel.canonical_id

    if channel.canonical_name:
        attrs["tvg-name"] = channel.canonical_name

    if channel.logo:
        attrs["tvg-logo"] = channel.logo

    attrs["group-title"] = channel.group

    attr_text = " ".join(
        f'{key}="{value}"'
        for key, value in attrs.items()
        if value != ""
    )

    return (
        f"#EXTINF:-1 {attr_text},"
        f"{channel.canonical_name or channel.name}"
    )


def write_m3u(
    channels: list[Channel],
    epgs: list[str],
):

    lines = [
        build_header(epgs),
    ]

    for channel in sorted(
        channels,
        key=channel_sort_key,
    ):

        lines.append(
            build_extinf(channel)
        )

        # Preserve KODIPROP / EXTVLCOPT /
        # EXTHTTP / EXT-X-* etc.
        for extra in channel.extra_lines:

            if extra.startswith(
                (
                    "#KODIPROP",
                    "#EXTVLCOPT",
                    "#EXTHTTP",
                    "#EXT-X-",
                    "#EXTGRP",
                    "#EXT-X-STREAM-INF",
                )
            ):
                lines.append(extra)

        lines.append(channel.url)

    OUTPUT.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


# ============================================================
# REPORT
# ============================================================

def redact_url(url: str) -> str:

    try:

        parsed = urlparse(url)

        sensitive = {
            "token",
            "auth",
            "key",
            "signature",
            "sig",
            "play_token",
            "access_token",
            "jwt",
        }

        query = []

        for key, value in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        ):

            if key.lower() in sensitive:
                value = "***"

            query.append(
                (key, value)
            )

        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urlencode(query),
                parsed.fragment,
            )
        )

    except Exception:
        return "<redacted>"


def write_report(
    channels: list[Channel],
    source_stats: dict,
):

    report = {
        "generated_at": time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(),
        ),
        "channels": len(channels),
        "alive": sum(
            1
            for x in channels
            if x.health and x.health.alive
        ),
        "dead": sum(
            1
            for x in channels
            if not x.health or not x.health.alive
        ),
        "sources": source_stats,
        "items": [],
    }

    for channel in channels:

        report["items"].append({
            "name": channel.canonical_name or channel.name,
            "group": channel.group,
            "tvg_id": channel.canonical_id,
            "source": channel.source,
            "url": redact_url(channel.url),
            "alive": (
                channel.health.alive
                if channel.health
                else False
            ),
            "status": (
                channel.health.status
                if channel.health
                else 0
            ),
            "content_type": (
                channel.health.content_type
                if channel.health
                else ""
            ),
            "latency": (
                round(channel.health.latency, 3)
                if channel.health
                else None
            ),
            "hls": (
                channel.health.is_hls
                if channel.health
                else False
            ),
            "playlist_valid": (
                channel.health.playlist_valid
                if channel.health
                else False
            ),
            "segment_alive": (
                channel.health.segment_alive
                if channel.health
                else False
            ),
            "score": (
                channel.health.score
                if channel.health
                else 0
            ),
        })

    REPORT.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


# ============================================================
# MAIN
# ============================================================

def main():

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    logging.info("Loading IPTV-org API...")

    try:
        channel_data = load_json(CHANNEL_API)
        logo_data = load_json(LOGO_API)

        channel_index = build_channel_index(
            channel_data
        )

        logo_index = build_logo_index(
            logo_data
        )

    except Exception as exc:

        logging.warning(
            "IPTV-org API unavailable: %s",
            exc,
        )

        channel_index = {}
        logo_index = {}

    all_channels = []
    all_epgs = []

    source_stats = {}

    # --------------------------------------------------------
    # Download sources
    # --------------------------------------------------------

    for source in SOURCES:

        name = source["name"]

        logging.info(
            "Fetching source: %s",
            name,
        )

        try:

            text = fetch_source(
                source["url"]
            )

            channels, epgs = parse_m3u(
                text,
                name,
                source["priority"],
            )

            before = len(channels)

            channels = [
                x
                for x in channels
                if not should_exclude(x)
            ]

            filtered = before - len(channels)

            all_channels.extend(channels)
            all_epgs.extend(epgs)

            source_stats[name] = {
                "downloaded": before,
                "excluded": filtered,
                "remaining": len(channels),
            }

            logging.info(
                "%s: %d channels, %d excluded",
                name,
                len(channels),
                filtered,
            )

        except Exception as exc:

            logging.exception(
                "Source failed: %s",
                name,
            )

            source_stats[name] = {
                "error": str(exc),
            }

    logging.info(
        "Total raw channels: %d",
        len(all_channels),
    )

    # --------------------------------------------------------
    # Canonical mapping
    # --------------------------------------------------------

    canonicalize_channels(
        all_channels,
        channel_index,
        logo_index,
    )

    # --------------------------------------------------------
    # Health check
    # --------------------------------------------------------

    logging.info(
        "Checking stream health..."
    )

    for index, channel in enumerate(
        all_channels,
        start=1,
    ):

        logging.info(
            "[%d/%d] %s",
            index,
            len(all_channels),
            channel.name,
        )

        channel.health = check_url(
            channel.url,
            channel,
        )

    # --------------------------------------------------------
    # Dedup
    # --------------------------------------------------------

    logging.info(
        "Fuzzy deduplicating..."
    )

    channels = deduplicate(
        all_channels
    )

    # One stream per channel.
    channels = choose_best_stream(
        channels
    )

    # --------------------------------------------------------
    # Output
    # --------------------------------------------------------

    write_m3u(
        channels,
        all_epgs,
    )

    write_report(
        channels,
        source_stats,
    )

    alive = sum(
        1
        for x in channels
        if x.health and x.health.alive
    )

    logging.info(
        "========================================"
    )

    logging.info(
        "FINAL CHANNELS : %d",
        len(channels),
    )

    logging.info(
        "ALIVE           : %d",
        alive,
    )

    logging.info(
        "DEAD            : %d",
        len(channels) - alive,
    )

    logging.info(
        "OUTPUT          : %s",
        OUTPUT,
    )

    logging.info(
        "REPORT          : %s",
        REPORT,
    )


if __name__ == "__main__":
    main()
