#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV M3U Optimizer

Pipeline:
    remote sources
        -> fetch
        -> parse
        -> filter
        -> canonical identity
        -> deduplicate
        -> priority group lock
        -> event classifier
        -> content classifier
        -> render
        -> m3u/listtivi.m3u
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
import unicodedata

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import yaml


# ============================================================
# CONFIG
# ============================================================

DEFAULT_OUTPUT = Path("m3u/listtivi.m3u")
DEFAULT_MAPPING = Path("m3u/canonical_channels.yml")

SOURCE_URLS = {
    "vmttv": (
        "https://raw.githubusercontent.com/"
        "vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv"
    ),
    "vietanhtv": "https://tv.vietanhtv.top/sex/",
    "dltivi": (
        "https://raw.githubusercontent.com/"
        "DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl"
    ),
    "iptv-org": (
        "https://raw.githubusercontent.com/"
        "iptv-org/iptv/refs/heads/master/streams/vn.m3u"
    ),
    "easport": "https://livesport.s.gy/easport",
}

SOURCE_PRIORITY = {
    "vmttv": 500,
    "vietanhtv": 400,
    "dltivi": 300,
    "iptv-org": 200,
    "easport": 100,
}

# User-Agent OTT IPTV App giả lập TiviMate để fetch mượt các nguồn anti-browser như EASport
OTT_UA = "TiviMate/4.7.0"
DALVIK_UA = "Dalvik/2.1.0 (Linux; U; Android 10; K)"

COMMON_HEADERS = {
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate",
}


# ============================================================
# FINAL GROUPS
# ============================================================

FINAL_GROUPS = {
    "VTV": "📺 VTV",
    "HTV": "📺 HTV",
    "SCTV": "📺 SCTV",
    "VTVCAB": "📡 VTVCab",
    "HTVC": "📡 HTVC",
    "THIET_YEU": "⭐ Thiết yếu",
    "DIA_PHUONG": "🏙️ Địa phương",
    "SU_KIEN": "🎟️ Sự kiện",
    "THE_THAO": "🏆 Thể thao",
    "PHIM": "🎬 Phim",
    "THIEU_NHI": "👧 Thiếu nhi",
    "AM_NHAC": "🎵 Âm nhạc",
    "TIN_TUC": "📰 Tin tức",
    "QUOC_TE": "🌍 Quốc tế",
    "KHAC": "📦 Khác",
}


PRIORITY_GROUPS = {
    "VTV",
    "HTV",
    "SCTV",
    "VTVCAB",
    "HTVC",
    "THIET_YEU",
    "DIA_PHUONG",
}


# ============================================================
# FILTER RULES
# ============================================================

UPDATE_GROUP_RE = re.compile(
    r"^\s*update\s+\d{1,2}:\d{2}\b.*$",
    re.IGNORECASE,
)

RADIO_RE = re.compile(
    r"""
    (
        \bradio\b
        |\bfm\b
        |\bam\b
        |\bvov\b
        |\bvov[0-9a-z]*\b
        |phat\s*thanh
        |phát\s*thanh
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

VMTTV_EXCLUDED_GROUPS = {
    "live events",
    "radio",
    "uk radio",
    "israel",
    "hàn quốc",
    "trung quốc",
    "thái lan",
    "cola tv",
    "cola tv sv2",
    "pháo hoa tv",
}

VIETANHTV_EXCLUDED_GROUPS = {
    "update",
    "dự phòng",
    "fpt",
    "sự kiện 360",
    "rạp phim",
    "radio",
    "socolive",
}

DLTIVI_EXCLUDED_GROUPS = {
    "vov",
}

EASPORT_EXCLUDED_GROUPS = {
    "info",
}


# ============================================================
# TEXT NORMALIZATION
# ============================================================

def strip_accents(value: str) -> str:
    value = value.replace("Đ", "D").replace("đ", "d")
    value = unicodedata.normalize("NFD", value)
    return "".join(c for c in value if unicodedata.category(c) != "Mn")


def normalize_text(value: str) -> str:
    if not value:
        return ""

    value = str(value).strip()
    value = value.replace("_", " ").replace("-", " ").replace(".", " ")
    value = value.replace("@", " ").replace("/", " ").replace("|", " ")

    value = strip_accents(value).lower()
    value = re.sub(r"\s+", " ", value)

    value = re.sub(
        r"\b(uhd|fhd|fullhd|hd|sd|4k|1080p|720p)\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    return re.sub(r"\s+", " ", value).strip()


def compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_text(value))


def clean_display_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.strip())


# ============================================================
# M3U ENTRY
# ============================================================

@dataclass
class M3UEntry:
    source: str
    extinf: str
    url: str
    extra_lines: List[str] = field(default_factory=list)

    duration: str = "-1"
    original_name: str = ""

    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""

    canonical_id: str = ""
    canonical_score: int = 0
    canonical_reason: str = ""

    canonical_name: str = ""
    canonical_group: str = ""
    epg_id: str = ""

    source_score: int = 0


# ============================================================
# EXTINF PARSER
# ============================================================

ATTR_RE = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')


def parse_extinf(line: str) -> Dict[str, str]:
    return {m.group(1).lower(): m.group(2) for m in ATTR_RE.finditer(line)}


def parse_display_name(line: str) -> str:
    if "," not in line:
        return ""
    return line.split(",", 1)[1].strip()


def parse_m3u(text: str, source: str) -> List[M3UEntry]:
    entries: List[M3UEntry] = []
    current_extinf: Optional[str] = None
    current_extra: List[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("#EXTINF:"):
            current_extinf = line
            current_extra = []
            continue

        if current_extinf is None:
            continue

        if not line.startswith("#"):
            attrs = parse_extinf(current_extinf)
            comma_pos = current_extinf.find(",")
            duration = "-1"

            if comma_pos >= 0:
                duration_part = current_extinf[len("#EXTINF:"):comma_pos]
                duration = duration_part.strip()

            entries.append(
                M3UEntry(
                    source=source,
                    extinf=current_extinf,
                    url=line,
                    extra_lines=list(current_extra),
                    duration=duration,
                    original_name=parse_display_name(current_extinf),
                    tvg_id=attrs.get("tvg-id", ""),
                    tvg_name=attrs.get("tvg-name", ""),
                    tvg_logo=attrs.get("tvg-logo", ""),
                    group_title=attrs.get("group-title", ""),
                    source_score=SOURCE_PRIORITY.get(source, 0),
                )
            )
            current_extinf = None
            current_extra = []
            continue

        current_extra.append(line)

    return entries


# ============================================================
# CANONICAL RESOLVER
# ============================================================

class CanonicalResolver:

    def __init__(self, mapping_path: Path):
        self.mapping_path = mapping_path
        self.channels: Dict[str, dict] = {}
        self.alias_exact: Dict[str, str] = {}
        self.alias_compact: Dict[str, str] = {}
        self.ambiguous_exact = set()
        self.ambiguous_compact = set()
        self.vtvcab_number_index: Dict[int, str] = {}

        self.load()

    def load(self) -> None:
        if not self.mapping_path.exists():
            raise FileNotFoundError(f"Canonical mapping not found: {self.mapping_path}")

        with self.mapping_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if "channels" in data:
            data = data["channels"]

        if not isinstance(data, dict):
            raise ValueError("canonical_channels.yml must contain a 'channels' mapping.")

        for canonical_id, raw in data.items():
            if not isinstance(raw, dict):
                raw = {}

            canonical_id = str(canonical_id).strip()
            self.channels[canonical_id] = raw

            aliases = raw.get("aliases", [])
            if isinstance(aliases, str):
                aliases = [aliases]
            aliases = list(aliases)
            aliases.append(canonical_id)

            if raw.get("name"):
                aliases.append(str(raw["name"]))

            provider_names = raw.get("provider_names", [])
            if isinstance(provider_names, str):
                provider_names = [provider_names]
            aliases.extend(str(x) for x in provider_names)

            for alias in aliases:
                self._register_alias(str(alias), canonical_id)

            number = raw.get("vtvcab_number")
            if number is not None:
                try:
                    self.vtvcab_number_index[int(number)] = canonical_id
                except (TypeError, ValueError):
                    pass

    def _register_alias(self, alias: str, canonical_id: str) -> None:
        exact = normalize_text(alias)
        short = compact(alias)

        if exact:
            old = self.alias_exact.get(exact)
            if old is not None and old != canonical_id:
                self.ambiguous_exact.add(exact)
            elif exact not in self.ambiguous_exact:
                self.alias_exact[exact] = canonical_id

        if short:
            old = self.alias_compact.get(short)
            if old is not None and old != canonical_id:
                self.ambiguous_compact.add(short)
            elif short not in self.ambiguous_compact:
                self.alias_compact[short] = canonical_id

    @staticmethod
    def detect_vtvcab_number(value: str) -> Optional[int]:
        if not value:
            return None
        normalized = normalize_text(value)
        compact_value = compact(value)

        patterns = (r"\bvtv\s*cab\s*(\d+)\b", r"\bvtvcab\s*(\d+)\b")
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.IGNORECASE)
            if match:
                return int(match.group(1))

        match = re.fullmatch(r"vtvcab(\d+)", compact_value)
        if match:
            return int(match.group(1))
        return None

    def detect_known_family(self, values: Iterable[str]) -> Optional[str]:
        for value in values:
            key = compact(value)
            if not key:
                continue

            for prefix in ("vtv", "htv", "sctv"):
                match = re.fullmatch(rf"{prefix}(\d+)(?:hd)?", key)
                if match:
                    candidate = f"{prefix}{match.group(1)}"
                    if candidate in self.channels:
                        return candidate
        return None

    def resolve(self, entry: M3UEntry) -> Tuple[Optional[str], int, str]:
        candidates = (
            ("tvg-id", entry.tvg_id, 100),
            ("name", entry.original_name, 90),
            ("tvg-name", entry.tvg_name, 90),
            ("group-title", entry.group_title, 80),
        )

        for reason, value, score in candidates:
            key = normalize_text(value)
            if not key or key in self.ambiguous_exact:
                continue
            canonical_id = self.alias_exact.get(key)
            if canonical_id:
                return canonical_id, score, f"{reason}:exact"

        for reason, value, score in candidates:
            key = compact(value)
            if not key or key in self.ambiguous_compact:
                continue
            canonical_id = self.alias_compact.get(key)
            if canonical_id:
                return canonical_id, score - 5, f"{reason}:compact"

        for reason, value, _ in candidates:
            number = self.detect_vtvcab_number(value)
            if number is None:
                continue
            canonical_id = self.vtvcab_number_index.get(number)
            if canonical_id:
                return canonical_id, 95, f"{reason}:vtvcab_number:{number}"

        family = self.detect_known_family(value for _, value, _ in candidates)
        if family:
            return family, 70, "known_family"

        return None, 0, "unknown"

    def get(self, canonical_id: str) -> dict:
        return self.channels.get(canonical_id, {})


# ============================================================
# FILTER HELPERS
# ============================================================

def normalized_group(group: str) -> str:
    return normalize_text(group)


VMTTV_EXCLUDED = {normalized_group(x) for x in VMTTV_EXCLUDED_GROUPS}
VIETANHTV_EXCLUDED = {normalized_group(x) for x in VIETANHTV_EXCLUDED_GROUPS}
DLTIVI_EXCLUDED = {normalized_group(x) for x in DLTIVI_EXCLUDED_GROUPS}
EASPORT_EXCLUDED = {normalized_group(x) for x in EASPORT_EXCLUDED_GROUPS}


def is_update_group(group: str) -> bool:
    return bool(UPDATE_GROUP_RE.match(group or ""))


def is_global_radio(entry: M3UEntry) -> bool:
    text = " ".join([entry.group_title, entry.tvg_id, entry.tvg_name, entry.original_name])
    return bool(RADIO_RE.search(text))


def entry_filter_text(entry: M3UEntry) -> str:
    return normalize_text(" ".join([entry.group_title, entry.tvg_id, entry.tvg_name, entry.original_name, entry.url]))


def is_vmttv_blocked_content(entry: M3UEntry) -> Optional[str]:
    if entry.source != "vmttv":
        return None
    group = normalized_group(entry.group_title)
    compact_text = compact(entry_filter_text(entry))

    if group == "live events" or "liveevents" in compact_text:
        return "LIVE EVENTS"
    if group == "cola tv sv2" or "colatvsv2" in compact_text:
        return "COLA TV SV2"
    return None


def group_is_excluded(entry: M3UEntry) -> bool:
    group = normalized_group(entry.group_title)
    if is_update_group(group) or is_global_radio(entry):
        return True
    if is_vmttv_blocked_content(entry):
        return True

    excluded = {
        "vmttv": VMTTV_EXCLUDED,
        "vietanhtv": VIETANHTV_EXCLUDED,
        "dltivi": DLTIVI_EXCLUDED,
        "easport": EASPORT_EXCLUDED,
    }.get(entry.source, set())

    return group in excluded


def is_vsbet(entry: M3UEntry) -> bool:
    text = " ".join([entry.tvg_id, entry.tvg_name, entry.original_name])
    return "vsbet" in compact(text)


GAMBLING_BRAND_NAME_RE = re.compile(r"^\s*blv\b", re.IGNORECASE)
GAMBLING_BRAND_DOMAINS = ("msdht.app", "phaohoa.live", "phaohoa1.live")
GAMBLING_BRAND_LOGO_MARKERS = ("colatv_logo", "phaohoa1.live")


def is_gambling_brand(entry: M3UEntry) -> bool:
    name = normalize_text(entry.original_name or entry.tvg_name)
    if GAMBLING_BRAND_NAME_RE.match(name):
        return True
    url_lower = (entry.url or "").lower()
    if any(domain in url_lower for domain in GAMBLING_BRAND_DOMAINS):
        return True
    logo_lower = (entry.tvg_logo or "").lower()
    if any(marker in logo_lower for marker in GAMBLING_BRAND_LOGO_MARKERS):
        return True
    return False


def should_remove(entry: M3UEntry) -> bool:
    return group_is_excluded(entry) or is_vsbet(entry) or is_gambling_brand(entry)


# ============================================================
# CANONICAL APPLY
# ============================================================

def apply_canonical(entry: M3UEntry, resolver: CanonicalResolver) -> None:
    canonical_id, score, reason = resolver.resolve(entry)

    if canonical_id:
        entry.canonical_id = canonical_id
        entry.canonical_score = score
        entry.canonical_reason = reason
        mapping = resolver.get(canonical_id)

        entry.canonical_name = str(mapping.get("name", "")).strip()
        entry.canonical_group = str(mapping.get("group", "")).strip()
        entry.epg_id = str(mapping.get("epg_id", "")).strip()
        return

    identity = compact(entry.tvg_id or entry.tvg_name or entry.original_name)
    if not identity:
        identity = compact(entry.url)

    digest = hashlib.sha1(identity.encode("utf-8")).hexdigest()[:12]
    entry.canonical_id = f"local-{digest}"
    entry.canonical_score = 10
    entry.canonical_reason = "local_identity"


# ============================================================
# GROUP HELPERS & CLASSIFICATION
# ============================================================

def priority_group_from_mapping(mapping: dict) -> Optional[str]:
    group_key = str(mapping.get("group", "")).strip()
    if group_key in PRIORITY_GROUPS:
        return FINAL_GROUPS[group_key]
    return None


def is_event_channel(entry: M3UEntry, mapping: dict) -> bool:
    if priority_group_from_mapping(mapping):
        return False

    text = normalize_text(
        " ".join(
            [
                entry.source,
                entry.group_title,
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                str(mapping.get("name", "")),
                str(mapping.get("provider", "")),
            ]
        )
    )

    mapped_group = str(mapping.get("group", "")).strip()
    if mapped_group == "SU_KIEN":
        return True

    if "vtvprime" in compact(text) or "vtv prime" in text:
        return True

    if any(k in text for k in ("tv360 su kien", "tv360 event", "fpt su kien", "fpt play su kien", "fpt event")):
        return True

    if re.search(r"\bsu kien\b", text) or re.search(r"\bevent\b", text):
        return True

    if "home_event" in (entry.tvg_logo or "").lower():
        return True

    return False


def classify_group(entry: M3UEntry, mapping: dict) -> str:
    locked = priority_group_from_mapping(mapping)
    if locked:
        return locked

    provider = normalize_text(str(mapping.get("provider", "")))
    if provider in ("vtvcab", "htvc", "sctv", "vtv", "htv"):
        return FINAL_GROUPS[provider.upper()]

    text = normalize_text(" ".join([entry.tvg_id, entry.tvg_name, entry.original_name, entry.group_title]))
    compact_text = compact(text)

    if re.search(r"\bvtv\s*\d+\b", text):
        return FINAL_GROUPS["VTV"]
    if re.search(r"\bhtv\s*\d+\b", text):
        return FINAL_GROUPS["HTV"]
    if re.search(r"\bsctv\b", text) or re.search(r"\bsctv\s*\d+\b", text):
        return FINAL_GROUPS["SCTV"]
    if "htvc" in compact_text:
        return FINAL_GROUPS["HTVC"]
    if "vtvcab" in compact_text:
        return FINAL_GROUPS["VTVCAB"]

    essential_patterns = (r"\bqpvn\b", r"\bquoc phong\b", r"\bantv\b", r"\ban ninh\b")
    if any(re.search(pattern, text) for pattern in essential_patterns):
        return FINAL_GROUPS["THIET_YEU"]

    # ĐỊA PHƯƠNG KEYWORDS (Đã phủ rộng 63 tỉnh thành)
    local_keywords = (
        "hanoi tv", "ha noi tv", "hanoitv", "hai phong", "da nang", "can tho", "quang ninh",
        "hai duong", "bac ninh", "nam dinh", "thai nguyen", "nghe an", "ha tinh", "quang binh",
        "quang tri", "thua thien hue", "hue tv", "hue", "binh dinh", "khanh hoa", "dak lak",
        "lam dong", "dong nai", "binh duong", "ba ria", "vung tau", "tay ninh", "long an",
        "tien giang", "ben tre", "vinh long", "dong thap", "an giang", "kien giang", "ca mau",
        "soc trang", "bac lieu", "tra vinh", "binh thuan", "phu yen", "gia lai", "kon tum",
        "dak nong", "ninh thuan", "son la", "dien bien", "lai chau", "lao cai", "yen bai",
        "ha giang", "cao bang", "bac kan", "tuyen quang", "thai binh", "hung yen", "lang son",
        "ninh binh", "hoa binh", "vinh phuc", "bac giang", "quang ngai", "phu tho", "quang nam",
        "binh phuoc", "sai gon", "tp hcm", "ho chi minh", "dai ptth", "thbrt", "la34", "thvl",
    )
    if any(keyword in text for keyword in local_keywords):
        return FINAL_GROUPS["DIA_PHUONG"]

    if is_event_channel(entry, mapping):
        return FINAL_GROUPS["SU_KIEN"]

    sports_keywords = ("the thao", "sport", "sports", "football", "soccer", "basketball", "tennis", "golf")
    if any(keyword in text for keyword in sports_keywords):
        return FINAL_GROUPS["THE_THAO"]

    movie_keywords = ("phim", "movie", "movies", "cinema", "film", "tvb")
    if any(keyword in text for keyword in movie_keywords):
        return FINAL_GROUPS["PHIM"]

    kids_keywords = ("thieu nhi", "kids", "kid", "children", "cartoon", "baby")
    if any(keyword in text for keyword in kids_keywords):
        return FINAL_GROUPS["THIEU_NHI"]

    music_keywords = ("am nhac", "music", "mtv", "karaoke")
    if any(keyword in text for keyword in music_keywords):
        return FINAL_GROUPS["AM_NHAC"]

    news_keywords = ("tin tuc", "news", "bbc", "cnn", "bloomberg")
    if any(keyword in text for keyword in news_keywords):
        return FINAL_GROUPS["TIN_TUC"]

    return FINAL_GROUPS["KHAC"]


# ============================================================
# DEDUPLICATE & SCORING
# ============================================================

def url_key(url: str) -> str:
    return re.sub(r"\s+", "", url.strip()).lower()


def winner_score(entry: M3UEntry) -> int:
    score = entry.canonical_score * 1000 + entry.source_score
    if entry.tvg_logo:
        score += 5
    if entry.tvg_id:
        score += 5
    if entry.tvg_name:
        score += 5
    return score


def deduplicate(entries: List[M3UEntry]) -> Tuple[List[M3UEntry], Dict[str, List[M3UEntry]]]:
    grouped: Dict[str, List[M3UEntry]] = defaultdict(list)
    for entry in entries:
        grouped[entry.canonical_id].append(entry)

    winners: List[M3UEntry] = []

    for candidates in grouped.values():
        unique_by_url: Dict[str, M3UEntry] = {}
        for entry in candidates:
            key = url_key(entry.url)
            if not key:
                continue
            old = unique_by_url.get(key)
            if old is None or winner_score(entry) > winner_score(old):
                unique_by_url[key] = entry

        candidates = list(unique_by_url.values())
        if not candidates:
            continue

        candidates.sort(key=winner_score, reverse=True)
        winners.append(candidates[0])

    return winners, grouped


# ============================================================
# EXTINF BUILD
# ============================================================

def upsert_extinf_attr(line: str, attr: str, value: str) -> str:
    pattern = re.compile(rf'({re.escape(attr)}=")[^"]*(")', re.IGNORECASE)
    if pattern.search(line):
        return pattern.sub(lambda m: m.group(1) + value + m.group(2), line, count=1)

    comma = line.find(",")
    if comma < 0:
        return line

    return f'{line[:comma]} {attr}="{value}"{line[comma:]}'


def prepare_output_entry(entry: M3UEntry, resolver: CanonicalResolver) -> None:
    mapping = resolver.get(entry.canonical_id)
    display_name = clean_display_name(
        entry.canonical_name or entry.tvg_name or entry.original_name or entry.tvg_id or entry.canonical_id
    )

    locked_group = priority_group_from_mapping(mapping)
    final_group = locked_group if locked_group else classify_group(entry, mapping)
    entry.canonical_group = final_group

    epg_id = entry.epg_id or mapping.get("epg_id") or entry.tvg_id
    logo = entry.tvg_logo or str(mapping.get("logo", "")).strip()

    line = entry.extinf
    line = upsert_extinf_attr(line, "tvg-id", str(epg_id or ""))
    line = upsert_extinf_attr(line, "tvg-name", display_name)
    line = upsert_extinf_attr(line, "group-title", final_group)
    if logo:
        line = upsert_extinf_attr(line, "tvg-logo", logo)

    if "," in line:
        line = f"{line.split(',', 1)[0]},{display_name}"

    entry.extinf = line


# ============================================================
# FETCH
# ============================================================

class FetchError(RuntimeError):
    pass


def build_headers(source: str) -> Dict[str, str]:
    headers = dict(COMMON_HEADERS)

    # Sửa lỗi fetch EASport: Dùng User-Agent giả lập OTT IPTV App thay vì Browser/Dalvik mặc định
    if source == "easport":
        headers.update(
            {
                "User-Agent": OTT_UA,
                "Accept": "*/*",
                "Referer": "https://livesport.s.gy/",
                "Origin": "https://livesport.s.gy",
            }
        )
    else:
        headers["User-Agent"] = DALVIK_UA

    return headers


def fetch_source(session: requests.Session, source: str, url: str, retries: int = 3) -> str:
    headers = build_headers(source)
    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=(15, 45), allow_redirects=True)
            response.raise_for_status()
            content = response.content

            if not content:
                raise FetchError(f"{source}: empty response")

            try:
                text = content.decode("utf-8-sig")
            except UnicodeDecodeError:
                text = content.decode("utf-8", errors="replace")

            if "#EXTINF" not in text.upper():
                preview = text[:300].replace("\n", " ")
                raise FetchError(f"{source}: response does not look like M3U: {preview}")

            return text

        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 8))

    raise FetchError(f"Unable to fetch {source}: {last_error}")


# ============================================================
# RENDER & SORT
# ============================================================

def render_m3u(entries: List[M3UEntry]) -> str:
    lines = ['#EXTM3U url-tvg="https://lichphatsong.io.vn/epg.xml"']
    for entry in entries:
        lines.append(entry.extinf)
        lines.extend(entry.extra_lines)
        lines.append(entry.url)
    return "\n".join(lines) + "\n"


GROUP_ORDER = {
    FINAL_GROUPS["VTV"]: 10,
    FINAL_GROUPS["HTV"]: 20,
    FINAL_GROUPS["SCTV"]: 30,
    FINAL_GROUPS["VTVCAB"]: 40,
    FINAL_GROUPS["HTVC"]: 50,
    FINAL_GROUPS["THIET_YEU"]: 60,
    FINAL_GROUPS["DIA_PHUONG"]: 70,
    FINAL_GROUPS["SU_KIEN"]: 80,
    FINAL_GROUPS["THE_THAO"]: 90,
    FINAL_GROUPS["PHIM"]: 100,
    FINAL_GROUPS["THIEU_NHI"]: 110,
    FINAL_GROUPS["AM_NHAC"]: 120,
    FINAL_GROUPS["TIN_TUC"]: 130,
    FINAL_GROUPS["QUOC_TE"]: 140,
    FINAL_GROUPS["KHAC"]: 900,
}


def natural_key(value: str):
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", value)]


def sort_entries(entries: List[M3UEntry]) -> List[M3UEntry]:
    return sorted(
        entries,
        key=lambda e: (
            GROUP_ORDER.get(e.canonical_group, 999),
            natural_key(e.canonical_name or e.tvg_name or e.original_name),
            e.canonical_id,
        ),
    )


# ============================================================
# MAIN OPTIMIZE
# ============================================================

def optimize(mapping_path: Path, output_path: Path) -> None:
    resolver = CanonicalResolver(mapping_path)
    session = requests.Session()

    all_entries: List[M3UEntry] = []
    raw_counts: Dict[str, int] = {}
    after_filter: Dict[str, int] = {}

    # 1. Fetch & Parse & Filter
    for source, url in SOURCE_URLS.items():
        try:
            text = fetch_source(session, source, url)
            entries = parse_m3u(text, source)
            raw_counts[source] = len(entries)

            # Lọc bỏ kênh thuộc quy tắc cấm (không in log remove chi tiết)
            kept = [entry for entry in entries if not should_remove(entry)]
            after_filter[source] = len(kept)
            all_entries.extend(kept)

        except Exception as exc:
            print(f"[ERROR] {source}: {exc}", file=sys.stderr)
            raw_counts[source] = 0
            after_filter[source] = 0

    if not all_entries:
        raise RuntimeError("All remote sources failed or were empty.")

    # 2. Canonical Identity
    for entry in all_entries:
        apply_canonical(entry, resolver)

    # 3. Deduplicate
    final_entries, _ = deduplicate(all_entries)

    # 4. Final Group Assignment
    for entry in final_entries:
        prepare_output_entry(entry, resolver)

    # 5. Log dạng chuẩn hoá theo yêu cầu: "list A, fetch xxx, canonical yyy, final zzz"
    final_counts = Counter(e.source for e in final_entries)
    for source in SOURCE_URLS:
        f_cnt = raw_counts.get(source, 0)
        c_cnt = after_filter.get(source, 0)
        z_cnt = final_counts.get(source, 0)
        print(f"list {source}, fetch {f_cnt}, canonical {c_cnt}, final {z_cnt}")

    # 6. Render & Output File
    final_entries = sort_entries(final_entries)

    if not final_entries:
        raise RuntimeError("Optimizer produced ZERO channels.")

    output_text = render_m3u(final_entries)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp_path.write_text(output_text, encoding="utf-8")
    tmp_path.replace(output_path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mapping", default=str(DEFAULT_MAPPING))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    try:
        optimize(Path(args.mapping), Path(args.output))
        return 0
    except Exception as exc:
        print(f"\n[FATAL] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
