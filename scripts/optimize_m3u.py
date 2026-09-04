#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV M3U Optimizer
------------------

Nguồn M3U được fetch TRỰC TIẾP từ URL.
Không sử dụng m3u/raw/*.m3u.

Pipeline:

    remote sources
        -> fetch
        -> parse
        -> filter
        -> canonical identity
        -> deduplicate
        -> classify group
        -> render
        -> m3u/listtivi.m3u

Canonical mapping KHÔNG phụ thuộc iptv-org.

Đặc biệt:
    VTV1
    VTV1 HD
    vtv1hd
    VTV1.vn@HD
    VTV1 HD.vn
    ...

có thể được đưa về cùng canonical ID:
    vtv1

Sau đó mới deduplicate.

Điều này rất quan trọng:
    canonicalize BEFORE dedupe
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

# ------------------------------------------------------------
# Remote M3U sources
# ------------------------------------------------------------

SOURCE_URLS = {
    "vmttv": (
        "https://raw.githubusercontent.com/"
        "vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv"
    ),

    "vietanhtv": (
        "https://tv.vietanhtv.top/sex/"
    ),

    "dltivi": (
        "https://raw.githubusercontent.com/"
        "DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl"
    ),

    "iptv-org": (
        "https://raw.githubusercontent.com/"
        "iptv-org/iptv/refs/heads/master/streams/vn.m3u"
    ),

    "easport": (
        "https://livesport.s.gy/easport"
    ),
}


# ------------------------------------------------------------
# Source priority
# ------------------------------------------------------------

SOURCE_PRIORITY = {
    "vmttv": 500,
    "vietanhtv": 400,
    "dltivi": 300,
    "iptv-org": 200,
    "easport": 100,
}


# ============================================================
# USER AGENTS
# ============================================================

# Dalvik-style Android UA requested by user.
DALVIK_UA = (
    "Dalvik/2.1.0 (Linux; U; Android 10; K) "
)

# Some servers accept the Dalvik UA only when basic Android/browser
# headers are also present.
COMMON_HEADERS = {
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate",
}


def build_headers(source: str) -> Dict[str, str]:
    """
    Build HTTP headers for each source.

    Easport explicitly requires Dalvik.
    For consistency and compatibility, remote M3U sources also use
    an Android/Dalvik-like identity.
    """

    headers = dict(COMMON_HEADERS)

    headers["User-Agent"] = DALVIK_UA

    if source == "easport":
        headers.update(
            {
                "User-Agent": DALVIK_UA,
                "Accept": "*/*",
                "Referer": "https://livesport.s.gy/",
                "Origin": "https://livesport.s.gy",
            }
        )

    return headers


# ============================================================
# FINAL GROUPS
# ============================================================

FINAL_GROUPS = {
    "VTV": "📺 VTV",
    "HTV": "📺 HTV",
    "SCTV": "📺 SCTV",
    "THIET_YEU": "📡 Thiết yếu",
    "DIA_PHUONG": "🏠 Địa phương",
    "VTVCAB": "📺 VTVCab",
    "HTVC": "📺 HTVC",
    "THE_THAO": "🏆 Thể thao",
    "PHIM": "🎬 Phim",
    "THIEU_NHI": "👧 Thiếu nhi",
    "AM_NHAC": "🎵 Âm nhạc",
    "TIN_TUC": "📰 Tin tức",
    "QUOC_TE": "🌍 Quốc tế",
    "KHAC": "📦 Khác",
}


# ============================================================
# FILTER RULES
# ============================================================

UPDATE_GROUP_RE = re.compile(
    r"^\s*update\s+\d{1,2}:\d{2}\b.*$",
    re.IGNORECASE,
)


# Global radio/FM/VOV filter.
RADIO_RE = re.compile(
    r"""
    (
        \bradio\b
        |\bfm\b
        |\bam\b
        |\bvov\b
        |\bvov[0-9a-z]*\b
        |radio\s*\d*
        |phat\s*thanh
        |phát\s*thanh
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


# ------------------------------------------------------------
# vmttv groups
# ------------------------------------------------------------

VMTTV_EXCLUDED_GROUPS = {
    "live events",
    "radio",
    "uk radio",
    "israel",
    "hàn quốc",
    "trung quốc",
    "thái lan",
    "cola tv",
    "pháo hoa tv",
}


# ------------------------------------------------------------
# vietanhtv groups
# ------------------------------------------------------------

VIETANHTV_EXCLUDED_GROUPS = {
    "update",
    "dự phòng",
    "fpt",
    "sự kiện 360",
    "rạp phim",
    "radio",
    "socolive",
}


# ------------------------------------------------------------
# dltivi groups
# ------------------------------------------------------------

DLTIVI_EXCLUDED_GROUPS = {
    "vov",
}


# ------------------------------------------------------------
# easport groups
# ------------------------------------------------------------

EASPORT_EXCLUDED_GROUPS = {
    "info",
}


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
    """
    Normalize text for matching only.

    This does NOT change display name.
    """

    if not value:
        return ""

    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace(".", " ")
    value = value.replace("@", " ")
    value = value.replace("/", " ")
    value = value.replace("|", " ")

    value = strip_accents(value)

    value = value.lower()

    value = re.sub(r"\s+", " ", value)

    return value.strip()


def compact(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        "",
        normalize_text(value),
    )


def clean_display_name(name: str) -> str:
    """
    Clean channel display name without destroying useful metadata.
    """

    if not name:
        return ""

    name = name.strip()

    # Remove repeated spaces.
    name = re.sub(r"\s+", " ", name)

    return name


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

    # Metadata copied from canonical mapping.
    canonical_name: str = ""
    canonical_group: str = ""
    epg_id: str = ""

    # Internal scoring.
    source_score: int = 0

    @property
    def identity_text(self) -> str:
        return " ".join(
            [
                self.tvg_id,
                self.tvg_name,
                self.original_name,
            ]
        ).strip()


# ============================================================
# EXTINF PARSER
# ============================================================

ATTR_RE = re.compile(
    r'([A-Za-z0-9_-]+)="([^"]*)"'
)


def parse_extinf(line: str) -> Dict[str, str]:
    attrs = {}

    for match in ATTR_RE.finditer(line):
        attrs[match.group(1).lower()] = match.group(2)

    return attrs


def parse_display_name(line: str) -> str:
    """
    Extract text after the final comma in EXTINF.
    """

    if "," not in line:
        return ""

    return line.split(",", 1)[1].strip()


def parse_m3u(
    text: str,
    source: str,
) -> List[M3UEntry]:

    lines = text.splitlines()

    entries: List[M3UEntry] = []

    current_extinf: Optional[str] = None
    current_extra: List[str] = []

    for raw_line in lines:

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF:"):

            # Reset previous incomplete block.
            current_extinf = line
            current_extra = []

            continue

        if current_extinf is None:
            continue

        # URL line.
        if not line.startswith("#"):

            attrs = parse_extinf(current_extinf)

            duration = "-1"

            if ":" in current_extinf:
                duration_part = current_extinf[
                    len("#EXTINF:"):
                    current_extinf.find(",")
                ]

                duration = duration_part.strip()

            name = parse_display_name(current_extinf)

            entry = M3UEntry(
                source=source,
                extinf=current_extinf,
                url=line,
                extra_lines=list(current_extra),
                duration=duration,
                original_name=name,
                tvg_id=attrs.get("tvg-id", ""),
                tvg_name=attrs.get("tvg-name", ""),
                tvg_logo=attrs.get("tvg-logo", ""),
                group_title=attrs.get("group-title", ""),
                source_score=SOURCE_PRIORITY.get(
                    source,
                    0,
                ),
            )

            entries.append(entry)

            current_extinf = None
            current_extra = []

            continue

        # Preserve #KODIPROP / #EXTVLCOPT /
        # #EXTHTTP / #EXT-X-* etc.
        current_extra.append(line)

    return entries


# ============================================================
# CANONICAL MAPPING
# ============================================================

class CanonicalResolver:

    def __init__(self, mapping_path: Path):

        self.mapping_path = mapping_path

        self.channels: Dict[str, dict] = {}

        self.alias_exact: Dict[str, str] = {}
        self.alias_compact: Dict[str, str] = {}

        self.ambiguous_exact = set()
        self.ambiguous_compact = set()

        self.load()

    # --------------------------------------------------------
    # Load YAML
    # --------------------------------------------------------

    def load(self) -> None:

        if not self.mapping_path.exists():

            raise FileNotFoundError(
                f"Canonical mapping not found: "
                f"{self.mapping_path}"
            )

        with self.mapping_path.open(
            "r",
            encoding="utf-8",
        ) as f:

            data = yaml.safe_load(f) or {}

        if isinstance(data, dict) and "channels" in data:
            data = data["channels"]

        if not isinstance(data, dict):
            raise ValueError(
                "canonical_channels.yml must contain "
                "a mapping/dictionary."
            )

        for canonical_id, raw in data.items():

            if not isinstance(raw, dict):
                raw = {}

            canonical_id = str(canonical_id).strip()

            self.channels[canonical_id] = raw

            aliases = raw.get("aliases", [])

            if isinstance(aliases, str):
                aliases = [aliases]

            aliases = list(aliases)

            # Canonical ID itself is always an alias.
            aliases.append(canonical_id)

            # Optional canonical name.
            canonical_name = raw.get("name")

            if canonical_name:
                aliases.append(str(canonical_name))

            for alias in aliases:

                self._register_alias(
                    str(alias),
                    canonical_id,
                )

    # --------------------------------------------------------
    # Alias registration
    # --------------------------------------------------------

    def _register_alias(
        self,
        alias: str,
        canonical_id: str,
    ):

        exact = normalize_text(alias)
        short = compact(alias)

        if not exact:
            return

        old = self.alias_exact.get(exact)

        if old is not None and old != canonical_id:

            self.ambiguous_exact.add(exact)

        else:

            if exact not in self.ambiguous_exact:
                self.alias_exact[exact] = canonical_id

        if short:

            old = self.alias_compact.get(short)

            if old is not None and old != canonical_id:

                self.ambiguous_compact.add(short)

            else:

                if short not in self.ambiguous_compact:
                    self.alias_compact[short] = canonical_id

    # --------------------------------------------------------
    # Resolve
    # --------------------------------------------------------

    def resolve(
        self,
        entry: M3UEntry,
    ) -> Tuple[Optional[str], int]:

        candidates = [
            entry.tvg_id,
            entry.tvg_name,
            entry.original_name,
        ]

        # ----------------------------------------------------
        # 1. Exact normalized alias
        # ----------------------------------------------------

        for value in candidates:

            key = normalize_text(value)

            if not key:
                continue

            if key in self.ambiguous_exact:
                continue

            canonical_id = self.alias_exact.get(key)

            if canonical_id:

                return canonical_id, 100

        # ----------------------------------------------------
        # 2. Compact alias
        # ----------------------------------------------------

        for value in candidates:

            key = compact(value)

            if not key:
                continue

            if key in self.ambiguous_compact:
                continue

            canonical_id = self.alias_compact.get(key)

            if canonical_id:

                return canonical_id, 90

        # ----------------------------------------------------
        # 3. Controlled family matching
        #
        # This is intentionally conservative.
        # Do not blindly convert arbitrary names.
        # ----------------------------------------------------

        family = self._detect_known_family(
            candidates
        )

        if family:

            canonical_id = family

            if canonical_id in self.channels:
                return canonical_id, 70

        return None, 0

    # --------------------------------------------------------
    # Controlled VTV / HTV / SCTV patterns
    # --------------------------------------------------------

    def _detect_known_family(
        self,
        values: Iterable[str],
    ) -> Optional[str]:

        for value in values:

            key = compact(value)

            if not key:
                continue

            # VTV1 / VTV2 / VTV3 / VTV4...
            m = re.fullmatch(
                r"vtv(\d+)(?:hd)?",
                key,
            )

            if m:
                candidate = f"vtv{m.group(1)}"

                if candidate in self.channels:
                    return candidate

            # HTV7 / HTV9...
            m = re.fullmatch(
                r"htv(\d+)(?:hd)?",
                key,
            )

            if m:
                candidate = f"htv{m.group(1)}"

                if candidate in self.channels:
                    return candidate

            # SCTV1 / SCTV2...
            m = re.fullmatch(
                r"sctv(\d+)(?:hd)?",
                key,
            )

            if m:
                candidate = f"sctv{m.group(1)}"

                if candidate in self.channels:
                    return candidate

        return None

    # --------------------------------------------------------
    # Mapping data
    # --------------------------------------------------------

    def get(self, canonical_id: str) -> dict:

        return self.channels.get(
            canonical_id,
            {},
        )


# ============================================================
# FILTERING
# ============================================================

def normalized_group(group: str) -> str:
    return normalize_text(group)


VMTTV_EXCLUDED = {
    normalized_group(x)
    for x in VMTTV_EXCLUDED_GROUPS
}

VIETANHTV_EXCLUDED = {
    normalized_group(x)
    for x in VIETANHTV_EXCLUDED_GROUPS
}

DLTIVI_EXCLUDED = {
    normalized_group(x)
    for x in DLTIVI_EXCLUDED_GROUPS
}

EASPORT_EXCLUDED = {
    normalized_group(x)
    for x in EASPORT_EXCLUDED_GROUPS
}


def is_update_group(group: str) -> bool:

    return bool(
        UPDATE_GROUP_RE.match(group or "")
    )


def is_global_radio(entry: M3UEntry) -> bool:

    text = " ".join(
        [
            entry.group_title,
            entry.tvg_id,
            entry.tvg_name,
            entry.original_name,
        ]
    )

    return bool(
        RADIO_RE.search(text)
    )


def group_is_excluded(
    entry: M3UEntry,
) -> bool:

    group = normalized_group(
        entry.group_title
    )

    if is_update_group(group):
        return True

    if is_global_radio(entry):
        return True

    if entry.source == "vmttv":
        if group in VMTTV_EXCLUDED:
            return True

    elif entry.source == "vietanhtv":
        if group in VIETANHTV_EXCLUDED:
            return True

    elif entry.source == "dltivi":
        if group in DLTIVI_EXCLUDED:
            return True

    elif entry.source == "easport":
        if group in EASPORT_EXCLUDED:
            return True

    return False


def is_vsbet(entry: M3UEntry) -> bool:

    text = " ".join(
        [
            entry.tvg_id,
            entry.tvg_name,
            entry.original_name,
        ]
    )

    return compact("vsbet") in compact(text)


def should_remove(entry: M3UEntry) -> bool:

    if group_is_excluded(entry):
        return True

    if is_vsbet(entry):
        return True

    return False


# ============================================================
# GROUP CLASSIFIER
# ============================================================

def classify_group(
    entry: M3UEntry,
    mapping: dict,
) -> str:

    # Mapping has highest priority.
    mapped_group = mapping.get("group")

    if mapped_group:
        return str(mapped_group)

    text = normalize_text(
        " ".join(
            [
                entry.group_title,
                entry.tvg_name,
                entry.original_name,
            ]
        )
    )

    compact_text = compact(text)

    # --------------------------------------------------------
    # VTV
    # --------------------------------------------------------

    if (
        re.search(r"\bvtv\s*\d+\b", text)
        or compact_text.startswith("vtv")
    ):
        return FINAL_GROUPS["VTV"]

    # --------------------------------------------------------
    # HTV
    # --------------------------------------------------------

    if (
        re.search(r"\bhtv\s*\d+\b", text)
        or compact_text.startswith("htv")
    ):
        return FINAL_GROUPS["HTV"]

    # --------------------------------------------------------
    # SCTV
    # --------------------------------------------------------

    if (
        re.search(r"\bsctv\s*\d+\b", text)
        or compact_text.startswith("sctv")
    ):
        return FINAL_GROUPS["SCTV"]

    # --------------------------------------------------------
    # Essential
    # --------------------------------------------------------

    if any(
        x in text
        for x in [
            "qpvn",
            "quoc phong",
            "antv",
            "an ninh",
        ]
    ):
        return FINAL_GROUPS["THIET_YEU"]

    # --------------------------------------------------------
    # Local
    # --------------------------------------------------------

    local_keywords = [
        "hanoi tv",
        "ha noi tv",
        "hanoitv",
        "thanh pho",
        "tp hcm",
        "hai phong",
        "da nang",
        "can tho",
        "quang ninh",
        "hai duong",
        "bac ninh",
        "nam dinh",
        "thai nguyen",
        "nghe an",
        "ha tinh",
        "quang binh",
        "quang tri",
        "thua thien hue",
        "binh dinh",
        "khanh hoa",
        "dak lak",
        "lam dong",
        "dong nai",
        "binh duong",
        "ba ria",
        "vung tau",
        "tay ninh",
        "long an",
        "tien giang",
        "ben tre",
        "vinh long",
        "dong thap",
        "an giang",
        "kien giang",
        "ca mau",
        "soc trang",
        "bac lieu",
        "tra vinh",
    ]

    if any(x in text for x in local_keywords):
        return FINAL_GROUPS["DIA_PHUONG"]

    # --------------------------------------------------------
    # VTVCab
    # --------------------------------------------------------

    if (
        "vtvcab" in compact_text
        or "vtv cab" in text
    ):
        return FINAL_GROUPS["VTVCAB"]

    # --------------------------------------------------------
    # HTVC
    # --------------------------------------------------------

    if "htvc" in compact_text:
        return FINAL_GROUPS["HTVC"]

    # --------------------------------------------------------
    # Sports
    #
    # All variants become one final group:
    #   Thể thao
    #   Thể thao quốc tế
    #   Portugal Sports
    #   UK Sports
    #   Spain Sports
    #   ...
    # --------------------------------------------------------

    sports_keywords = [
        "the thao",
        "sport",
        "sports",
        "football",
        "soccer",
        "basketball",
        "tennis",
        "volleyball",
        "boxing",
        "wrestling",
        "golf",
        "racing",
        "motogp",
        "formula 1",
        "f1",
        "ufc",
        "nba",
        "nfl",
        "nhl",
        "mlb",
    ]

    if any(x in text for x in sports_keywords):
        return FINAL_GROUPS["THE_THAO"]

    # --------------------------------------------------------
    # Movies
    #
    # TVB / In The Box are treated as content signals,
    # not automatically preserved as source groups.
    # --------------------------------------------------------

    movie_keywords = [
        "phim",
        "movie",
        "movies",
        "cinema",
        "film",
        "tvb",
        "in the box",
        "inthebox",
    ]

    if any(x in text for x in movie_keywords):
        return FINAL_GROUPS["PHIM"]

    # --------------------------------------------------------
    # Kids
    # --------------------------------------------------------

    kids_keywords = [
        "thieu nhi",
        "kids",
        "kid",
        "children",
        "cartoon",
        "animation",
        "baby",
    ]

    if any(x in text for x in kids_keywords):
        return FINAL_GROUPS["THIEU_NHI"]

    # --------------------------------------------------------
    # Music
    # --------------------------------------------------------

    music_keywords = [
        "am nhac",
        "music",
        "mtv",
        "karaoke",
    ]

    if any(x in text for x in music_keywords):
        return FINAL_GROUPS["AM_NHAC"]

    # --------------------------------------------------------
    # News
    # --------------------------------------------------------

    news_keywords = [
        "tin tuc",
        "news",
        "newsasia",
        "bbc news",
        "cnn",
        "al jazeera",
        "bloomberg",
    ]

    if any(x in text for x in news_keywords):
        return FINAL_GROUPS["TIN_TUC"]

    # --------------------------------------------------------
    # International
    # --------------------------------------------------------

    international_keywords = [
        "quoc te",
        "international",
        "world",
        "korea",
        "japan",
        "china",
        "thai",
        "uk",
        "usa",
        "france",
        "germany",
        "italy",
        "spain",
        "portugal",
    ]

    if any(x in text for x in international_keywords):
        return FINAL_GROUPS["QUOC_TE"]

    return FINAL_GROUPS["KHAC"]


# ============================================================
# CANONICAL ENRICHMENT
# ============================================================

def apply_canonical(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> None:

    canonical_id, score = resolver.resolve(entry)

    if canonical_id:

        entry.canonical_id = canonical_id
        entry.canonical_score = score

        mapping = resolver.get(
            canonical_id
        )

        entry.canonical_name = str(
            mapping.get("name", "")
        ).strip()

        entry.canonical_group = str(
            mapping.get("group", "")
        ).strip()

        entry.epg_id = str(
            mapping.get("epg_id", "")
        ).strip()

        return

    # --------------------------------------------------------
    # Unknown channel
    #
    # We still need deterministic identity so that duplicates
    # within sources can be removed.
    # --------------------------------------------------------

    identity = compact(
        entry.tvg_id
        or entry.tvg_name
        or entry.original_name
    )

    if not identity:
        identity = compact(entry.url)

    digest = hashlib.sha1(
        identity.encode("utf-8")
    ).hexdigest()[:12]

    entry.canonical_id = f"local-{digest}"
    entry.canonical_score = 10


# ============================================================
# URL NORMALIZATION
# ============================================================

def normalize_url(url: str) -> str:

    url = url.strip()

    # Remove accidental whitespace.
    url = re.sub(r"\s+", "", url)

    return url


def url_key(url: str) -> str:

    return normalize_url(url).lower()


# ============================================================
# ENTRY SCORING
# ============================================================

def metadata_bonus(entry: M3UEntry) -> int:

    score = 0

    if entry.tvg_logo:
        score += 5

    if entry.tvg_id:
        score += 5

    if entry.tvg_name:
        score += 5

    if entry.group_title:
        score += 2

    if entry.extra_lines:
        score += 3

    return score


def winner_score(entry: M3UEntry) -> int:

    """
    Score used to select ONE stream per canonical channel.

    Canonical identity confidence comes first.

    Then source priority.

    Then metadata completeness.
    """

    return (
        entry.canonical_score * 1000
        + entry.source_score
        + metadata_bonus(entry)
    )


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate(
    entries: List[M3UEntry],
) -> Tuple[List[M3UEntry], Dict[str, List[M3UEntry]]]:

    grouped: Dict[
        str,
        List[M3UEntry]
    ] = defaultdict(list)

    for entry in entries:

        grouped[
            entry.canonical_id
        ].append(entry)

    winners: List[M3UEntry] = []

    for canonical_id, candidates in grouped.items():

        # ----------------------------------------------------
        # First eliminate identical URLs.
        # ----------------------------------------------------

        unique_by_url = {}

        for entry in candidates:

            key = url_key(entry.url)

            if not key:
                continue

            old = unique_by_url.get(key)

            if old is None:
                unique_by_url[key] = entry
                continue

            if winner_score(entry) > winner_score(old):
                unique_by_url[key] = entry

        candidates = list(
            unique_by_url.values()
        )

        if not candidates:
            continue

        # ----------------------------------------------------
        # Select ONE URL for ONE canonical channel.
        # ----------------------------------------------------

        candidates.sort(
            key=winner_score,
            reverse=True,
        )

        winners.append(
            candidates[0]
        )

    # ----------------------------------------------------------
    # Second pass: resolve URL collisions ACROSS DIFFERENT canonical
    # channels.
    #
    # This is NOT the same bug as duplicate URLs within one canonical
    # group (already handled above). This happens when the RAW SOURCE
    # itself lists two differently-named channels (different tvg-id /
    # different display name -> different canonical_id) pointing at the
    # EXACT SAME stream URL - e.g. a copy-paste mistake upstream, or a
    # shared relay/placeholder endpoint reused for multiple channel
    # labels. Each canonical group picks it as its own "best" candidate
    # independently, so the literal URL ends up duplicated across two
    # different channels in the final list.
    #
    # We do NOT want this (a data-quality quirk, not a dedup algorithm
    # failure) to crash the entire build via validate_output(). Instead:
    # keep the highest-scoring claim, drop the channel(s) whose ONLY
    # candidate stream duplicates another channel's URL, and log it so
    # it can be reviewed / fixed at the source mapping level later.
    # ----------------------------------------------------------

    best_by_url: Dict[str, M3UEntry] = {}

    for entry in winners:

        key = url_key(entry.url)

        if not key:
            continue

        current = best_by_url.get(key)

        if current is None or winner_score(entry) > winner_score(current):
            best_by_url[key] = entry

    resolved_winners = [
        entry
        for entry in winners
        if best_by_url.get(url_key(entry.url)) is entry
    ]

    dropped = len(winners) - len(resolved_winners)

    if dropped:

        print(
            f"[DEDUPE] Cross-channel URL collision: dropped "
            f"{dropped} channel(s) whose only stream duplicated "
            f"another channel's URL (kept the higher-scoring one)."
        )

    return resolved_winners, grouped


# ============================================================
# EXTINF ATTRIBUTES
# ============================================================

def replace_extinf_attr(
    line: str,
    attr: str,
    value: str,
) -> str:

    pattern = re.compile(
        rf'({re.escape(attr)}=")[^"]*(")',
        re.IGNORECASE,
    )

    if pattern.search(line):

        return pattern.sub(
            lambda m: (
                m.group(1)
                + value
                + m.group(2)
            ),
            line,
            count=1,
        )

    return line


def remove_extinf_attr(
    line: str,
    attr: str,
) -> str:

    pattern = re.compile(
        rf'\s+{re.escape(attr)}="[^"]*"',
        re.IGNORECASE,
    )

    return pattern.sub(
        "",
        line,
        count=1,
    )


# ============================================================
# APPLY OUTPUT METADATA
# ============================================================

def prepare_output_entry(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> None:

    mapping = resolver.get(
        entry.canonical_id
    )

    # --------------------------------------------------------
    # Name
    # --------------------------------------------------------

    display_name = (
        entry.canonical_name
        or entry.tvg_name
        or entry.original_name
        or entry.tvg_id
        or entry.canonical_id
    )

    display_name = clean_display_name(
        display_name
    )

    # --------------------------------------------------------
    # Group
    # --------------------------------------------------------

    final_group = classify_group(
        entry,
        mapping,
    )

    entry.canonical_group = final_group

    # --------------------------------------------------------
    # EPG
    # --------------------------------------------------------

    epg_id = (
        entry.epg_id
        or mapping.get("epg_id")
        or entry.tvg_id
    )

    # --------------------------------------------------------
    # Rebuild EXTINF
    # --------------------------------------------------------

    line = entry.extinf

    line = replace_extinf_attr(
        line,
        "tvg-id",
        str(epg_id or ""),
    )

    line = replace_extinf_attr(
        line,
        "tvg-name",
        display_name,
    )

    line = replace_extinf_attr(
        line,
        "group-title",
        final_group,
    )

    # --------------------------------------------------------
    # Logo priority:
    #
    # 1. Raw source logo
    # 2. Canonical mapping logo
    # --------------------------------------------------------

    logo = (
        entry.tvg_logo
        or str(mapping.get("logo", "")).strip()
    )

    if logo:

        line = replace_extinf_attr(
            line,
            "tvg-logo",
            logo,
        )

    # --------------------------------------------------------
    # Replace display name after comma.
    # --------------------------------------------------------

    if "," in line:

        prefix = line.split(",", 1)[0]

        line = (
            prefix
            + ","
            + display_name
        )

    entry.extinf = line


# ============================================================
# FETCH
# ============================================================

class FetchError(RuntimeError):
    pass


def fetch_source(
    session: requests.Session,
    source: str,
    url: str,
    retries: int = 3,
    timeout: Tuple[int, int] = (15, 45),
) -> str:

    headers = build_headers(source)

    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):

        try:

            print(
                f"[FETCH] {source}: {url}"
            )

            response = session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            )

            response.raise_for_status()

            content = response.content

            if not content:
                raise FetchError(
                    f"{source}: empty response"
                )

            # UTF-8 first.
            try:
                text = content.decode(
                    "utf-8-sig"
                )
            except UnicodeDecodeError:
                text = content.decode(
                    "utf-8",
                    errors="replace",
                )

            if "#EXTINF" not in text.upper():

                # Some servers return an HTML error page.
                preview = text[:300].replace(
                    "\n",
                    " ",
                )

                raise FetchError(
                    f"{source}: response does not "
                    f"look like M3U: {preview}"
                )

            print(
                f"[OK] {source}: "
                f"{len(text):,} bytes"
            )

            return text

        except Exception as exc:

            last_error = exc

            print(
                f"[WARN] {source} attempt "
                f"{attempt}/{retries} failed: "
                f"{exc}",
                file=sys.stderr,
            )

            if attempt < retries:

                time.sleep(
                    min(2 ** attempt, 8)
                )

    raise FetchError(
        f"Unable to fetch {source}: "
        f"{last_error}"
    )


# ============================================================
# EPG HEADER
# ============================================================

def build_header() -> str:

    # Keep a stable EPG source.
    #
    # Individual tvg-id values are preserved from canonical
    # mapping / raw source.
    #
    # This can be changed later without touching the pipeline.

    return (
        '#EXTM3U '
        'url-tvg="https://lichphatsong.io.vn/epg.xml"'
    )


# ============================================================
# RENDER
# ============================================================

def render_m3u(
    entries: List[M3UEntry],
) -> str:

    lines = [
        build_header()
    ]

    for entry in entries:

        lines.append(
            entry.extinf
        )

        # Preserve all playback directives:
        #
        # #KODIPROP
        # #EXTVLCOPT
        # #EXTHTTP
        # #EXT-X-*
        # etc.
        #
        for extra in entry.extra_lines:

            lines.append(extra)

        lines.append(
            entry.url
        )

    return "\n".join(lines) + "\n"


# ============================================================
# SORT
# ============================================================

GROUP_ORDER = {
    FINAL_GROUPS["VTV"]: 10,
    FINAL_GROUPS["HTV"]: 20,
    FINAL_GROUPS["SCTV"]: 30,
    FINAL_GROUPS["THIET_YEU"]: 40,
    FINAL_GROUPS["DIA_PHUONG"]: 50,
    FINAL_GROUPS["VTVCAB"]: 60,
    FINAL_GROUPS["HTVC"]: 70,
    FINAL_GROUPS["THE_THAO"]: 80,
    FINAL_GROUPS["PHIM"]: 90,
    FINAL_GROUPS["THIEU_NHI"]: 100,
    FINAL_GROUPS["AM_NHAC"]: 110,
    FINAL_GROUPS["TIN_TUC"]: 120,
    FINAL_GROUPS["QUOC_TE"]: 130,
    FINAL_GROUPS["KHAC"]: 900,
}


def natural_key(value: str):

    return [
        int(part)
        if part.isdigit()
        else part.lower()
        for part in re.split(
            r"(\d+)",
            value,
        )
    ]


def sort_entries(
    entries: List[M3UEntry],
) -> List[M3UEntry]:

    return sorted(
        entries,
        key=lambda e: (
            GROUP_ORDER.get(
                e.canonical_group,
                999,
            ),
            natural_key(
                e.canonical_name
                or e.tvg_name
                or e.original_name
            ),
            e.canonical_id,
        ),
    )


# ============================================================
# VALIDATION
# ============================================================

def validate_output(
    entries: List[M3UEntry],
) -> None:

    if not entries:
        raise RuntimeError(
            "Optimizer produced ZERO channels. "
            "Refusing to overwrite output."
        )

    ids = [
        e.canonical_id
        for e in entries
    ]

    duplicates = [
        item
        for item, count in Counter(ids).items()
        if count > 1
    ]

    if duplicates:

        raise RuntimeError(
            "Canonical deduplication failed. "
            f"Duplicate IDs: {duplicates[:20]}"
        )

    urls = [
        url_key(e.url)
        for e in entries
    ]

    duplicate_urls = [
        item
        for item, count in Counter(urls).items()
        if item and count > 1
    ]

    if duplicate_urls:

        raise RuntimeError(
            "Duplicate stream URLs detected: "
            f"{duplicate_urls[:10]}"
        )


# ============================================================
# STATISTICS
# ============================================================

def print_statistics(
    raw_counts: Dict[str, int],
    after_filter: Dict[str, int],
    entries: List[M3UEntry],
    grouped: Dict[str, List[M3UEntry]],
) -> None:

    print()
    print("=" * 60)
    print("OPTIMIZE M3U STATISTICS")
    print("=" * 60)

    print()
    print("Remote source entries:")

    for source in SOURCE_URLS:

        print(
            f"  {source:12s}: "
            f"{raw_counts.get(source, 0):5d}"
        )

    print()
    print("After filtering:")

    for source in SOURCE_URLS:

        print(
            f"  {source:12s}: "
            f"{after_filter.get(source, 0):5d}"
        )

    print()
    print(
        f"Canonical groups: {len(grouped):,}"
    )

    print(
        f"Final channels:   {len(entries):,}"
    )

    print()
    print("Final groups:")

    group_counts = Counter(
        e.canonical_group
        for e in entries
    )

    for group, count in sorted(
        group_counts.items(),
        key=lambda x: (
            GROUP_ORDER.get(x[0], 999),
            x[0],
        ),
    ):

        print(
            f"  {group:20s}: {count:4d}"
        )

    print("=" * 60)
    print()


# ============================================================
# MAIN PIPELINE
# ============================================================

def optimize(
    mapping_path: Path,
    output_path: Path,
) -> None:

    print("=" * 60)
    print("IPTV M3U OPTIMIZER")
    print("=" * 60)

    print()
    print("Sources are REMOTE URLs.")
    print("No m3u/raw/*.m3u files are used.")
    print()

    resolver = CanonicalResolver(
        mapping_path
    )

    print(
        f"[OK] Loaded canonical mapping: "
        f"{mapping_path}"
    )

    print(
        f"[OK] Canonical channels: "
        f"{len(resolver.channels):,}"
    )

    # --------------------------------------------------------
    # HTTP session
    # --------------------------------------------------------

    session = requests.Session()

    # Connection pool / HTTP behavior.
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=0,
    )

    session.mount(
        "http://",
        adapter,
    )

    session.mount(
        "https://",
        adapter,
    )

    # --------------------------------------------------------
    # Fetch + parse
    # --------------------------------------------------------

    all_entries: List[M3UEntry] = []

    raw_counts: Dict[str, int] = {}
    after_filter: Dict[str, int] = {}

    for source, url in SOURCE_URLS.items():

        try:

            text = fetch_source(
                session,
                source,
                url,
            )

            entries = parse_m3u(
                text,
                source,
            )

            raw_counts[source] = len(
                entries
            )

            print(
                f"[PARSE] {source}: "
                f"{len(entries):,} entries"
            )

            kept = []

            for entry in entries:

                if should_remove(entry):
                    continue

                kept.append(entry)

            after_filter[source] = len(
                kept
            )

            print(
                f"[FILTER] {source}: "
                f"{len(entries):,} -> "
                f"{len(kept):,}"
            )

            all_entries.extend(
                kept
            )

        except Exception as exc:

            # A failed source should not silently produce an
            # empty playlist. We log it and continue so other
            # sources can still be processed.
            #
            # Final validation protects against bad output.

            print(
                f"[ERROR] {source}: {exc}",
                file=sys.stderr,
            )

            raw_counts[source] = 0
            after_filter[source] = 0

    # --------------------------------------------------------
    # Safety: require at least one source
    # --------------------------------------------------------

    if not all_entries:

        raise RuntimeError(
            "All remote sources failed or were empty."
        )

    print()
    print(
        f"[TOTAL] Entries after filtering: "
        f"{len(all_entries):,}"
    )

    # --------------------------------------------------------
    # Canonicalize BEFORE deduplication
    # --------------------------------------------------------

    print()
    print(
        "[CANONICAL] Resolving channel identities..."
    )

    for entry in all_entries:

        apply_canonical(
            entry,
            resolver,
        )

    # --------------------------------------------------------
    # Diagnostic: common canonical collisions
    # --------------------------------------------------------

    canonical_sources = defaultdict(set)

    for entry in all_entries:

        canonical_sources[
            entry.canonical_id
        ].add(entry.source)

    collisions = {
        cid: sources
        for cid, sources
        in canonical_sources.items()
        if len(sources) > 1
    }

    print(
        f"[CANONICAL] "
        f"{len(collisions):,} channels "
        f"have multiple source candidates."
    )

    # --------------------------------------------------------
    # Deduplicate
    # --------------------------------------------------------

    print()
    print(
        "[DEDUPE] Selecting ONE stream "
        "per canonical channel..."
    )

    final_entries, grouped = deduplicate(
        all_entries
    )

    # --------------------------------------------------------
    # Prepare output metadata
    # --------------------------------------------------------

    for entry in final_entries:

        prepare_output_entry(
            entry,
            resolver,
        )

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    final_entries = sort_entries(
        final_entries
    )

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    validate_output(
        final_entries
    )

    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    print_statistics(
        raw_counts,
        after_filter,
        final_entries,
        grouped,
    )

    # --------------------------------------------------------
    # Render
    # --------------------------------------------------------

    output_text = render_m3u(
        final_entries
    )

    # --------------------------------------------------------
    # Atomic write
    # --------------------------------------------------------

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp_path = output_path.with_suffix(
        output_path.suffix + ".tmp"
    )

    tmp_path.write_text(
        output_text,
        encoding="utf-8",
    )

    tmp_path.replace(
        output_path
    )

    print(
        f"[OK] Output written: "
        f"{output_path}"
    )

    print(
        f"[OK] Final unique channels: "
        f"{len(final_entries):,}"
    )


# ============================================================
# CLI
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Fetch remote IPTV M3U sources, "
            "canonicalize, deduplicate and "
            "generate final playlist."
        )
    )

    parser.add_argument(
        "--mapping",
        default=str(DEFAULT_MAPPING),
        help=(
            "Canonical mapping YAML. "
            f"Default: {DEFAULT_MAPPING}"
        ),
    )

    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=(
            "Output M3U path. "
            f"Default: {DEFAULT_OUTPUT}"
        ),
    )

    return parser.parse_args()


def main() -> int:

    args = parse_args()

    try:

        optimize(
            mapping_path=Path(
                args.mapping
            ),
            output_path=Path(
                args.output
            ),
        )

        return 0

    except KeyboardInterrupt:

        print(
            "\n[ABORTED] Interrupted.",
            file=sys.stderr,
        )

        return 130

    except Exception as exc:

        print(
            f"\n[FATAL] {exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
