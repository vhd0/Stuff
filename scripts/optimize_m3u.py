#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV/M3U Optimizer V3.2

Pipeline:
    Fetch sources
      -> parse M3U
      -> source exclusions
      -> canonicalize with iptv-org
      -> normalize channel title / tvg-id
      -> determine OTT group
      -> deduplicate
      -> select best stream by source priority
      -> natural sort
      -> add final group emoji
      -> write M3U

IMPORTANT:
- No stream health checking.
- Preserve playback metadata.
- One stream per canonical channel.
- iptv-org is used as canonical identity where possible.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import yaml


# ============================================================
# CONFIG
# ============================================================

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / ".cache" / "iptv"
OUTPUT = ROOT / "m3u" / "listtivi.m3u"
ALIASES_FILE = ROOT / "m3u" / "channel_aliases.yml"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


SOURCES = {
    "vmttv": {
        "url": "https://raw.githubusercontent.com/vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv",
        "priority": 10,
    },
    "vietanhtv": {
        "url": "https://tv.vietanhtv.top/sex/",
        "priority": 20,
    },
    "dltivi": {
        "url": "https://raw.githubusercontent.com/DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl",
        "priority": 30,
    },
    "iptv-org": {
        "url": "https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/vn.m3u",
        "priority": 40,
    },
    "easport": {
        "url": "https://livesport.s.gy/easport",
        "priority": 50,
    },
}


IPTV_ORG_CHANNELS = "https://iptv-org.github.io/api/channels.json"
IPTV_ORG_LOGOS = "https://iptv-org.github.io/api/logos.json"


# Dalvik-like Android UA
USER_AGENT = (
    "Dalvik/2.1.0 (Linux; U; Android 13; SM-G998B Build/TP1A.220624.014)"
)

HTTP_TIMEOUT = (10, 30)
MAX_RETRIES = 3


# ============================================================
# FINAL GROUP TAXONOMY
# ============================================================

GROUP_ORDER = [
    "VTV",
    "HTV",
    "SCTV",
    "Thiết yếu",
    "Địa phương",
    "VTVCab",
    "HTVC",
    "Thể thao",
    "Phim",
    "Thiếu nhi",
    "Âm nhạc",
    "Tin tức",
    "Quốc tế",
    "Khác",
]


GROUP_EMOJI = {
    "VTV": "📺 VTV",
    "HTV": "📺 HTV",
    "SCTV": "📺 SCTV",
    "Thiết yếu": "📡 Thiết yếu",
    "Địa phương": "🏠 Địa phương",
    "VTVCab": "📺 VTVCab",
    "HTVC": "📺 HTVC",
    "Thể thao": "🏆 Thể thao",
    "Phim": "🎬 Phim",
    "Thiếu nhi": "👧 Thiếu nhi",
    "Âm nhạc": "🎵 Âm nhạc",
    "Tin tức": "📰 Tin tức",
    "Quốc tế": "🌍 Quốc tế",
    "Khác": "📦 Khác",
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
    if not value:
        return ""

    value = unicodedata.normalize("NFKC", value)
    value = value.replace("&amp;", "&")

    # Remove emoji and decorative symbols.
    value = re.sub(
        r"[\U00010000-\U0010ffff]",
        " ",
        value,
    )

    value = strip_accents(value)
    value = value.lower()

    # Normalize separators.
    value = re.sub(r"[_|/\\]+", " ", value)
    value = re.sub(r"[-–—]+", " ", value)
    value = re.sub(r"[()[\]{}:;,]+", " ", value)

    # Remove common quality markers.
    value = re.sub(
        r"\b(?:hd|fhd|uhd|4k|sd|1080p|720p|480p)\b",
        " ",
        value,
    )

    value = re.sub(r"\s+", " ", value).strip()

    return value


def compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_text(value))


def normalize_url(url: str) -> str:
    return url.strip()


def safe_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value)


# ============================================================
# SOURCE EXCLUSIONS
# ============================================================

EXCLUDED_GROUP_WORDS = {
    "live events",
    "radio",
    "uk radio",
    "israel",
    "han quoc",
    "trung quoc",
    "thai lan",
    "cola tv",
    "phao hoa tv",
    "update",
    "du phong",
    "fpt",
    "su kien 360",
    "rap phim",
    "socolive",
    "info",
}


EXCLUDED_TITLE_WORDS = {
    "radio",
    "fm",
    "am radio",
    "vov",
    "voice of vietnam",
}


def is_update_group(group: str) -> bool:
    """
    Catch:
        UPDATE
        UPDATE 08:30
        UPDATE HH:MM...
        UPDATE 12:00 - ...
        update hh:mm...
    """
    g = normalize_text(group)

    return bool(
        re.match(
            r"^update(?:\s+hh\s*mm)?(?:\s+\d{1,2}\s*[:.]\s*\d{2})?.*$",
            g,
            re.I,
        )
    )


def is_radio_title(title: str) -> bool:
    t = normalize_text(title)

    if not t:
        return False

    if re.search(r"\bvov(?:\d+)?\b", t):
        return True

    if "voice of vietnam" in t:
        return True

    if re.search(r"\bfm\s*\d{2,3}(?:\.\d+)?\b", t):
        return True

    if re.search(r"\b\d{2,3}(?:\.\d+)?\s*fm\b", t):
        return True

    if re.search(r"\bradio\b", t):
        return True

    return False


def is_excluded(source: str, group: str, title: str) -> bool:
    g = normalize_text(group)
    t = normalize_text(title)

    # Universal radio exclusion.
    if is_radio_title(title):
        return True

    # UPDATE groups.
    if is_update_group(group):
        return True

    # VOV group / channel.
    if "vov" in compact(group):
        return True

    if source == "dltivi" and "vov" in compact(group):
        return True

    # Source-specific exclusions.
    for word in EXCLUDED_GROUP_WORDS:
        if word in g:
            if word == "update":
                return True
            if word == "radio":
                return True
            if word in {
                "live events",
                "israel",
                "han quoc",
                "trung quoc",
                "thai lan",
                "cola tv",
                "phao hoa tv",
                "du phong",
                "fpt",
                "su kien 360",
                "rap phim",
                "socolive",
                "info",
            }:
                if source in {"vmttv", "vietanhtv", "easport"}:
                    return True

    # iptv-org VSBet.
    if source == "iptv-org":
        if "vsbet" in compact(title):
            return True

    return False


# ============================================================
# M3U MODEL
# ============================================================

@dataclass
class Channel:
    source: str
    title: str
    tvg_id: str
    tvg_name: str
    group: str
    logo: str
    url: str
    extinf: str

    metadata: List[str] = field(default_factory=list)

    canonical_id: str = ""
    canonical_title: str = ""
    canonical_group: str = ""

    match_score: int = 0

    @property
    def priority(self) -> int:
        return SOURCES[self.source]["priority"]


# ============================================================
# HTTP
# ============================================================

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
})


def fetch(url: str) -> str:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(
                url,
                timeout=HTTP_TIMEOUT,
            )
            response.raise_for_status()

            return response.text

        except Exception as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                time.sleep(attempt * 2)

    raise RuntimeError(
        f"Cannot fetch {url}: {last_error}"
    )


def fetch_json_cached(
    url: str,
    filename: str,
    max_age: int = 86400,
):
    path = CACHE_DIR / filename

    if path.exists():
        age = time.time() - path.stat().st_mtime

        if age < max_age:
            try:
                return json.loads(path.read_text(
                    encoding="utf-8"
                ))
            except Exception:
                pass

    data = fetch(url)

    path.write_text(
        data,
        encoding="utf-8",
    )

    return json.loads(data)


# ============================================================
# M3U PARSER
# ============================================================

ATTR_RE = re.compile(
    r'([\w-]+)="([^"]*)"'
)


def parse_extinf(line: str) -> Dict[str, str]:
    attrs = {
        key.lower(): value
        for key, value in ATTR_RE.findall(line)
    }

    # Extract display title after comma.
    title = ""

    if "," in line:
        title = line.split(",", 1)[1].strip()

    attrs["title"] = title

    return attrs


def parse_m3u(text: str, source: str) -> List[Channel]:
    lines = text.splitlines()

    channels: List[Channel] = []

    current_extinf = None
    current_attrs = None
    current_metadata: List[str] = []

    for raw in lines:
        line = raw.strip()

        if not line:
            continue

        if line.startswith("#EXTINF"):
            current_extinf = line
            current_attrs = parse_extinf(line)
            current_metadata = []
            continue

        if current_extinf is None:
            continue

        if line.startswith("#"):
            if (
                line.startswith("#KODIPROP:")
                or line.startswith("#EXTVLCOPT:")
                or line.startswith("#EXTHTTP:")
                or line.startswith("#EXT-X-")
            ):
                current_metadata.append(line)

            continue

        # URL
        url = normalize_url(line)

        if not url:
            current_extinf = None
            current_attrs = None
            current_metadata = []
            continue

        attrs = current_attrs or {}

        title = attrs.get("title", "").strip()

        channel = Channel(
            source=source,
            title=title,
            tvg_id=attrs.get("tvg-id", "").strip(),
            tvg_name=attrs.get("tvg-name", "").strip(),
            group=attrs.get("group-title", "").strip(),
            logo=attrs.get("tvg-logo", "").strip(),
            url=url,
            extinf=current_extinf,
            metadata=list(current_metadata),
        )

        if not is_excluded(
            source,
            channel.group,
            channel.title,
        ):
            channels.append(channel)

        current_extinf = None
        current_attrs = None
        current_metadata = []

    return channels


# ============================================================
# IPTV-ORG INDEX
# ============================================================

@dataclass
class OrgChannel:
    id: str
    name: str
    alt_names: List[str]
    network: str
    categories: List[str]
    country: str


class IPTVOrgIndex:

    def __init__(self):
        self.by_id: Dict[str, OrgChannel] = {}
        self.by_name: Dict[str, OrgChannel] = {}
        self.by_compact_name: Dict[str, OrgChannel] = {}

    def add(self, channel: OrgChannel):
        self.by_id[channel.id.lower()] = channel

        names = [
            channel.name,
            *channel.alt_names,
        ]

        for name in names:
            if not name:
                continue

            n = normalize_text(name)
            c = compact(name)

            if n:
                self.by_name[n] = channel

            if c:
                self.by_compact_name[c] = channel

    def find(
        self,
        tvg_id: str,
        title: str,
        tvg_name: str,
    ) -> Optional[OrgChannel]:

        # 1. Exact tvg-id is strongest.
        if tvg_id:
            channel = self.by_id.get(
                tvg_id.lower()
            )

            if channel:
                return channel

        # 2. Exact normalized name.
        for value in (title, tvg_name):
            if not value:
                continue

            channel = self.by_name.get(
                normalize_text(value)
            )

            if channel:
                return channel

        # 3. Compact name.
        for value in (title, tvg_name):
            if not value:
                continue

            channel = self.by_compact_name.get(
                compact(value)
            )

            if channel:
                return channel

        return None


def load_iptv_org() -> Tuple[IPTVOrgIndex, Dict[str, str]]:
    raw_channels = fetch_json_cached(
        IPTV_ORG_CHANNELS,
        "channels.json",
    )

    raw_logos = fetch_json_cached(
        IPTV_ORG_LOGOS,
        "logos.json",
    )

    index = IPTVOrgIndex()

    for item in raw_channels:
        channel_id = str(item.get("id", "")).strip()

        if not channel_id:
            continue

        country = str(
            item.get("country", "")
        ).strip()

        # VN only for canonical matching.
        if country and country.lower() != "vn":
            continue

        alt_names = item.get(
            "alt_names",
            [],
        )

        if not isinstance(alt_names, list):
            alt_names = []

        channel = OrgChannel(
            id=channel_id,
            name=str(
                item.get("name", "")
            ).strip(),

            alt_names=[
                str(x).strip()
                for x in alt_names
                if x
            ],

            network=str(
                item.get("network", "")
            ).strip(),

            categories=[
                str(x).strip().lower()
                for x in item.get(
                    "categories",
                    [],
                )
                if x
            ],

            country=country,
        )

        index.add(channel)

    logos: Dict[str, str] = {}

    if isinstance(raw_logos, list):
        for item in raw_logos:
            channel_id = str(
                item.get("channel", "")
            ).strip()

            url = str(
                item.get("url", "")
            ).strip()

            if channel_id and url:
                logos[channel_id] = url

    return index, logos


# ============================================================
# ALIAS MAP
# ============================================================

def load_aliases() -> dict:
    if not ALIASES_FILE.exists():
        return {}

    try:
        data = yaml.safe_load(
            ALIASES_FILE.read_text(
                encoding="utf-8"
            )
        )

        return data or {}

    except Exception as exc:
        print(
            f"WARNING: cannot load aliases: {exc}",
            file=sys.stderr,
        )

        return {}


def build_alias_index(config: dict):
    aliases = {}

    for canonical_id, item in (
        config.get("channels", {})
    ).items():

        if not isinstance(item, dict):
            continue

        names = [
            canonical_id,
            item.get("name", ""),
            *item.get("aliases", []),
        ]

        for name in names:
            if not name:
                continue

            aliases[normalize_text(name)] = canonical_id
            aliases[compact(name)] = canonical_id

    return aliases


# ============================================================
# CANONICALIZATION
# ============================================================

def canonicalize(
    channel: Channel,
    org_index: IPTVOrgIndex,
    aliases: Dict[str, str],
    logo_map: Dict[str, str],
):
    org = org_index.find(
        channel.tvg_id,
        channel.title,
        channel.tvg_name,
    )

    if org:
        channel.canonical_id = org.id
        channel.canonical_title = org.name

        # Force canonical title immediately.
        channel.title = org.name

        if not channel.tvg_name:
            channel.tvg_name = org.name

        if not channel.logo:
            channel.logo = logo_map.get(
                org.id,
                "",
            )

        channel.match_score = 100

        channel.canonical_group = classify_group(
            channel,
            org,
        )

        return

    # Alias fallback.
    candidates = [
        channel.tvg_id,
        channel.tvg_name,
        channel.title,
    ]

    canonical_id = ""

    for value in candidates:
        if not value:
            continue

        canonical_id = (
            aliases.get(
                normalize_text(value)
            )
            or aliases.get(
                compact(value)
            )
            or ""
        )

        if canonical_id:
            break

    if canonical_id:
        channel.canonical_id = canonical_id
        channel.match_score = 80

        config_name = canonical_id

        channel.canonical_title = (
            config_name
        )

        channel.title = channel.canonical_title

        channel.canonical_group = classify_group(
            channel,
            None,
        )

        return

    # Local fallback.
    fallback = compact(
        channel.tvg_id
        or channel.tvg_name
        or channel.title
    )

    channel.canonical_id = (
        f"local:{fallback}"
    )

    channel.canonical_title = (
        channel.title.strip()
    )

    channel.canonical_group = classify_group(
        channel,
        None,
    )

    channel.match_score = 10


# ============================================================
# GROUP CLASSIFICATION
# ============================================================

def has_any(text: str, words: Iterable[str]) -> bool:
    t = normalize_text(text)
    return any(
        normalize_text(word) in t
        for word in words
    )


def classify_group(
    channel: Channel,
    org: Optional[OrgChannel],
) -> str:

    title = normalize_text(
        channel.canonical_title
        or channel.title
    )

    tvg_id = normalize_text(
        channel.canonical_id
        or channel.tvg_id
    )

    network = ""

    categories: List[str] = []

    if org:
        network = normalize_text(
            org.network
        )

        categories = [
            normalize_text(x)
            for x in org.categories
        ]

    # --------------------------------------------------------
    # NETWORK FIRST
    # --------------------------------------------------------

    # VTV family
    if (
        "vtv" in network
        or re.search(r"\bvtv\s*\d+", title)
        or tvg_id.startswith("vtv")
    ):
        # VTVCab must not become VTV.
        if "vtvcab" in title or "vtvcab" in network:
            return "VTVCab"

        return "VTV"

    # HTV family
    if (
        "htv" in network
        or re.search(r"\bhtv\s*\d+", title)
        or tvg_id.startswith("htv")
    ):
        # HTVC is separate.
        if "htvc" in title or "htvc" in network:
            return "HTVC"

        return "HTV"

    # SCTV
    if (
        "sctv" in network
        or re.search(r"\bsctv\s*\d+", title)
        or tvg_id.startswith("sctv")
    ):
        return "SCTV"

    # VTVCab
    if (
        "vtvcab" in network
        or "vtvcab" in title
        or "vtvcab" in tvg_id
    ):
        return "VTVCab"

    # HTVC
    if (
        "htvc" in network
        or "htvc" in title
        or "htvc" in tvg_id
    ):
        return "HTVC"

    # --------------------------------------------------------
    # ESSENTIAL CHANNELS
    # --------------------------------------------------------

    if has_any(
        title,
        [
            "qpvn",
            "quoc phong viet nam",
            "antv",
            "an ninh tv",
        ],
    ):
        return "Thiết yếu"

    if has_any(
        network,
        [
            "antv",
            "qpvn",
        ],
    ):
        return "Thiết yếu"

    # --------------------------------------------------------
    # LOCAL
    # HanoiTV is intentionally local.
    # --------------------------------------------------------

    if (
        "hanoitv" in compact(title)
        or "hanoi tv" in title
        or "hanoitv" in compact(network)
    ):
        return "Địa phương"

    if (
        "local" in categories
        or "regional" in categories
    ):
        return "Địa phương"

    # Common Vietnamese provincial TV patterns.
    if has_any(
        title,
        [
            "an giang",
            "bac ninh",
            "bac lieu",
            "ben tre",
            "binh duong",
            "binh phuoc",
            "binh thuan",
            "ca mau",
            "can tho",
            "cao bang",
            "da nang",
            "dak lak",
            "dak nong",
            "dien bien",
            "dong nai",
            "dong thap",
            "gia lai",
            "ha giang",
            "ha nam",
            "ha noi",
            "ha tinh",
            "hai duong",
            "hai phong",
            "hau giang",
            "hoa binh",
            "hung yen",
            "khanh hoa",
            "kien giang",
            "kon tum",
            "lai chau",
            "lang son",
            "lao cai",
            "lam dong",
            "long an",
            "nam dinh",
            "nghe an",
            "ninh binh",
            "ninh thuan",
            "phu tho",
            "phu yen",
            "quang binh",
            "quang nam",
            "quang ngai",
            "quang ninh",
            "quang tri",
            "soc trang",
            "son la",
            "tay ninh",
            "thai binh",
            "thai nguyen",
            "thanh hoa",
            "thua thien hue",
            "tien giang",
            "tra vinh",
            "tuyen quang",
            "vinh long",
            "vinh phuc",
            "yen bai",
        ],
    ):
        return "Địa phương"

    # --------------------------------------------------------
    # SPORTS
    # ALL sports variants become one group.
    # --------------------------------------------------------

    if (
        "sports" in categories
        or "sport" in categories
        or has_any(
            title,
            [
                "sports",
                "sport",
                "football",
                "soccer",
                "tennis",
                "basketball",
                "golf",
                "motorsport",
                "f1",
                "formula 1",
            ],
        )
        or has_any(
            channel.group,
            [
                "the thao",
                "the thao quoc te",
                "international sports",
                "portugal sports",
                "uk sports",
                "us sports",
                "sports",
            ],
        )
    ):
        return "Thể thao"

    # --------------------------------------------------------
    # MOVIES
    # --------------------------------------------------------

    if (
        "movies" in categories
        or "movie" in categories
        or "entertainment" in categories
        and has_any(
            title,
            [
                "movie",
                "movies",
                "cinema",
                "film",
                "phim",
            ],
        )
        or has_any(
            channel.group,
            [
                "phim",
                "movie",
                "movies",
                "cinema",
                "film",
                "rap phim",
            ],
        )
    ):
        return "Phim"

    # --------------------------------------------------------
    # KIDS
    # --------------------------------------------------------

    if (
        "kids" in categories
        or "children" in categories
        or has_any(
            title,
            [
                "kids",
                "children",
                "kids",
                "thieu nhi",
            ],
        )
    ):
        return "Thiếu nhi"

    # --------------------------------------------------------
    # MUSIC
    # --------------------------------------------------------

    if (
        "music" in categories
        or has_any(
            title,
            [
                "music",
                "music tv",
                "mtv",
                "am nhac",
            ],
        )
    ):
        return "Âm nhạc"

    # --------------------------------------------------------
    # NEWS
    # --------------------------------------------------------

    if (
        "news" in categories
        or has_any(
            title,
            [
                "news",
                "news tv",
                "tin tuc",
            ],
        )
    ):
        return "Tin tức"

    # --------------------------------------------------------
    # INTERNATIONAL
    # --------------------------------------------------------

    if org:
        if org.country and org.country.lower() != "vn":
            return "Quốc tế"

    return "Khác"


# ============================================================
# DEDUPLICATION
# ============================================================

def identity_keys(channel: Channel) -> List[str]:
    keys = []

    if channel.canonical_id:
        keys.append(
            f"id:{channel.canonical_id.lower()}"
        )

    if channel.tvg_id:
        keys.append(
            f"tvg:{compact(channel.tvg_id)}"
        )

    if channel.tvg_name:
        keys.append(
            f"name:{compact(channel.tvg_name)}"
        )

    keys.append(
        f"title:{compact(channel.title)}"
    )

    return list(dict.fromkeys(keys))


def channel_score(channel: Channel) -> Tuple:
    """
    Higher is better.

    Canonical match dominates.
    Source priority comes next.
    A logo and clean metadata are minor bonuses.
    """

    metadata_bonus = min(
        len(channel.metadata),
        5,
    )

    logo_bonus = 2 if channel.logo else 0

    return (
        channel.match_score,
        -channel.priority,
        logo_bonus,
        metadata_bonus,
        len(channel.url),
    )


def deduplicate(
    channels: List[Channel],
) -> List[Channel]:

    selected: Dict[str, Channel] = {}

    for channel in channels:

        # Primary canonical ID.
        key = (
            channel.canonical_id.lower()
            if channel.canonical_id
            else f"local:{compact(channel.title)}"
        )

        existing = selected.get(key)

        if existing is None:
            selected[key] = channel
            continue

        if channel_score(channel) > channel_score(existing):
            selected[key] = channel

    return list(selected.values())


# ============================================================
# NATURAL SORT
# ============================================================

def natural_tokens(value: str):
    value = normalize_text(value)

    parts = re.split(
        r"(\d+)",
        value,
    )

    result = []

    for part in parts:
        if part.isdigit():
            result.append(
                (0, int(part))
            )
        else:
            result.append(
                (1, part)
            )

    return result


def channel_sort_key(
    channel: Channel,
):

    title = (
        channel.canonical_title
        or channel.title
    )

    compact_title = compact(title)

    # --------------------------------------------------------
    # Explicit network ordering.
    # --------------------------------------------------------

    network_rank = 50

    group = channel.canonical_group

    if group == "VTV":
        network_rank = 10

    elif group == "HTV":
        network_rank = 20

    elif group == "SCTV":
        network_rank = 30

    elif group == "Thiết yếu":
        network_rank = 40

    elif group == "Địa phương":
        network_rank = 50

    elif group == "VTVCab":
        network_rank = 60

    elif group == "HTVC":
        network_rank = 70

    elif group == "Thể thao":
        network_rank = 80

    elif group == "Phim":
        network_rank = 90

    elif group == "Thiếu nhi":
        network_rank = 100

    elif group == "Âm nhạc":
        network_rank = 110

    elif group == "Tin tức":
        network_rank = 120

    elif group == "Quốc tế":
        network_rank = 130

    else:
        network_rank = 999

    # VTV1, VTV2, ... VTV10 natural.
    prefix_rank = 9999

    number = 9999

    m = re.search(
        r"\b(?:vtv|htv|sctv)\s*(\d+)\b",
        title,
        re.I,
    )

    if m:
        prefix = m.group(0).lower()

        prefix_rank = {
            "vtv": 1,
            "htv": 2,
            "sctv": 3,
        }.get(
            re.sub(r"\d+", "", prefix).strip(),
            9,
        )

        number = int(m.group(1))

    return (
        network_rank,
        prefix_rank,
        number,
        natural_tokens(title),
        compact_title,
    )


# ============================================================
# OUTPUT
# ============================================================

def make_extinf(channel: Channel) -> str:
    attrs = []

    if channel.tvg_id:
        attrs.append(
            f'tvg-id="{channel.tvg_id}"'
        )

    if channel.tvg_name:
        attrs.append(
            f'tvg-name="{channel.tvg_name}"'
        )

    if channel.logo:
        attrs.append(
            f'tvg-logo="{channel.logo}"'
        )

    if channel.canonical_group:
        attrs.append(
            f'group-title="{GROUP_EMOJI[channel.canonical_group]}"'
        )

    return (
        "#EXTINF:-1 "
        + " ".join(attrs)
        + ","
        + channel.canonical_title
    )


def collect_epg_headers(texts: List[str]) -> List[str]:
    urls = []

    for text in texts:
        for line in text.splitlines():
            line = line.strip()

            if not line.startswith("#EXTM3U"):
                continue

            match = re.search(
                r'url-tvg="([^"]+)"',
                line,
                re.I,
            )

            if match:
                url = match.group(1).strip()

                if url and url not in urls:
                    urls.append(url)

    return urls


def write_output(
    channels: List[Channel],
    epg_urls: List[str],
):
    lines = []

    if epg_urls:
        lines.append(
            '#EXTM3U url-tvg="'
            + ",".join(epg_urls)
            + '"'
        )
    else:
        lines.append("#EXTM3U")

    for channel in channels:

        lines.append(
            make_extinf(channel)
        )

        lines.extend(
            channel.metadata
        )

        lines.append(
            channel.url
        )

    temp = OUTPUT.with_suffix(
        ".m3u.tmp"
    )

    temp.write_text(
        "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )

    # Atomic replacement.
    temp.replace(OUTPUT)


# ============================================================
# VALIDATION
# ============================================================

def validate(
    channels: List[Channel],
):

    if len(channels) < 100:
        raise RuntimeError(
            f"Generated only {len(channels)} channels. "
            "Refusing to overwrite fallback playlist."
        )

    seen = set()

    for channel in channels:
        key = channel.canonical_id.lower()

        if key in seen:
            raise RuntimeError(
                f"Duplicate canonical channel: {key}"
            )

        seen.add(key)

        if not channel.url:
            raise RuntimeError(
                f"Empty stream URL: {channel.title}"
            )

    # Ensure forbidden content is absent.
    for channel in channels:
        haystack = normalize_text(
            f"{channel.title} "
            f"{channel.group}"
        )

        if is_radio_title(channel.title):
            raise RuntimeError(
                f"Radio channel survived filtering: "
                f"{channel.title}"
            )

        if "vov" in haystack:
            raise RuntimeError(
                f"VOV channel survived filtering: "
                f"{channel.title}"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=== IPTV Optimizer V3.2 ===")

    print("Loading iptv-org API...")

    org_index, logo_map = load_iptv_org()

    print(
        f"iptv-org channels: "
        f"{len(org_index.by_id)}"
    )

    config = load_aliases()

    aliases = build_alias_index(
        config
    )

    all_channels: List[Channel] = []
    raw_texts: List[str] = []

    # --------------------------------------------------------
    # FETCH + PARSE
    # --------------------------------------------------------

    for source, info in SOURCES.items():

        print(
            f"[{source}] fetching..."
        )

        try:
            text = fetch(
                info["url"]
            )

            raw_texts.append(text)

            channels = parse_m3u(
                text,
                source,
            )

            print(
                f"[{source}] parsed: "
                f"{len(channels)}"
            )

            all_channels.extend(
                channels
            )

        except Exception as exc:
            print(
                f"[{source}] FAILED: {exc}",
                file=sys.stderr,
            )

    if not all_channels:
        raise RuntimeError(
            "No channels were parsed."
        )

    print(
        f"Total after source filtering: "
        f"{len(all_channels)}"
    )

    # --------------------------------------------------------
    # CANONICALIZE
    # --------------------------------------------------------

    for channel in all_channels:
        canonicalize(
            channel,
            org_index,
            aliases,
            logo_map,
        )

    canonical_count = sum(
        1
        for c in all_channels
        if c.match_score >= 100
    )

    print(
        f"Canonicalized by iptv-org: "
        f"{canonical_count}/{len(all_channels)}"
    )

    # --------------------------------------------------------
    # DEDUPLICATE
    # --------------------------------------------------------

    before = len(all_channels)

    all_channels = deduplicate(
        all_channels
    )

    print(
        f"Deduplicated: "
        f"{before} -> {len(all_channels)}"
    )

    # --------------------------------------------------------
    # FINAL GROUP
    # --------------------------------------------------------

    for channel in all_channels:
        if channel.canonical_group not in GROUP_ORDER:
            channel.canonical_group = "Khác"

    # --------------------------------------------------------
    # SORT
    # --------------------------------------------------------

    all_channels.sort(
        key=channel_sort_key
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    validate(
        all_channels
    )

    # --------------------------------------------------------
    # EPG
    # --------------------------------------------------------

    epg_urls = collect_epg_headers(
        raw_texts
    )

    print(
        f"EPG sources: {len(epg_urls)}"
    )

    # --------------------------------------------------------
    # WRITE
    # --------------------------------------------------------

    write_output(
        all_channels,
        epg_urls,
    )

    print(
        f"Output: {OUTPUT}"
    )

    print(
        f"Final channels: "
        f"{len(all_channels)}"
    )

    # Group statistics.
    stats: Dict[str, int] = {}

    for channel in all_channels:
        stats[channel.canonical_group] = (
            stats.get(
                channel.canonical_group,
                0,
            )
            + 1
        )

    print("\nGroups:")

    for group in GROUP_ORDER:
        count = stats.get(
            group,
            0,
        )

        if count:
            print(
                f"  {GROUP_EMOJI[group]}: "
                f"{count}"
            )


if __name__ == "__main__":
    main()
