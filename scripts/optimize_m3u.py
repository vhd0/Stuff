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

IMPORTANT:
    canonicalize BEFORE dedupe.

Priority groups are HARD LOCKED:
    VTV
    HTV
    SCTV
    VTVCab
    HTVC
    Thiết yếu
    Địa phương

If a channel belongs to one of these groups, content classification
MUST NOT move it to another group.

Examples:
    ON Football      -> 📡 VTVCab
    ON Sports+       -> 📡 VTVCab
    ON Sports News   -> 📡 VTVCab
    ON Movies        -> 📡 VTVCab
    ON Kids          -> 📡 VTVCab
    ON Music         -> 📡 VTVCab

Event group:
    TV360 Sự kiện    -> 🎟️ Sự kiện
    FPT Sự kiện      -> 🎟️ Sự kiện
    VTVPrime         -> 🎟️ Sự kiện

Filtering:
    vmttv LIVE EVENTS -> removed
    vmttv COLA TV SV2 -> removed
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
    # Priority groups
    "VTV": "📺 VTV",
    "HTV": "📺 HTV",
    "SCTV": "📺 SCTV",
    "VTVCAB": "📡 VTVCab",
    "HTVC": "📡 HTVC",
    "THIET_YEU": "⭐ Thiết yếu",
    "DIA_PHUONG": "🏙️ Địa phương",

    # Non-priority content groups
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


# Exact/normalized group exclusions.
#
# IMPORTANT:
# "cola tv sv2" is intentionally separate from "cola tv".
# LIVE EVENTS is also handled by content-level filtering below.
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


# These are blocked specifically because vmttv may occasionally
# expose a bad/mislabelled group/name.
VMTTV_BLOCKED_TOKENS = {
    "liveevents",
    "colatvsv2",
}


# ============================================================
# TEXT NORMALIZATION
# ============================================================

def strip_accents(value: str) -> str:
    value = unicodedata.normalize("NFD", value)

    return "".join(
        c
        for c in value
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(value: str) -> str:
    """
    Matching normalization only.

    Does NOT collapse regional VTV channels:
        VTV5 Tây Nam Bộ != VTV5
        VTV5 Tây Nguyên != VTV5
    """

    if not value:
        return ""

    value = str(value).strip()

    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace(".", " ")
    value = value.replace("@", " ")
    value = value.replace("/", " ")
    value = value.replace("|", " ")

    value = strip_accents(value)
    value = value.lower()

    value = re.sub(r"\s+", " ", value)

    # Remove quality suffixes only as standalone tokens.
    value = re.sub(
        r"\b(uhd|fhd|fullhd|hd|sd|4k|1080p|720p)\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    return re.sub(r"\s+", " ", value).strip()


def compact(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        "",
        normalize_text(value),
    )


def clean_display_name(name: str) -> str:
    if not name:
        return ""

    return re.sub(
        r"\s+",
        " ",
        name.strip(),
    )


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
# EXTINF
# ============================================================

ATTR_RE = re.compile(
    r'([A-Za-z0-9_-]+)="([^"]*)"'
)


def parse_extinf(line: str) -> Dict[str, str]:
    return {
        m.group(1).lower(): m.group(2)
        for m in ATTR_RE.finditer(line)
    }


def parse_display_name(line: str) -> str:
    if "," not in line:
        return ""

    return line.split(",", 1)[1].strip()


def parse_m3u(
    text: str,
    source: str,
) -> List[M3UEntry]:

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

            attrs = parse_extinf(
                current_extinf
            )

            comma_pos = current_extinf.find(",")

            duration = "-1"

            if comma_pos >= 0:

                duration_part = current_extinf[
                    len("#EXTINF:"):comma_pos
                ]

                duration = duration_part.strip()

            entries.append(
                M3UEntry(
                    source=source,
                    extinf=current_extinf,
                    url=line,
                    extra_lines=list(current_extra),
                    duration=duration,
                    original_name=parse_display_name(
                        current_extinf
                    ),
                    tvg_id=attrs.get(
                        "tvg-id",
                        "",
                    ),
                    tvg_name=attrs.get(
                        "tvg-name",
                        "",
                    ),
                    tvg_logo=attrs.get(
                        "tvg-logo",
                        "",
                    ),
                    group_title=attrs.get(
                        "group-title",
                        "",
                    ),
                    source_score=SOURCE_PRIORITY.get(
                        source,
                        0,
                    ),
                )
            )

            current_extinf = None
            current_extra = []

            continue

        # Preserve playback directives.
        current_extra.append(line)

    return entries


# ============================================================
# CANONICAL RESOLVER
# ============================================================

class CanonicalResolver:

    def __init__(
        self,
        mapping_path: Path,
    ):
        self.mapping_path = mapping_path

        self.channels: Dict[str, dict] = {}

        self.alias_exact: Dict[str, str] = {}
        self.alias_compact: Dict[str, str] = {}

        self.ambiguous_exact = set()
        self.ambiguous_compact = set()

        self.vtvcab_number_index: Dict[
            int,
            str,
        ] = {}

        self.load()

    # --------------------------------------------------------
    # LOAD YAML
    # --------------------------------------------------------

    def load(self) -> None:

        if not self.mapping_path.exists():
            raise FileNotFoundError(
                "Canonical mapping not found: "
                f"{self.mapping_path}"
            )

        with self.mapping_path.open(
            "r",
            encoding="utf-8",
        ) as f:

            data = yaml.safe_load(f) or {}

        if "channels" in data:
            data = data["channels"]

        if not isinstance(data, dict):
            raise ValueError(
                "canonical_channels.yml must contain "
                "a 'channels' mapping."
            )

        for canonical_id, raw in data.items():

            if not isinstance(raw, dict):
                raw = {}

            canonical_id = str(
                canonical_id
            ).strip()

            self.channels[
                canonical_id
            ] = raw

            aliases = raw.get(
                "aliases",
                [],
            )

            if isinstance(
                aliases,
                str,
            ):
                aliases = [
                    aliases
                ]

            aliases = list(aliases)

            aliases.append(
                canonical_id
            )

            if raw.get("name"):
                aliases.append(
                    str(raw["name"])
                )

            # Optional provider names.
            provider_names = raw.get(
                "provider_names",
                [],
            )

            if isinstance(
                provider_names,
                str,
            ):
                provider_names = [
                    provider_names
                ]

            aliases.extend(
                str(x)
                for x in provider_names
            )

            for alias in aliases:

                self._register_alias(
                    str(alias),
                    canonical_id,
                )

            number = raw.get(
                "vtvcab_number"
            )

            if number is not None:

                try:

                    self.vtvcab_number_index[
                        int(number)
                    ] = canonical_id

                except (
                    TypeError,
                    ValueError,
                ):
                    pass

    # --------------------------------------------------------
    # REGISTER ALIAS
    # --------------------------------------------------------

    def _register_alias(
        self,
        alias: str,
        canonical_id: str,
    ) -> None:

        exact = normalize_text(alias)
        short = compact(alias)

        if exact:

            old = self.alias_exact.get(
                exact
            )

            if (
                old is not None
                and old != canonical_id
            ):

                self.ambiguous_exact.add(
                    exact
                )

            elif exact not in self.ambiguous_exact:

                self.alias_exact[
                    exact
                ] = canonical_id

        if short:

            old = self.alias_compact.get(
                short
            )

            if (
                old is not None
                and old != canonical_id
            ):

                self.ambiguous_compact.add(
                    short
                )

            elif short not in self.ambiguous_compact:

                self.alias_compact[
                    short
                ] = canonical_id

    # --------------------------------------------------------
    # VTVCAB NUMBER
    # --------------------------------------------------------

    @staticmethod
    def detect_vtvcab_number(
        value: str,
    ) -> Optional[int]:

        if not value:
            return None

        normalized = normalize_text(
            value
        )

        compact_value = compact(
            value
        )

        patterns = (
            r"\bvtv\s*cab\s*(\d+)\b",
            r"\bvtvcab\s*(\d+)\b",
        )

        for pattern in patterns:

            match = re.search(
                pattern,
                normalized,
                flags=re.IGNORECASE,
            )

            if match:
                return int(
                    match.group(1)
                )

        match = re.fullmatch(
            r"vtvcab(\d+)",
            compact_value,
        )

        if match:
            return int(
                match.group(1)
            )

        return None

    # --------------------------------------------------------
    # KNOWN FAMILY
    # --------------------------------------------------------

    def detect_known_family(
        self,
        values: Iterable[str],
    ) -> Optional[str]:

        for value in values:

            key = compact(
                value
            )

            if not key:
                continue

            # VTV1 ... VTV9 etc.
            match = re.fullmatch(
                r"vtv(\d+)(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    f"vtv{match.group(1)}"
                )

                if candidate in self.channels:
                    return candidate

            # HTV1 ...
            match = re.fullmatch(
                r"htv(\d+)(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    f"htv{match.group(1)}"
                )

                if candidate in self.channels:
                    return candidate

            # SCTV1 ...
            match = re.fullmatch(
                r"sctv(\d+)(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    f"sctv{match.group(1)}"
                )

                if candidate in self.channels:
                    return candidate

        return None

    # --------------------------------------------------------
    # RESOLVE
    # --------------------------------------------------------

    def resolve(
        self,
        entry: M3UEntry,
    ) -> Tuple[
        Optional[str],
        int,
        str,
    ]:

        candidates = (
            (
                "tvg-id",
                entry.tvg_id,
                100,
            ),
            (
                "name",
                entry.original_name,
                90,
            ),
            (
                "tvg-name",
                entry.tvg_name,
                90,
            ),
            (
                "group-title",
                entry.group_title,
                80,
            ),
        )

        # ----------------------------------------------------
        # 1. Exact normalized alias
        # ----------------------------------------------------

        for (
            reason,
            value,
            score,
        ) in candidates:

            key = normalize_text(
                value
            )

            if (
                not key
                or key in self.ambiguous_exact
            ):
                continue

            canonical_id = (
                self.alias_exact.get(key)
            )

            if canonical_id:

                return (
                    canonical_id,
                    score,
                    f"{reason}:exact",
                )

        # ----------------------------------------------------
        # 2. Compact alias
        # ----------------------------------------------------

        for (
            reason,
            value,
            score,
        ) in candidates:

            key = compact(
                value
            )

            if (
                not key
                or key in self.ambiguous_compact
            ):
                continue

            canonical_id = (
                self.alias_compact.get(key)
            )

            if canonical_id:

                return (
                    canonical_id,
                    score - 5,
                    f"{reason}:compact",
                )

        # ----------------------------------------------------
        # 3. VTVCab channel number
        # ----------------------------------------------------

        for (
            reason,
            value,
            _score,
        ) in candidates:

            number = (
                self.detect_vtvcab_number(
                    value
                )
            )

            if number is None:
                continue

            canonical_id = (
                self.vtvcab_number_index.get(
                    number
                )
            )

            if canonical_id:

                return (
                    canonical_id,
                    95,
                    (
                        f"{reason}:"
                        f"vtvcab_number:"
                        f"{number}"
                    ),
                )

        # ----------------------------------------------------
        # 4. Conservative numbered family
        # ----------------------------------------------------

        family = (
            self.detect_known_family(
                value
                for _, value, _ in candidates
            )
        )

        if family:

            return (
                family,
                70,
                "known_family",
            )

        return (
            None,
            0,
            "unknown",
        )

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def get(
        self,
        canonical_id: str,
    ) -> dict:

        return self.channels.get(
            canonical_id,
            {},
        )


# ============================================================
# FILTER HELPERS
# ============================================================

def normalized_group(
    group: str,
) -> str:

    return normalize_text(
        group
    )


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


def is_update_group(
    group: str,
) -> bool:

    return bool(
        UPDATE_GROUP_RE.match(
            group or ""
        )
    )


def is_global_radio(
    entry: M3UEntry,
) -> bool:

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


def entry_filter_text(
    entry: M3UEntry,
) -> str:

    return normalize_text(
        " ".join(
            [
                entry.group_title,
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                entry.url,
            ]
        )
    )


def is_vmttv_blocked_content(
    entry: M3UEntry,
) -> Optional[str]:

    if entry.source != "vmttv":
        return None

    group = normalized_group(
        entry.group_title
    )

    text = entry_filter_text(
        entry
    )

    compact_text = compact(
        text
    )

    # --------------------------------------------------------
    # Explicit group: LIVE EVENTS
    # --------------------------------------------------------

    if group == "live events":

        return "LIVE EVENTS"

    # Handle variations:
    # LIVE-EVENTS
    # LIVE_EVENTS
    # LIVE EVENTS HD
    if (
        "liveevents" in compact_text
    ):

        return "LIVE EVENTS"

    # --------------------------------------------------------
    # Explicit group/name: COLA TV SV2
    # --------------------------------------------------------

    if (
        group == "cola tv sv2"
    ):

        return "COLA TV SV2"

    if (
        "colatvsv2" in compact_text
    ):

        return "COLA TV SV2"

    return None


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

    vmttv_blocked = (
        is_vmttv_blocked_content(
            entry
        )
    )

    if vmttv_blocked:
        return True

    excluded = {
        "vmttv": VMTTV_EXCLUDED,
        "vietanhtv": VIETANHTV_EXCLUDED,
        "dltivi": DLTIVI_EXCLUDED,
        "easport": EASPORT_EXCLUDED,
    }.get(
        entry.source,
        set(),
    )

    return group in excluded


def is_vsbet(
    entry: M3UEntry,
) -> bool:

    text = " ".join(
        [
            entry.tvg_id,
            entry.tvg_name,
            entry.original_name,
        ]
    )

    return (
        "vsbet" in compact(text)
    )


def should_remove(
    entry: M3UEntry,
) -> bool:

    return (
        group_is_excluded(entry)
        or is_vsbet(entry)
    )


def filter_reason(
    entry: M3UEntry,
) -> str:

    blocked = (
        is_vmttv_blocked_content(
            entry
        )
    )

    if blocked:
        return blocked

    group = normalized_group(
        entry.group_title
    )

    if is_update_group(group):
        return "UPDATE"

    if is_global_radio(entry):
        return "RADIO"

    if is_vsbet(entry):
        return "VSBET"

    if (
        entry.source == "vmttv"
        and group in VMTTV_EXCLUDED
    ):
        return entry.group_title or "VMTTV_EXCLUDED"

    if (
        entry.source == "vietanhtv"
        and group in VIETANHTV_EXCLUDED
    ):
        return entry.group_title or "VIETANHTV_EXCLUDED"

    if (
        entry.source == "dltivi"
        and group in DLTIVI_EXCLUDED
    ):
        return entry.group_title or "DLTIVI_EXCLUDED"

    if (
        entry.source == "easport"
        and group in EASPORT_EXCLUDED
    ):
        return entry.group_title or "EASPORT_EXCLUDED"

    return "FILTERED"


# ============================================================
# CANONICAL APPLY
# ============================================================

def apply_canonical(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> None:

    (
        canonical_id,
        score,
        reason,
    ) = resolver.resolve(entry)

    if canonical_id:

        entry.canonical_id = (
            canonical_id
        )

        entry.canonical_score = score
        entry.canonical_reason = reason

        mapping = resolver.get(
            canonical_id
        )

        entry.canonical_name = str(
            mapping.get(
                "name",
                "",
            )
        ).strip()

        entry.canonical_group = str(
            mapping.get(
                "group",
                "",
            )
        ).strip()

        entry.epg_id = str(
            mapping.get(
                "epg_id",
                "",
            )
        ).strip()

        return

    # --------------------------------------------------------
    # Unknown channel:
    # deterministic local identity.
    #
    # NOTE:
    # We intentionally do NOT use URL as the first identity.
    # Name/tvg-id is preferred because several channels can
    # legitimately share a relay URL.
    # --------------------------------------------------------

    identity = compact(
        entry.tvg_id
        or entry.tvg_name
        or entry.original_name
    )

    if not identity:
        identity = compact(
            entry.url
        )

    digest = hashlib.sha1(
        identity.encode("utf-8")
    ).hexdigest()[:12]

    entry.canonical_id = (
        f"local-{digest}"
    )

    entry.canonical_score = 10
    entry.canonical_reason = (
        "local_identity"
    )


# ============================================================
# GROUP HELPERS
# ============================================================

def group_label(
    group_key: str,
) -> str:

    key = str(
        group_key or ""
    ).strip()

    if key in FINAL_GROUPS:
        return FINAL_GROUPS[key]

    if key in FINAL_GROUPS.values():
        return key

    return ""


def priority_group_from_mapping(
    mapping: dict,
) -> Optional[str]:

    group_key = str(
        mapping.get(
            "group",
            "",
        )
    ).strip()

    # --------------------------------------------------------
    # HARD LOCK
    # --------------------------------------------------------

    if group_key in PRIORITY_GROUPS:
        return FINAL_GROUPS[
            group_key
        ]

    return None


def mapping_provider(
    mapping: dict,
) -> str:

    return normalize_text(
        str(
            mapping.get(
                "provider",
                "",
            )
        )
    )


# ============================================================
# EVENT CLASSIFIER
# ============================================================

EVENT_PATTERNS = (
    "tv360 su kien",
    "tv360 event",
    "tv360 events",
    "fpt su kien",
    "fpt play su kien",
    "fpt event",
    "fpt events",
    "vtvprime",
    "vtv prime",
)


def is_event_channel(
    entry: M3UEntry,
    mapping: dict,
) -> bool:

    # --------------------------------------------------------
    # Priority group ALWAYS wins.
    # --------------------------------------------------------

    if priority_group_from_mapping(
        mapping
    ):
        return False

    text = normalize_text(
        " ".join(
            [
                entry.source,
                entry.group_title,
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                str(
                    mapping.get(
                        "name",
                        "",
                    )
                ),
                str(
                    mapping.get(
                        "provider",
                        "",
                    )
                ),
            ]
        )
    )

    compact_text = compact(
        text
    )

    # Explicit canonical YAML event group.
    mapped_group = str(
        mapping.get(
            "group",
            "",
        )
    ).strip()

    if mapped_group == "SU_KIEN":
        return True

    # VTVPrime.
    if (
        "vtvprime" in compact_text
        or "vtv prime" in text
    ):
        return True

    # TV360 event.
    if (
        "tv360 su kien" in text
        or "tv360 event" in text
        or "tv360 events" in text
    ):
        return True

    # FPT event.
    if (
        "fpt su kien" in text
        or "fpt play su kien" in text
        or "fpt event" in text
        or "fpt events" in text
    ):
        return True

    return False


# ============================================================
# GROUP RESOLUTION
# ============================================================

def classify_group(
    entry: M3UEntry,
    mapping: dict,
) -> str:

    # ========================================================
    # 1. PRIORITY GROUP LOCK
    #
    # THIS MUST BE THE FIRST DECISION.
    # ========================================================

    locked = (
        priority_group_from_mapping(
            mapping
        )
    )

    if locked:
        return locked

    # --------------------------------------------------------
    # Provider-level lock.
    #
    # Useful when a mapping explicitly declares provider
    # but group metadata is missing.
    # --------------------------------------------------------

    provider = mapping_provider(
        mapping
    )

    if provider == "vtvcab":
        return FINAL_GROUPS[
            "VTVCAB"
        ]

    if provider == "htvc":
        return FINAL_GROUPS[
            "HTVC"
        ]

    if provider == "sctv":
        return FINAL_GROUPS[
            "SCTV"
        ]

    if provider == "vtv":
        return FINAL_GROUPS[
            "VTV"
        ]

    if provider == "htv":
        return FINAL_GROUPS[
            "HTV"
        ]

    # ========================================================
    # 2. CONTROLLED FALLBACK RECOGNITION
    # ========================================================

    text = normalize_text(
        " ".join(
            [
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                entry.group_title,
            ]
        )
    )

    compact_text = compact(
        text
    )

    # --------------------------------------------------------
    # VTV
    #
    # Supports:
    #   VTV1
    #   VTV 1
    #   VTV1 HD
    #   VTV5 Tây Nguyên
    #   VTV5 Tây Nam Bộ
    #
    # Mapping should provide canonical IDs for known channels,
    # but fallback still protects the group.
    # --------------------------------------------------------

    if re.search(
        r"\bvtv\s*\d+\b",
        text,
    ):
        return FINAL_GROUPS[
            "VTV"
        ]

    # --------------------------------------------------------
    # HTV
    # --------------------------------------------------------

    if re.search(
        r"\bhtv\s*\d+\b",
        text,
    ):
        return FINAL_GROUPS[
            "HTV"
        ]

    # --------------------------------------------------------
    # SCTV
    #
    # FIX:
    # SCTV is a priority group and must never fall to KHAC.
    # --------------------------------------------------------

    if re.search(
        r"\bsctv\s*\d+\b",
        text,
    ):
        return FINAL_GROUPS[
            "SCTV"
        ]

    if re.search(
        r"\bsctv\b",
        text,
    ):
        return FINAL_GROUPS[
            "SCTV"
        ]

    # --------------------------------------------------------
    # HTVC
    # --------------------------------------------------------

    if (
        "htvc" in compact_text
    ):
        return FINAL_GROUPS[
            "HTVC"
        ]

    # --------------------------------------------------------
    # VTVCab / ON
    #
    # Do NOT classify ON channels by content.
    # They must remain under VTVCab.
    # --------------------------------------------------------

    if (
        "vtvcab" in compact_text
    ):
        return FINAL_GROUPS[
            "VTVCAB"
        ]

    # Known ON channel names.
    on_patterns = (
        "on football",
        "on sports+",
        "on sports plus",
        "on sports news",
        "on sports",
        "on golf",
        "on phim viet",
        "on movies",
        "on cine",
        "on vie giai tri",
        "on vie dramas",
        "on echannel",
        "on style",
        "on kids",
        "on bibi",
        "on info tv",
        "on o2tv",
        "on life",
        "on music",
        "on vfamily",
        "on trending",
        "on homeshopping",
    )

    if any(
        pattern in text
        for pattern in on_patterns
    ):
        return FINAL_GROUPS[
            "VTVCAB"
        ]

    # ========================================================
    # 3. THIẾT YẾU
    # ========================================================

    essential_patterns = (
        "qpvn",
        "quoc phong",
        "antv",
        "an ninh",
    )

    if any(
        pattern in text
        for pattern in essential_patterns
    ):
        return FINAL_GROUPS[
            "THIET_YEU"
        ]

    # ========================================================
    # 4. ĐỊA PHƯƠNG
    # ========================================================

    local_keywords = (
        "hanoi tv",
        "ha noi tv",
        "hanoitv",
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
        "hue tv",
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
        "binh thuan",
        "phu yen",
        "gia lai",
        "kon tum",
        "dak nong",
        "ninh thuan",
    )

    if any(
        keyword in text
        for keyword in local_keywords
    ):
        return FINAL_GROUPS[
            "DIA_PHUONG"
        ]

    # ========================================================
    # 5. SỰ KIỆN
    # ========================================================

    if is_event_channel(
        entry,
        mapping,
    ):
        return FINAL_GROUPS[
            "SU_KIEN"
        ]

    # ========================================================
    # 6. CONTENT CLASSIFICATION
    # ========================================================

    sports_keywords = (
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
    )

    if any(
        keyword in text
        for keyword in sports_keywords
    ):
        return FINAL_GROUPS[
            "THE_THAO"
        ]

    movie_keywords = (
        "phim",
        "movie",
        "movies",
        "cinema",
        "film",
        "tvb",
        "in the box",
        "inthebox",
    )

    if any(
        keyword in text
        for keyword in movie_keywords
    ):
        return FINAL_GROUPS[
            "PHIM"
        ]

    kids_keywords = (
        "thieu nhi",
        "kids",
        "kid",
        "children",
        "cartoon",
        "animation",
        "baby",
    )

    if any(
        keyword in text
        for keyword in kids_keywords
    ):
        return FINAL_GROUPS[
            "THIEU_NHI"
        ]

    music_keywords = (
        "am nhac",
        "music",
        "mtv",
        "karaoke",
    )

    if any(
        keyword in text
        for keyword in music_keywords
    ):
        return FINAL_GROUPS[
            "AM_NHAC"
        ]

    news_keywords = (
        "tin tuc",
        "news",
        "newsasia",
        "bbc news",
        "cnn",
        "al jazeera",
        "bloomberg",
    )

    if any(
        keyword in text
        for keyword in news_keywords
    ):
        return FINAL_GROUPS[
            "TIN_TUC"
        ]

    international_keywords = (
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
    )

    if any(
        keyword in text
        for keyword in international_keywords
    ):
        return FINAL_GROUPS[
            "QUOC_TE"
        ]

    return FINAL_GROUPS[
        "KHAC"
    ]


# ============================================================
# URL / SCORING
# ============================================================

def normalize_url(
    url: str,
) -> str:

    return re.sub(
        r"\s+",
        "",
        url.strip(),
    )


def url_key(
    url: str,
) -> str:

    return normalize_url(
        url
    ).lower()


def metadata_bonus(
    entry: M3UEntry,
) -> int:

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


def winner_score(
    entry: M3UEntry,
) -> int:

    return (
        entry.canonical_score * 1000
        + entry.source_score
        + metadata_bonus(entry)
    )


# ============================================================
# DEDUPE
# ============================================================

def deduplicate(
    entries: List[M3UEntry],
) -> Tuple[
    List[M3UEntry],
    Dict[str, List[M3UEntry]],
]:

    grouped: Dict[
        str,
        List[M3UEntry],
    ] = defaultdict(list)

    for entry in entries:

        grouped[
            entry.canonical_id
        ].append(entry)

    winners: List[M3UEntry] = []

    for candidates in grouped.values():

        # ----------------------------------------------------
        # Same canonical ID + same URL:
        # keep best metadata/source.
        # ----------------------------------------------------

        unique_by_url: Dict[
            str,
            M3UEntry,
        ] = {}

        for entry in candidates:

            key = url_key(
                entry.url
            )

            if not key:
                continue

            old = unique_by_url.get(
                key
            )

            if (
                old is None
                or winner_score(entry)
                > winner_score(old)
            ):

                unique_by_url[
                    key
                ] = entry

        candidates = list(
            unique_by_url.values()
        )

        if not candidates:
            continue

        # ----------------------------------------------------
        # ONE best stream for ONE canonical channel.
        # ----------------------------------------------------

        candidates.sort(
            key=winner_score,
            reverse=True,
        )

        winners.append(
            candidates[0]
        )

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # We deliberately DO NOT globally remove duplicate URLs.
    #
    # Two different canonical channels may legitimately point
    # to the same relay/stream.
    # --------------------------------------------------------

    return (
        winners,
        grouped,
    )


# ============================================================
# EXTINF ATTRIBUTES
# ============================================================

def upsert_extinf_attr(
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
            lambda m:
            m.group(1)
            + value
            + m.group(2),
            line,
            count=1,
        )

    comma = line.find(",")

    if comma < 0:
        return line

    prefix = line[:comma]
    suffix = line[comma:]

    return (
        prefix
        + f' {attr}="{value}"'
        + suffix
    )


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
# OUTPUT METADATA
# ============================================================

def prepare_output_entry(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> None:

    mapping = resolver.get(
        entry.canonical_id
    )

    display_name = clean_display_name(
        entry.canonical_name
        or entry.tvg_name
        or entry.original_name
        or entry.tvg_id
        or entry.canonical_id
    )

    # --------------------------------------------------------
    # HARD PRIORITY GROUP LOCK.
    # --------------------------------------------------------

    locked_group = (
        priority_group_from_mapping(
            mapping
        )
    )

    if locked_group:

        final_group = locked_group

        print(
            "[GROUP LOCK] "
            f"{display_name} "
            f"-> {final_group}"
        )

        github_notice(
            "GROUP LOCK",
            (
                f"{display_name} "
                f"-> {final_group}"
            ),
        )

    else:

        final_group = classify_group(
            entry,
            mapping,
        )

    entry.canonical_group = (
        final_group
    )

    # --------------------------------------------------------
    # EPG ID:
    # explicit mapping EPG -> source tvg-id.
    # --------------------------------------------------------

    epg_id = (
        entry.epg_id
        or mapping.get("epg_id")
        or entry.tvg_id
    )

    logo = (
        entry.tvg_logo
        or str(
            mapping.get(
                "logo",
                "",
            )
        ).strip()
    )

    line = entry.extinf

    line = upsert_extinf_attr(
        line,
        "tvg-id",
        str(
            epg_id or ""
        ),
    )

    line = upsert_extinf_attr(
        line,
        "tvg-name",
        display_name,
    )

    line = upsert_extinf_attr(
        line,
        "group-title",
        final_group,
    )

    if logo:

        line = upsert_extinf_attr(
            line,
            "tvg-logo",
            logo,
        )

    # Make display name after comma canonical.
    if "," in line:

        prefix = line.split(
            ",",
            1,
        )[0]

        line = (
            prefix
            + ","
            + display_name
        )

    entry.extinf = line


# ============================================================
# GITHUB ACTIONS LOGGING
# ============================================================

def github_notice(
    title: str,
    message: str,
) -> None:

    # GitHub Actions understands these annotations.
    print(
        "::notice title="
        + str(title).replace(
            "\n",
            " ",
        )
        + "::"
        + str(message).replace(
            "\n",
            " ",
        )
    )


def github_warning(
    title: str,
    message: str,
) -> None:

    print(
        "::warning title="
        + str(title).replace(
            "\n",
            " ",
        )
        + "::"
        + str(message).replace(
            "\n",
            " ",
        )
    )


def github_error(
    title: str,
    message: str,
) -> None:

    print(
        "::error title="
        + str(title).replace(
            "\n",
            " ",
        )
        + "::"
        + str(message).replace(
            "\n",
            " ",
        )
    )


# ============================================================
# FETCH
# ============================================================

class FetchError(RuntimeError):
    pass


def build_headers(
    source: str,
) -> Dict[str, str]:

    headers = dict(
        COMMON_HEADERS
    )

    headers["User-Agent"] = (
        DALVIK_UA
    )

    if source == "easport":

        headers.update(
            {
                "User-Agent": DALVIK_UA,
                "Accept": "*/*",
                "Referer": (
                    "https://livesport.s.gy/"
                ),
                "Origin": (
                    "https://livesport.s.gy"
                ),
            }
        )

    return headers


def fetch_source(
    session: requests.Session,
    source: str,
    url: str,
    retries: int = 3,
    timeout: Tuple[int, int] = (
        15,
        45,
    ),
) -> str:

    headers = build_headers(
        source
    )

    last_error: Optional[
        Exception
    ] = None

    for attempt in range(
        1,
        retries + 1,
    ):

        try:

            print(
                f"[FETCH] {source}: "
                f"{url}"
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
                    f"{source}: "
                    "empty response"
                )

            try:

                text = content.decode(
                    "utf-8-sig"
                )

            except UnicodeDecodeError:

                text = content.decode(
                    "utf-8",
                    errors="replace",
                )

            if (
                "#EXTINF"
                not in text.upper()
            ):

                preview = (
                    text[:300]
                    .replace(
                        "\n",
                        " ",
                    )
                )

                raise FetchError(
                    f"{source}: "
                    "response does not look "
                    f"like M3U: {preview}"
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

            github_warning(
                "Source fetch failed",
                (
                    f"{source} "
                    f"attempt {attempt}/"
                    f"{retries}: {exc}"
                ),
            )

            if attempt < retries:

                time.sleep(
                    min(
                        2 ** attempt,
                        8,
                    )
                )

    raise FetchError(
        f"Unable to fetch {source}: "
        f"{last_error}"
    )


# ============================================================
# RENDER
# ============================================================

def build_header() -> str:

    return (
        '#EXTM3U '
        'url-tvg='
        '"https://lichphatsong.io.vn/epg.xml"'
    )


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

        for extra in entry.extra_lines:
            lines.append(extra)

        lines.append(
            entry.url
        )

    return (
        "\n".join(lines)
        + "\n"
    )


# ============================================================
# SORT
# ============================================================

GROUP_ORDER = {
    # Priority groups
    FINAL_GROUPS["VTV"]: 10,
    FINAL_GROUPS["HTV"]: 20,
    FINAL_GROUPS["SCTV"]: 30,
    FINAL_GROUPS["VTVCAB"]: 40,
    FINAL_GROUPS["HTVC"]: 50,
    FINAL_GROUPS["THIET_YEU"]: 60,
    FINAL_GROUPS["DIA_PHUONG"]: 70,

    # Other groups
    FINAL_GROUPS["SU_KIEN"]: 80,
    FINAL_GROUPS["THE_THAO"]: 90,
    FINAL_GROUPS["PHIM"]: 100,
    FINAL_GROUPS["THIEU_NHI"]: 110,
    FINAL_GROUPS["AM_NHAC"]: 120,
    FINAL_GROUPS["TIN_TUC"]: 130,
    FINAL_GROUPS["QUOC_TE"]: 140,
    FINAL_GROUPS["KHAC"]: 900,
}


def natural_key(
    value: str,
):
    return [
        (
            int(part)
            if part.isdigit()
            else part.lower()
        )
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
            "Optimizer produced ZERO "
            "channels. Refusing to "
            "overwrite output."
        )

    ids = [
        e.canonical_id
        for e in entries
    ]

    duplicates = [
        item
        for item, count
        in Counter(ids).items()
        if count > 1
    ]

    if duplicates:

        github_error(
            "Duplicate canonical IDs",
            str(
                duplicates[:20]
            ),
        )

        raise RuntimeError(
            "Canonical deduplication "
            "failed. Duplicate IDs: "
            f"{duplicates[:20]}"
        )

    urls = [
        url_key(e.url)
        for e in entries
    ]

    duplicate_urls = [
        item
        for item, count
        in Counter(urls).items()
        if item and count > 1
    ]

    # Same URL across different canonical
    # channels is allowed.
    if duplicate_urls:

        print(
            "[WARN] "
            f"{len(duplicate_urls)} "
            "stream URL(s) are shared "
            "by multiple canonical "
            "channels."
        )

        github_warning(
            "Shared stream URLs",
            (
                f"{len(duplicate_urls)} "
                "URL(s) are shared by "
                "different canonical "
                "channels."
            ),
        )


# ============================================================
# DIAGNOSTICS
# ============================================================

def print_canonical_diagnostic(
    entries: List[M3UEntry],
    limit: int = 50,
) -> None:

    print()
    print(
        "[CANONICAL] "
        "Sample resolution:"
    )

    for entry in entries[:limit]:

        display = (
            entry.original_name
            or entry.tvg_name
            or entry.tvg_id
        )

        print(
            f"  {display} "
            f"-> {entry.canonical_id} "
            f"-> {entry.canonical_group} "
            f"score={entry.canonical_score} "
            f"reason={entry.canonical_reason} "
            f"source={entry.source}"
        )


def print_statistics(
    raw_counts: Dict[str, int],
    after_filter: Dict[str, int],
    entries: List[M3UEntry],
    grouped: Dict[
        str,
        List[M3UEntry],
    ],
    removed_counts: Dict[
        str,
        Counter,
    ],
) -> None:

    print()
    print("=" * 70)
    print(
        "OPTIMIZE M3U STATISTICS"
    )
    print("=" * 70)

    print()
    print(
        "Remote source entries:"
    )

    for source in SOURCE_URLS:

        print(
            f"  {source:12s}: "
            f"{raw_counts.get(source, 0):5d}"
        )

    print()
    print(
        "After filtering:"
    )

    for source in SOURCE_URLS:

        print(
            f"  {source:12s}: "
            f"{after_filter.get(source, 0):5d}"
        )

    print()
    print(
        "Removed by reason:"
    )

    total_removed = 0

    for source in SOURCE_URLS:

        counter = removed_counts.get(
            source,
            Counter(),
        )

        source_total = sum(
            counter.values()
        )

        total_removed += (
            source_total
        )

        if source_total == 0:

            print(
                f"  {source:12s}: none"
            )

            continue

        print(
            f"  {source:12s}: "
            f"{source_total:5d}"
        )

        for reason, count in counter.most_common():

            print(
                f"      - "
                f"{reason}: "
                f"{count}"
            )

    print()
    print(
        f"Total removed:   "
        f"{total_removed:,}"
    )

    print(
        f"Canonical groups: "
        f"{len(grouped):,}"
    )

    print(
        f"Final channels:   "
        f"{len(entries):,}"
    )

    print()
    print(
        "Final groups:"
    )

    group_counts = Counter(
        e.canonical_group
        for e in entries
    )

    for group, count in sorted(
        group_counts.items(),
        key=lambda x: (
            GROUP_ORDER.get(
                x[0],
                999,
            ),
            x[0],
        ),
    ):

        print(
            f"  {group:20s}: "
            f"{count:4d}"
        )

    print()
    print(
        "Priority group verification:"
    )

    for key in (
        "VTV",
        "HTV",
        "SCTV",
        "VTVCAB",
        "HTVC",
        "THIET_YEU",
        "DIA_PHUONG",
    ):

        label = FINAL_GROUPS[
            key
        ]

        count = group_counts.get(
            label,
            0,
        )

        print(
            f"  {label:20s}: "
            f"{count:4d}"
        )

    print()
    print("=" * 70)
    print()


# ============================================================
# MAIN OPTIMIZE
# ============================================================

def optimize(
    mapping_path: Path,
    output_path: Path,
) -> None:

    print("=" * 70)
    print(
        "IPTV M3U OPTIMIZER"
    )
    print("=" * 70)

    print()

    resolver = CanonicalResolver(
        mapping_path
    )

    print(
        "[OK] Loaded canonical mapping: "
        f"{mapping_path}"
    )

    print(
        "[OK] Canonical channels: "
        f"{len(resolver.channels):,}"
    )

    print()
    print(
        "[CONFIG] Priority groups:"
    )

    for key in (
        "VTV",
        "HTV",
        "SCTV",
        "VTVCAB",
        "HTVC",
        "THIET_YEU",
        "DIA_PHUONG",
    ):

        print(
            f"  {key:12s} "
            f"-> {FINAL_GROUPS[key]}"
        )

    print()
    print(
        "[CONFIG] vmttv blocked:"
    )

    print(
        "  - LIVE EVENTS"
    )

    print(
        "  - COLA TV SV2"
    )

    print()

    session = requests.Session()

    adapter = (
        requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0,
        )
    )

    session.mount(
        "http://",
        adapter,
    )

    session.mount(
        "https://",
        adapter,
    )

    all_entries: List[
        M3UEntry
    ] = []

    raw_counts: Dict[
        str,
        int,
    ] = {}

    after_filter: Dict[
        str,
        int,
    ] = {}

    removed_counts: Dict[
        str,
        Counter,
    ] = defaultdict(
        Counter
    )

    # ========================================================
    # FETCH + PARSE + FILTER
    # ========================================================

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

            raw_counts[
                source
            ] = len(entries)

            print(
                f"[PARSE] {source}: "
                f"{len(entries):,} entries"
            )

            kept: List[
                M3UEntry
            ] = []

            for entry in entries:

                if should_remove(
                    entry
                ):

                    reason = (
                        filter_reason(
                            entry
                        )
                    )

                    removed_counts[
                        source
                    ][reason] += 1

                    display = (
                        entry.original_name
                        or entry.tvg_name
                        or entry.tvg_id
                        or "(unnamed)"
                    )

                    print(
                        "[FILTER] "
                        f"{source} | "
                        f"{reason} | "
                        f"{display}"
                    )

                    github_notice(
                        "M3U entry filtered",
                        (
                            f"{source} | "
                            f"{reason} | "
                            f"{display}"
                        ),
                    )

                    continue

                kept.append(
                    entry
                )

            after_filter[
                source
            ] = len(kept)

            print(
                f"[FILTER] {source}: "
                f"{len(entries):,} -> "
                f"{len(kept):,}"
            )

            all_entries.extend(
                kept
            )

        except Exception as exc:

            print(
                f"[ERROR] {source}: "
                f"{exc}",
                file=sys.stderr,
            )

            github_error(
                "Source failed",
                f"{source}: {exc}",
            )

            raw_counts[
                source
            ] = 0

            after_filter[
                source
            ] = 0

    if not all_entries:

        raise RuntimeError(
            "All remote sources failed "
            "or were empty."
        )

    print()
    print(
        "[TOTAL] Entries after "
        "filtering: "
        f"{len(all_entries):,}"
    )

    # ========================================================
    # CANONICALIZE BEFORE DEDUPE
    # ========================================================

    print()
    print(
        "[CANONICAL] Resolving "
        "channel identities..."
    )

    for entry in all_entries:

        apply_canonical(
            entry,
            resolver,
        )

    canonical_sources = (
        defaultdict(set)
    )

    for entry in all_entries:

        canonical_sources[
            entry.canonical_id
        ].add(
            entry.source
        )

    collisions = {
        cid: sources
        for cid, sources
        in canonical_sources.items()
        if len(sources) > 1
    }

    print(
        "[CANONICAL] "
        f"{len(collisions):,} "
        "canonical channels have "
        "multiple source candidates."
    )

    # --------------------------------------------------------
    # Canonical resolution diagnostics
    # --------------------------------------------------------

    print_canonical_diagnostic(
        all_entries,
        limit=50,
    )

    # ========================================================
    # DEDUPE
    # ========================================================

    print()
    print(
        "[DEDUPE] Selecting ONE "
        "stream per canonical channel..."
    )

    final_entries, grouped = (
        deduplicate(
            all_entries
        )
    )

    print(
        "[DEDUPE] "
        f"{len(all_entries):,} "
        "input entries -> "
        f"{len(final_entries):,} "
        "canonical streams"
    )

    # ========================================================
    # OUTPUT METADATA / GROUP LOCK
    # ========================================================

    print()
    print(
        "[GROUP] Resolving final groups..."
    )

    group_changes = Counter()

    for entry in final_entries:

        before = (
            entry.group_title
        )

        prepare_output_entry(
            entry,
            resolver,
        )

        after = (
            entry.canonical_group
        )

        group_changes[
            after
        ] += 1

        display = (
            entry.canonical_name
            or entry.original_name
            or entry.tvg_name
            or entry.tvg_id
        )

        # Explicit event logging.
        mapping = resolver.get(
            entry.canonical_id
        )

        if (
            after
            == FINAL_GROUPS["SU_KIEN"]
        ):

            print(
                "[EVENT] "
                f"{display} "
                f"-> {after} "
                f"source={entry.source}"
            )

            github_notice(
                "Event channel",
                (
                    f"{display} "
                    f"-> {after}"
                ),
            )

        # Warn when an otherwise identifiable channel
        # ends up in KHAC.
        if (
            after
            == FINAL_GROUPS["KHAC"]
            and entry.canonical_score
            >= 70
        ):

            print(
                "[WARN] Strong canonical "
                "match still classified "
                f"as KHAC: {display} "
                f"canonical={entry.canonical_id} "
                f"score={entry.canonical_score}"
            )

            github_warning(
                "Unexpected KHAC",
                (
                    f"{display} | "
                    f"canonical="
                    f"{entry.canonical_id} | "
                    f"score="
                    f"{entry.canonical_score}"
                ),
            )

    # ========================================================
    # SORT + VALIDATE
    # ========================================================

    final_entries = sort_entries(
        final_entries
    )

    validate_output(
        final_entries
    )

    # ========================================================
    # STATISTICS
    # ========================================================

    print_statistics(
        raw_counts,
        after_filter,
        final_entries,
        grouped,
        removed_counts,
    )

    # ========================================================
    # RENDER
    # ========================================================

    output_text = render_m3u(
        final_entries
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp_path = output_path.with_suffix(
        output_path.suffix
        + ".tmp"
    )

    tmp_path.write_text(
        output_text,
        encoding="utf-8",
    )

    tmp_path.replace(
        output_path
    )

    print(
        "[OK] Output written: "
        f"{output_path}"
    )

    print(
        "[OK] Final unique channels: "
        f"{len(final_entries):,}"
    )

    github_notice(
        "M3U optimizer completed",
        (
            f"{len(final_entries):,} "
            "unique channels written."
        ),
    )


# ============================================================
# CLI
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Fetch remote IPTV M3U "
            "sources, canonicalize, "
            "deduplicate and generate "
            "final playlist."
        )
    )

    parser.add_argument(
        "--mapping",
        default=str(
            DEFAULT_MAPPING
        ),
        help=(
            "Canonical mapping. "
            f"Default: {DEFAULT_MAPPING}"
        ),
    )

    parser.add_argument(
        "--output",
        default=str(
            DEFAULT_OUTPUT
        ),
        help=(
            "Output M3U. "
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

        github_error(
            "Optimizer failed",
            str(exc),
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
