from __future__ import annotations

import html
import json
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml
from rapidfuzz import fuzz, process


# ============================================================
# PATHS
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

CONFIG_FILE = ROOT / "m3u" / "channel_aliases.yml"
CACHE_DIR = ROOT / ".cache" / "iptv"

OUTPUT_FILE = ROOT / "listtivi.m3u"
FALLBACK_FILE = ROOT / "m3u" / "listtivi.m3u"


# ============================================================
# SETTINGS
# ============================================================

META_CACHE_TTL = int(
    os.getenv("META_CACHE_TTL", "86400")
)

MIN_CHANNELS = int(
    os.getenv("MIN_CHANNELS", "100")
)

MIN_RATIO = float(
    os.getenv("MIN_RATIO", "0.70")
)

CONNECT_TIMEOUT = int(
    os.getenv("CONNECT_TIMEOUT", "15")
)

READ_TIMEOUT = int(
    os.getenv("READ_TIMEOUT", "45")
)

REQUEST_TIMEOUT = (
    CONNECT_TIMEOUT,
    READ_TIMEOUT,
)

DALVIK_USER_AGENT = (
    "Dalvik/2.1.0 "
    "(Linux; U; Android 13; SM-S918B "
    "Build/TP1A.220624.014)"
)

HTTP_HEADERS = {
    "User-Agent": DALVIK_USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive",
}


# ============================================================
# OFFICIAL IPTV-ORG API
# ============================================================

IPTV_ORG_CHANNELS_URL = (
    "https://iptv-org.github.io/api/channels.json"
)

IPTV_ORG_LOGOS_URL = (
    "https://iptv-org.github.io/api/logos.json"
)


# ============================================================
# DEFAULT SOURCES
# ============================================================

DEFAULT_SOURCES = {
    "vmttv": (
        "https://raw.githubusercontent.com/"
        "vuminhthanh12/vuminhthanh12/"
        "refs/heads/main/vmttv"
    ),

    "vietanhtv": (
        "https://tv.vietanhtv.top/sex/"
    ),

    "dltivi": (
        "https://raw.githubusercontent.com/"
        "DinhLap96/ListTivi/"
        "refs/heads/main/ListTiVi/dltivi_v2.ndl"
    ),

    "iptv-org": (
        "https://raw.githubusercontent.com/"
        "iptv-org/iptv/"
        "refs/heads/master/streams/vn.m3u"
    ),

    "easport": (
        "https://livesport.s.gy/easport"
    ),
}


# ============================================================
# DATA MODEL
# ============================================================

@dataclass
class M3UEntry:
    source: str
    extinf: str
    url: str

    metadata: list[str] = field(
        default_factory=list
    )

    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group: str = ""
    name: str = ""

    canonical_id: str = ""
    canonical_name: str = ""
    canonical_group: str = ""

    mapping_method: str = "unknown"
    mapping_score: int = 0


# ============================================================
# HTTP SESSION
# ============================================================

SESSION = requests.Session()
SESSION.headers.update(HTTP_HEADERS)


def fetch_text(url: str) -> str:
    """
    Download text with retry.
    No stream health-checking is performed.
    """

    last_error = None

    for attempt in range(3):
        try:
            response = SESSION.get(
                url,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            if not response.encoding:
                response.encoding = "utf-8"

            return response.text

        except Exception as exc:
            last_error = exc

            if attempt < 2:
                time.sleep(2 ** attempt)

    raise RuntimeError(
        f"Không thể tải nguồn: {url} | {last_error}"
    )


def load_cached_json(
    url: str,
    filename: str,
):
    """
    Cache only metadata.
    """

    CACHE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    cache_file = CACHE_DIR / filename

    if cache_file.exists():
        age = (
            time.time()
            - cache_file.stat().st_mtime
        )

        if age < META_CACHE_TTL:
            try:
                return json.loads(
                    cache_file.read_text(
                        encoding="utf-8"
                    )
                )
            except Exception:
                pass

    data = fetch_text(url)

    parsed = json.loads(data)

    cache_file.write_text(
        json.dumps(
            parsed,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return parsed


# ============================================================
# TEXT NORMALIZATION
# ============================================================

def remove_accents(value: str) -> str:
    value = unicodedata.normalize(
        "NFD",
        value,
    )

    return "".join(
        char
        for char in value
        if unicodedata.category(char) != "Mn"
    )


def normalize_text(value: str) -> str:
    if not value:
        return ""

    value = html.unescape(str(value))

    value = value.lower().strip()

    value = remove_accents(value)

    value = value.replace("đ", "d")

    # Quality suffixes are not part of channel identity.
    value = re.sub(
        r"\b("
        r"8k|4k|uhd|fhd|fullhd|"
        r"1080p|1080i|720p|576p|480p|"
        r"hd|sd"
        r")\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    # Common words that don't identify the channel.
    value = re.sub(
        r"\b("
        r"live|truc tiep|trực tiếp|"
        r"channel|kenh|kênh"
        r")\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    value = value.replace(
        "&",
        " and ",
    )

    value = re.sub(
        r"[_./|:+\-]+",
        " ",
        value,
    )

    value = re.sub(
        r"[^a-z0-9\s]",
        " ",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def compact(value: str) -> str:
    return normalize_text(value).replace(
        " ",
        "",
    )


# ============================================================
# M3U PARSER
# ============================================================

def extract_m3u_payload(text: str) -> str:
    """
    Some sources may return HTML or extra content
    before the actual M3U payload.
    """

    extm3u_pos = text.find("#EXTM3U")

    if extm3u_pos >= 0:
        return text[extm3u_pos:]

    extinf_pos = text.find("#EXTINF")

    if extinf_pos >= 0:
        return (
            "#EXTM3U\n"
            + text[extinf_pos:]
        )

    raise ValueError(
        "Nguồn không chứa dữ liệu M3U hợp lệ."
    )


def parse_extinf(line: str):
    attributes = {}

    for match in re.finditer(
        r'([\w-]+)="([^"]*)"',
        line,
    ):
        key = match.group(1).lower()
        value = html.unescape(
            match.group(2)
        )

        attributes[key] = value

    if "," in line:
        name = line.split(
            ",",
            1,
        )[1].strip()
    else:
        name = attributes.get(
            "tvg-name",
            "",
        ).strip()

    group = attributes.get(
        "group-title",
        "",
    ).strip()

    return (
        attributes,
        name,
        group,
    )


def parse_m3u(
    text: str,
    source: str,
):
    text = extract_m3u_payload(text)

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    epg_urls = []

    if lines:
        header = lines[0]

        for match in re.finditer(
            r'url-tvg="([^"]+)"',
            header,
            flags=re.IGNORECASE,
        ):
            epg_urls.extend(
                item.strip()
                for item in match.group(1).split(",")
                if item.strip()
            )

    entries = []

    index = 0

    while index < len(lines):

        line = lines[index]

        if not line.startswith(
            "#EXTINF"
        ):
            index += 1
            continue

        (
            attributes,
            name,
            group,
        ) = parse_extinf(line)

        metadata = []

        url = ""

        cursor = index + 1

        while cursor < len(lines):

            current = lines[cursor]

            if current.startswith(
                "#EXTINF"
            ):
                break

            if current.startswith("#"):

                # Preserve playback-related metadata.
                if (
                    current.startswith(
                        "#KODIPROP"
                    )
                    or current.startswith(
                        "#EXTVLCOPT"
                    )
                    or current.startswith(
                        "#EXTHTTP"
                    )
                    or current.startswith(
                        "#EXT-X-"
                    )
                ):
                    metadata.append(
                        current
                    )

                cursor += 1
                continue

            url = current
            break

        if url:
            entries.append(
                M3UEntry(
                    source=source,
                    extinf=line,
                    url=url,
                    metadata=metadata,
                    tvg_id=attributes.get(
                        "tvg-id",
                        "",
                    ).strip(),
                    tvg_name=attributes.get(
                        "tvg-name",
                        "",
                    ).strip(),
                    tvg_logo=attributes.get(
                        "tvg-logo",
                        "",
                    ).strip(),
                    group=group,
                    name=name,
                )
            )

        index = max(
            cursor,
            index + 1,
        )

    return (
        epg_urls,
        entries,
    )


# ============================================================
# IPTV-ORG DATABASE
# ============================================================

class IPTVOrgDatabase:

    def __init__(
        self,
        channels,
        logos,
    ):
        self.channels = channels
        self.logos = logos

        self.by_id = {}
        self.name_to_id = {}
        self.logo_by_channel = {}

        self._build()

    def _build(self):

        for channel in self.channels:

            channel_id = str(
                channel.get(
                    "id",
                    "",
                )
            ).strip()

            if not channel_id:
                continue

            self.by_id[
                channel_id
            ] = channel

            names = []

            name = channel.get(
                "name"
            )

            if name:
                names.append(
                    str(name)
                )

            alt_names = (
                channel.get(
                    "alt_names"
                )
                or []
            )

            names.extend(
                str(x)
                for x in alt_names
                if x
            )

            for name in names:

                key = compact(name)

                if key:
                    self.name_to_id.setdefault(
                        key,
                        channel_id,
                    )

        for logo in self.logos:

            channel_id = str(
                logo.get(
                    "channel",
                    "",
                )
            ).strip()

            logo_url = str(
                logo.get(
                    "url",
                    "",
                )
            ).strip()

            if (
                not channel_id
                or not logo_url
            ):
                continue

            current = self.logo_by_channel.get(
                channel_id
            )

            # Prefer an in-use logo.
            if (
                current is None
                or logo.get(
                    "in_use"
                ) is True
            ):
                self.logo_by_channel[
                    channel_id
                ] = logo_url


# ============================================================
# CONFIG
# ============================================================

def load_config():
    if not CONFIG_FILE.exists():
        return {
            "source_priority": {
                "vmttv": 10,
                "vietanhtv": 20,
                "dltivi": 30,
                "iptv-org": 40,
                "easport": 50,
            },
            "sources": DEFAULT_SOURCES,
            "groups": {},
            "channels": {},
            "exclude": {},
            "avoid_labels": [],
            "group_order": [],
        }

    return yaml.safe_load(
        CONFIG_FILE.read_text(
            encoding="utf-8"
        )
    ) or {}


# ============================================================
# YAML CHANNEL INDEX
# ============================================================

def build_yaml_channel_indexes(
    config: dict,
):
    channels = config.get(
        "channels",
        {},
    )

    by_id = {}
    alias_to_id = {}

    for canonical_id, definition in channels.items():

        canonical_id = str(
            canonical_id
        ).strip()

        if not canonical_id:
            continue

        definition = (
            definition
            if isinstance(
                definition,
                dict,
            )
            else {}
        )

        by_id[
            canonical_id
        ] = definition

        values = [
            canonical_id,
            definition.get(
                "name",
                "",
            ),
        ]

        values.extend(
            definition.get(
                "aliases",
                [],
            )
            or []
        )

        values.extend(
            definition.get(
                "ids",
                [],
            )
            or []
        )

        for value in values:

            if not value:
                continue

            alias_to_id[
                compact(value)
            ] = canonical_id

    return (
        by_id,
        alias_to_id,
    )


# ============================================================
# EXCLUSION
# ============================================================

def is_excluded(
    entry: M3UEntry,
    config: dict,
) -> bool:

    exclude = config.get(
        "exclude",
        {},
    )

    group_exclusions = (
        exclude.get(
            "groups",
            {},
        )
        or {}
    )

    channel_exclusions = (
        exclude.get(
            "channels",
            {},
        )
        or {}
    )

    forbidden_groups = (
        group_exclusions.get(
            entry.source,
            [],
        )
        or []
    )

    forbidden_channels = (
        channel_exclusions.get(
            entry.source,
            [],
        )
        or []
    )

    group_key = compact(
        entry.group
    )

    name_key = compact(
        entry.name
    )

    tvg_id_key = compact(
        entry.tvg_id
    )

    for forbidden in forbidden_groups:

        if group_key == compact(
            forbidden
        ):
            return True

    for forbidden in forbidden_channels:

        forbidden_key = compact(
            forbidden
        )

        if (
            name_key == forbidden_key
            or tvg_id_key == forbidden_key
        ):
            return True

    return False


# ============================================================
# GROUP MAPPING
# ============================================================

def map_group(
    group: str,
    config: dict,
) -> str:

    if not group:
        return "Khác"

    groups = config.get(
        "groups",
        {},
    ) or {}

    key = compact(group)

    # Exact first.
    for canonical, definition in groups.items():

        definition = (
            definition
            if isinstance(
                definition,
                dict,
            )
            else {}
        )

        aliases = [
            canonical
        ]

        aliases.extend(
            definition.get(
                "aliases",
                [],
            )
            or []
        )

        for alias in aliases:

            if key == compact(alias):
                return canonical

    # Conservative fuzzy fallback.
    candidates = {}

    for canonical, definition in groups.items():

        definition = (
            definition
            if isinstance(
                definition,
                dict,
            )
            else {}
        )

        aliases = [
            canonical
        ]

        aliases.extend(
            definition.get(
                "aliases",
                [],
            )
            or []
        )

        for alias in aliases:

            normalized = normalize_text(
                alias
            )

            if normalized:
                candidates[
                    normalized
                ] = canonical

    if candidates:

        result = process.extractOne(
            normalize_text(group),
            candidates.keys(),
            scorer=fuzz.ratio,
        )

        if result:

            matched, score, _ = result

            if score >= 94:
                return candidates[
                    matched
                ]

    return group.strip()


# ============================================================
# CHANNEL MAPPING
# ============================================================

def map_channel(
    entry: M3UEntry,
    config: dict,
    db: IPTVOrgDatabase,
):
    (
        yaml_channels,
        yaml_aliases,
    ) = build_yaml_channel_indexes(
        config
    )

    # --------------------------------------------------------
    # 1. EXACT IPTV-ORG ID
    # Highest confidence.
    # --------------------------------------------------------

    tvg_id = entry.tvg_id.strip()

    if tvg_id in db.by_id:

        channel = db.by_id[
            tvg_id
        ]

        entry.canonical_id = tvg_id

        entry.canonical_name = str(
            channel.get(
                "name"
            )
            or entry.name
        ).strip()

        entry.mapping_method = (
            "iptv-org-id"
        )

        entry.mapping_score = 100

        return

    # --------------------------------------------------------
    # 2. EXACT YAML ID
    # --------------------------------------------------------

    tvg_id_key = compact(
        tvg_id
    )

    if (
        tvg_id_key
        and tvg_id_key in yaml_aliases
    ):

        canonical_id = yaml_aliases[
            tvg_id_key
        ]

        definition = yaml_channels[
            canonical_id
        ]

        entry.canonical_id = (
            canonical_id
        )

        entry.canonical_name = (
            definition.get(
                "name"
            )
            or entry.name
        )

        entry.mapping_method = (
            "yaml-id"
        )

        entry.mapping_score = 99

        return

    # --------------------------------------------------------
    # 3. EXACT YAML ALIAS
    # --------------------------------------------------------

    for candidate in [
        entry.tvg_name,
        entry.name,
    ]:

        key = compact(candidate)

        if (
            key
            and key in yaml_aliases
        ):

            canonical_id = yaml_aliases[
                key
            ]

            definition = yaml_channels[
                canonical_id
            ]

            entry.canonical_id = (
                canonical_id
            )

            entry.canonical_name = (
                definition.get(
                    "name"
                )
                or candidate
            )

            entry.mapping_method = (
                "yaml-alias"
            )

            entry.mapping_score = 98

            return

    # --------------------------------------------------------
    # 4. EXACT IPTV-ORG NAME / ALT NAME
    # --------------------------------------------------------

    for candidate in [
        entry.tvg_name,
        entry.name,
    ]:

        key = compact(candidate)

        if (
            key
            and key in db.name_to_id
        ):

            canonical_id = db.name_to_id[
                key
            ]

            channel = db.by_id.get(
                canonical_id,
                {},
            )

            entry.canonical_id = (
                canonical_id
            )

            entry.canonical_name = str(
                channel.get(
                    "name"
                )
                or candidate
            ).strip()

            entry.mapping_method = (
                "iptv-org-name"
            )

            entry.mapping_score = 95

            return

    # --------------------------------------------------------
    # 5. HIGH-CONFIDENCE FUZZY
    # Only unknown channels reach this point.
    # --------------------------------------------------------

    query = normalize_text(
        entry.tvg_name
        or entry.name
    )

    if query:

        result = process.extractOne(
            query,
            db.name_to_id.keys(),
            scorer=fuzz.ratio,
        )

        if result:

            matched, score, _ = result

            if score >= 95:

                canonical_id = (
                    db.name_to_id[
                        matched
                    ]
                )

                channel = db.by_id.get(
                    canonical_id,
                    {},
                )

                entry.canonical_id = (
                    canonical_id
                )

                entry.canonical_name = str(
                    channel.get(
                        "name"
                    )
                    or entry.name
                ).strip()

                entry.mapping_method = (
                    "iptv-org-fuzzy"
                )

                entry.mapping_score = int(
                    score
                )

                return

    # --------------------------------------------------------
    # 6. LOCAL FALLBACK
    # --------------------------------------------------------

    local_name = (
        entry.tvg_name
        or entry.name
    ).strip()

    entry.canonical_id = (
        compact(local_name)
        or "unknown"
    )

    entry.canonical_name = (
        local_name
        or entry.canonical_id
    )

    entry.mapping_method = (
        "local"
    )

    entry.mapping_score = 50


# ============================================================
# LOGO
# ============================================================

def resolve_logo(
    entry: M3UEntry,
    db: IPTVOrgDatabase,
) -> str:

    # Raw source logo always wins.
    if entry.tvg_logo.strip():
        return entry.tvg_logo.strip()

    # Fallback to iptv-org.
    return db.logo_by_channel.get(
        entry.canonical_id,
        "",
    )


# ============================================================
# STREAM URL VALIDATION
# ============================================================

def valid_stream_url(
    url: str,
) -> bool:

    try:
        parsed = urlparse(
            url.strip()
        )

        return (
            parsed.scheme
            in {
                "http",
                "https",
            }
            and bool(
                parsed.hostname
            )
        )

    except Exception:
        return False


# ============================================================
# STREAM SELECTION
# ============================================================

def stream_score(
    entry: M3UEntry,
    config: dict,
) -> int:

    source_priority = config.get(
        "source_priority",
        {},
    ) or {}

    # Higher score = preferred source.
    priority = int(
        source_priority.get(
            entry.source,
            999,
        )
    )

    score = 10000 - (
        priority * 100
    )

    # Mapping confidence.
    score += (
        entry.mapping_score
    )

    # Exact tvg-id gets a bonus.
    if entry.tvg_id.strip():
        score += 20

    # Raw logo bonus.
    if entry.tvg_logo.strip():
        score += 5

    # Avoid bad labels without performing
    # network health checks.
    lower_name = (
        entry.name.lower()
    )

    for bad_label in (
        config.get(
            "avoid_labels",
            [],
        )
        or []
    ):

        if bad_label.lower() in lower_name:
            score -= 1000

    # Slight preference for HLS within
    # the same source.
    if ".m3u8" in entry.url.lower():
        score += 3

    return score


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate(
    entries: list[M3UEntry],
    config: dict,
):
    """
    Exactly one stream per canonical channel.
    """

    buckets = {}

    for entry in entries:

        key = (
            entry.canonical_id
            or compact(entry.name)
        )

        if not key:
            continue

        buckets.setdefault(
            key,
            [],
        ).append(entry)

    winners = []

    for canonical_id, candidates in buckets.items():

        candidates = [
            entry
            for entry in candidates
            if valid_stream_url(
                entry.url
            )
        ]

        if not candidates:
            continue

        # One URL only.
        unique_by_url = {}

        for entry in candidates:

            url = entry.url.strip()

            if url not in unique_by_url:
                unique_by_url[
                    url
                ] = entry

        candidates = list(
            unique_by_url.values()
        )

        winner = max(
            candidates,
            key=lambda entry:
                stream_score(
                    entry,
                    config,
                ),
        )

        winners.append(
            winner
        )

    return winners


# ============================================================
# EXTINF GENERATION
# ============================================================

def escape_attribute(
    value: str,
) -> str:

    return (
        str(value)
        .replace(
            "&",
            "&amp;",
        )
        .replace(
            '"',
            "&quot;",
        )
    )


def build_extinf(
    entry: M3UEntry,
    logo: str,
) -> str:

    attributes, _, _ = (
        parse_extinf(
            entry.extinf
        )
    )

    attributes[
        "tvg-id"
    ] = entry.canonical_id

    attributes[
        "tvg-name"
    ] = entry.canonical_name

    attributes[
        "group-title"
    ] = entry.canonical_group

    if logo:
        attributes[
            "tvg-logo"
        ] = logo
    else:
        attributes.pop(
            "tvg-logo",
            None,
        )

    # Standard output order.
    ordered_keys = [
        "tvg-id",
        "tvg-name",
        "tvg-logo",
        "group-title",
    ]

    parts = []

    for key in ordered_keys:

        value = attributes.get(
            key,
            "",
        )

        if value:
            parts.append(
                f'{key}="{escape_attribute(value)}"'
            )

    return (
        "#EXTINF:-1 "
        + " ".join(parts)
        + ","
        + entry.canonical_name
    )


# ============================================================
# WRITE PLAYLIST
# ============================================================

def write_playlist(
    path: Path,
    epg_urls: list[str],
    entries: list[M3UEntry],
    db: IPTVOrgDatabase,
):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    epg_urls = list(
        dict.fromkeys(
            x.strip()
            for x in epg_urls
            if x.strip()
        )
    )

    header = "#EXTM3U"

    if epg_urls:
        header += (
            ' url-tvg="'
            + ",".join(
                epg_urls
            )
            + '"'
        )

    lines = [header]

    for entry in entries:

        logo = resolve_logo(
            entry,
            db,
        )

        lines.append(
            build_extinf(
                entry,
                logo,
            )
        )

        # Preserve playback metadata.
        lines.extend(
            entry.metadata
        )

        lines.append(
            entry.url.strip()
        )

    path.write_text(
        "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )


# ============================================================
# SAFETY VALIDATION
# ============================================================

def count_channels(
    path: Path,
) -> int:

    if not path.exists():
        return 0

    return sum(
        1
        for line in path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).splitlines()
        if line.startswith(
            "#EXTINF"
        )
    )


def validate_result(
    output: Path,
):

    new_count = count_channels(
        output
    )

    print(
        f"Số channel tạo mới: {new_count}"
    )

    if new_count < MIN_CHANNELS:

        raise RuntimeError(
            "Playlist quá ít channel: "
            f"{new_count} < {MIN_CHANNELS}"
        )

    old_count = count_channels(
        FALLBACK_FILE
    )

    if old_count <= 0:
        return

    ratio = (
        new_count
        / old_count
    )

    print(
        "Playlist cũ: "
        f"{old_count} | "
        f"Tỷ lệ mới: "
        f"{ratio:.2%}"
    )

    if ratio < MIN_RATIO:

        raise RuntimeError(
            "Playlist mới giảm bất thường: "
            f"{ratio:.2%} < "
            f"{MIN_RATIO:.2%}"
        )


# ============================================================
# SUMMARY
# ============================================================

def print_summary(
    entries: list[M3UEntry],
):

    source_count = {}
    mapping_count = {}

    for entry in entries:

        source_count[
            entry.source
        ] = (
            source_count.get(
                entry.source,
                0,
            )
            + 1
        )

        mapping_count[
            entry.mapping_method
        ] = (
            mapping_count.get(
                entry.mapping_method,
                0,
            )
            + 1
        )

    print()
    print(
        "=== STREAM ĐƯỢC CHỌN ==="
    )

    for source, count in sorted(
        source_count.items()
    ):
        print(
            f"  {source}: {count}"
        )

    print()
    print(
        "=== PHƯƠNG THỨC MAPPING ==="
    )

    for method, count in sorted(
        mapping_count.items()
    ):
        print(
            f"  {method}: {count}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )
    print(
        " IPTV M3U OPTIMIZER V3.1"
    )
    print(
        "========================================"
    )
    print(
        "Health-check stream: TẮT"
    )
    print(
        "Canonical database: IPTV-org API"
    )
    print(
        "Deduplication: 1 channel = 1 stream"
    )
    print()

    config = load_config()

    # --------------------------------------------------------
    # Load canonical metadata.
    # --------------------------------------------------------

    print(
        "Đang tải metadata IPTV-org..."
    )

    channels = load_cached_json(
        IPTV_ORG_CHANNELS_URL,
        "channels.json",
    )

    logos = load_cached_json(
        IPTV_ORG_LOGOS_URL,
        "logos.json",
    )

    db = IPTVOrgDatabase(
        channels,
        logos,
    )

    print(
        f"  Channels database: "
        f"{len(db.by_id)}"
    )

    print(
        f"  Logo database: "
        f"{len(db.logo_by_channel)}"
    )

    # --------------------------------------------------------
    # Sources.
    # --------------------------------------------------------

    sources = config.get(
        "sources",
        DEFAULT_SOURCES,
    )

    all_entries = []
    all_epg_urls = []

    # --------------------------------------------------------
    # Fetch all five sources.
    # --------------------------------------------------------

    for source, url in sources.items():

        print()
        print(
            f"[{source}]"
        )

        try:

            text = fetch_text(
                url
            )

            (
                epg_urls,
                entries,
            ) = parse_m3u(
                text,
                source,
            )

            all_epg_urls.extend(
                epg_urls
            )

            print(
                f"  Parsed: "
                f"{len(entries)}"
            )

            original_count = len(
                entries
            )

            entries = [
                entry
                for entry in entries
                if not is_excluded(
                    entry,
                    config,
                )
            ]

            print(
                "  Sau khi loại trừ: "
                f"{len(entries)} "
                f"(loại "
                f"{original_count - len(entries)})"
            )

            for entry in entries:

                entry.canonical_group = (
                    map_group(
                        entry.group,
                        config,
                    )
                )

                map_channel(
                    entry,
                    config,
                    db,
                )

            all_entries.extend(
                entries
            )

        except Exception as exc:

            # One failed source should not
            # automatically destroy the playlist.
            print(
                f"  CẢNH BÁO: "
                f"Không đọc được {source}: "
                f"{exc}"
            )

    # --------------------------------------------------------
    # Ensure we have data.
    # --------------------------------------------------------

    if not all_entries:

        raise RuntimeError(
            "Không đọc được stream nào "
            "từ cả 5 nguồn."
        )

    print()
    print(
        f"Tổng stream đầu vào: "
        f"{len(all_entries)}"
    )

    # --------------------------------------------------------
    # Deduplicate.
    # --------------------------------------------------------

    winners = deduplicate(
        all_entries,
        config,
    )

    # --------------------------------------------------------
    # Sort by OTT-style group order.
    # --------------------------------------------------------

    group_order = config.get(
        "group_order",
        [],
    ) or {}

    group_index = {
        group: index
        for index, group
        in enumerate(group_order)
    }

    winners.sort(
        key=lambda entry: (
            group_index.get(
                entry.canonical_group,
                999,
            ),
            normalize_text(
                entry.canonical_name
            ),
        )
    )

    print()
    print(
        f"Channel duy nhất sau dedup: "
        f"{len(winners)}"
    )

    # --------------------------------------------------------
    # Write.
    # --------------------------------------------------------

    write_playlist(
        OUTPUT_FILE,
        all_epg_urls,
        winners,
        db,
    )

    # --------------------------------------------------------
    # Safety validation.
    # --------------------------------------------------------

    validate_result(
        OUTPUT_FILE
    )

    print_summary(
        winners
    )

    print()
    print(
        f"Đã tạo: {OUTPUT_FILE}"
    )
    print(
        "Hoàn tất."
    )


if __name__ == "__main__":

    try:
        main()

    except Exception as exc:

        print(
            f"\nLỖI: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
