"""
Ghi m3u/cache/build_stats.json (muc 20 QUALITY GATE - danh sach thong ke
khuyen nghi).
"""

import json
import os

from . import config


def write_build_stats(stats):
    os.makedirs(config.CACHE_DIR, exist_ok=True)
    with open(config.BUILD_STATS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
