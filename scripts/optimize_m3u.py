#!/usr/bin/env python3
"""Repository entrypoint. Run from repository root: python3 scripts/optimize_m3u.py"""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from m3u.pipeline import build

if __name__ == '__main__':
    raise SystemExit(build(ROOT))
