# M3U Optimizer

Run from repository root:

```bash
python3 scripts/optimize_m3u.py
```

The architecture intentionally trusts the source `group-title` first, then
uses canonical OTT-style category rules as secondary classification. It does
not hard-filter ordinary words such as `movie`, `sport`, `news`, or `event`.

Detailed recovery rules: `guidelines.txt`.
