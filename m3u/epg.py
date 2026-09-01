from __future__ import annotations
from pathlib import Path
from urllib.request import Request,urlopen
import gzip, xml.etree.ElementTree as ET

def update(url, cache, timeout=30):
    cache=Path(cache); cache.parent.mkdir(parents=True,exist_ok=True)
    try:
        req=Request(url,headers={'User-Agent':'Mozilla/5.0 (M3U-Optimizer/2.0)'})
        with urlopen(req,timeout=timeout) as r: data=r.read()
        if data[:2]==b'\x1f\x8b': data=gzip.decompress(data)
        text=data.decode('utf-8-sig','replace'); ET.fromstring(text)
        tmp=cache.with_suffix('.tmp'); tmp.write_text(text,encoding='utf-8'); tmp.replace(cache)
        return 'fresh'
    except Exception:
        return 'cached' if cache.exists() else 'unavailable'
