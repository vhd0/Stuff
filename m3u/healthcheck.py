from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request,urlopen
from urllib.parse import urlparse

def _one(c, timeout=8, max_bytes=8192):
    if c.kodi:
        return c,'skipped','KODIPROP'
    headers={'User-Agent':'Mozilla/5.0 (M3U-Optimizer/2.0)','Range':f'bytes=0-{max_bytes-1}'}
    headers.update(c.headers)
    try:
        with urlopen(Request(c.url,headers=headers),timeout=timeout) as r:
            data=r.read(max_bytes)
            code=getattr(r,'status',200)
            return c,('healthy' if data and code in (200,206) else 'dead'),f'HTTP {code}'
    except Exception as e:
        return c,'dead',str(e)[:160]

def check(candidates, workers=32, timeout=8, max_bytes=8192):
    unique={}
    for c in candidates:
        unique.setdefault((c.url,tuple(sorted(c.headers.items()))),c)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for c,status,reason in ex.map(lambda x:_one(x,timeout,max_bytes),unique.values()):
            c.health=status;c.health_reason=reason
    return len(unique)
