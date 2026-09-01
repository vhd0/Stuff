from __future__ import annotations
import re, unicodedata
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parent
CFG=yaml.safe_load((ROOT/'config.yaml').read_text(encoding='utf-8'))
CHANNELS=yaml.safe_load((ROOT/'channels.yaml').read_text(encoding='utf-8')).get('channels',{})

def deaccent(s):
    return ''.join(c for c in unicodedata.normalize('NFD',s or '') if unicodedata.category(c)!='Mn')

def key(s):
    s=deaccent(s).lower()
    s=re.sub(r'[|_/]+',' ',s)
    return re.sub(r'[^a-z0-9+]+',' ',s).strip()

def clean_name(raw: str) -> str:
    s=(raw or '').strip()
    # Remove source annotations/quality markers anywhere at the edge.
    markers=CFG['normalization'].get('strip_markers',[])
    for marker in markers:
        s=re.sub(re.escape(marker), '', s, flags=re.I)
    tech=CFG['normalization']['technical_suffixes']
    # Remove repeated technical suffixes, e.g. 'VTV5 HD 50FPS'.
    prev=None
    while s != prev:
        prev=s
        s=re.sub(rf'\s*[\[\(]?\s*(?:{tech})\s*[\]\)]?\s*$', '', s, flags=re.I)
    # Repeated whitespace and decorative separators.
    s=re.sub(r'\s+', ' ', s).strip(' -–—|')
    return s

def channel_identity(name: str, tvgid: str):
    n=key(clean_name(name)); t=key(tvgid)
    for cid,r in CHANNELS.items():
        vals={key(cid),key(r.get('name',''))}|{key(x) for x in r.get('aliases',[])}
        if n in vals or t in vals:
            return cid,r['name'],r
    return (t or n),clean_name(name) or tvgid,runtime_rule(t or n)

def runtime_rule(cid):
    return {'groups':[],'order':999999,'aliases':[],'name':''}
