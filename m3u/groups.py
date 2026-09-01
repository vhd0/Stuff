from __future__ import annotations
from pathlib import Path
import re, unicodedata, yaml

ROOT=Path(__file__).resolve().parent
DATA=yaml.safe_load((ROOT/'groups.yaml').read_text(encoding='utf-8'))
GROUPS={x['id']:x for x in DATA['groups']}
ALIASES=DATA['aliases']; NAME=DATA['name_keywords']

def norm(s):
    s=''.join(c for c in unicodedata.normalize('NFD',s or '') if unicodedata.category(c)!='Mn').lower()
    return re.sub(r'[^a-z0-9]+',' ',s).strip()

def map_source_group(raw: str):
    k=norm(raw)
    if not k: return None
    # Exact semantic aliases first. This preserves source classification.
    for gid, aliases in ALIASES.items():
        if k in {norm(a) for a in aliases}: return gid
    # Then controlled containment, longest alias first, so 'thể thao quốc tế'
    # becomes sports instead of other/international.
    choices=[]
    for gid,aliases in ALIASES.items():
        for a in aliases:
            ak=norm(a)
            if ak and (ak in k or k in ak): choices.append((len(ak),gid))
    return max(choices)[1] if choices else None

def name_groups(name: str):
    k=norm(name); out=[]
    for gid, words in NAME.items():
        if any(norm(w) and norm(w) in k for w in words): out.append(gid)
    return out

def classify(source_group: str, name: str, tvgid: str, manual_groups=None):
    out=[]
    if manual_groups: out.extend(manual_groups)
    sg=map_source_group(source_group)
    if sg: out.append(sg)
    # Canonical family identity has priority over content guesses.
    t=norm(f'{name} {tvgid}')
    if re.search(r'\bvtv\s*[1-9]\b',t): out.append('vtv')
    if re.search(r'\bhtv\s*[1-9]\b',t): out.append('htv')
    if 'vtvcab' in t: out.append('vtvcab')
    if 'htvc' in t: out.append('htvc')
    if re.search(r'\bsctv\b',t): out.append('sctv')
    # Only use name content categories as secondary memberships.
    out.extend(name_groups(name))
    # International is never a canonical output group. Its source group is
    # intentionally ignored; content categories above may still classify it.
    out=[g for g in dict.fromkeys(out) if g in GROUPS and g!='international']
    if not out: out=['other']
    return out
