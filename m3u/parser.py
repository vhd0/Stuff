from __future__ import annotations
import copy, json, re
from dataclasses import dataclass, field

ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')
URL_RE = re.compile(r'^(?:https?|rtmp|rtsp|udp)://', re.I)

@dataclass
class Item:
    source: str
    priority: int
    attrs: dict[str,str]
    name: str
    url: str
    extras: list[str] = field(default_factory=list)
    kodi: list[str] = field(default_factory=list)
    headers: dict[str,str] = field(default_factory=dict)
    source_group: str = ''
    quality: int = 0
    health: str = 'unknown'
    health_reason: str = ''

def parse_extinf(line: str):
    body=line[len('#EXTINF:'):]
    if ',' in body: left,name=body.split(',',1)
    else: left,name=body,''
    return dict(ATTR_RE.findall(left)), name.strip()

def _clone(base: Item, url: str):
    x=copy.copy(base)
    x.attrs=dict(base.attrs); x.extras=list(base.extras); x.kodi=list(base.kodi)
    x.headers=dict(base.headers); x.url=url
    return x

def parse_m3u(text: str, source: str, priority: int):
    """Parse normal M3U plus lists where one EXTINF has several consecutive URLs.

    Several of the supplied lists use exactly that pattern for backup streams.
    Every URL becomes a separate candidate while sharing the same metadata.
    """
    out=[]; cur=None
    for raw in text.splitlines():
        s=raw.strip()
        if not s: continue
        if s.startswith('#EXTINF:'):
            cur=None
            a,n=parse_extinf(s)
            cur=Item(source,priority,a,n,'')
            cur.source_group=a.get('group-title','')
            continue
        if cur is None: continue
        if s.startswith('#KODIPROP:'):
            cur.kodi.append(s); cur.extras.append(s); continue
        if s.startswith('#EXTVLCOPT:'):
            cur.extras.append(s)
            if '=' in s:
                k,v=s.split(':',1)[1].split('=',1)
                k=k.lower().strip(); v=v.strip()
                if 'http-user-agent' in k: cur.headers['User-Agent']=v
                elif 'http-referrer' in k or 'http-referer' in k: cur.headers['Referer']=v
            continue
        if s.startswith('#'):
            # Keep harmless directives, but do not let comments terminate an
            # EXTINF block: some lists place labels before backup URLs.
            cur.extras.append(s); continue
        if URL_RE.match(s):
            out.append(_clone(cur,s))
            continue
    return out

def json_to_m3u(text: str) -> str:
    obj=json.loads(text)
    arr=obj if isinstance(obj,list) else obj.get('channels',obj.get('streams',[])) if isinstance(obj,dict) else []
    lines=['#EXTM3U']
    for x in arr:
        if not isinstance(x,dict): continue
        url=x.get('url') or x.get('stream') or x.get('stream_url')
        name=x.get('name') or x.get('title') or ''
        if not url or not name: continue
        attrs=[]
        for k,m in [('tvg_id','tvg-id'),('tvg_name','tvg-name'),('tvg_logo','tvg-logo'),('group','group-title'),('group_title','group-title')]:
            if x.get(k): attrs.append(f'{m}="{str(x[k]).replace(chr(34),chr(39))}"')
        lines.append('#EXTINF:-1 '+ ' '.join(dict.fromkeys(attrs))+','+str(name))
        lines.append(str(url))
    return '\n'.join(lines)
