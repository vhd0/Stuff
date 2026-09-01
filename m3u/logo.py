from __future__ import annotations
from .normalize import key

def choose_logo(channel, item):
    logo=(item.attrs.get('tvg-logo') or '').strip()
    if logo and item.priority>channel.logo_priority:
        channel.logo=logo; channel.logo_priority=item.priority

def apply_iptv_org_fallback(channels, items):
    index={}
    for i in items:
        if i.source!='iptv_org': continue
        logo=(i.attrs.get('tvg-logo') or '').strip()
        if not logo: continue
        for k in (i.attrs.get('tvg-id',''),i.attrs.get('tvg-name',''),i.name):
            if k: index[key(k)]=logo
    for c in channels.values():
        if c.logo: continue
        for k in (c.tvg_id,c.name,c.id):
            if key(k) in index:
                c.logo=index[key(k)]; break
