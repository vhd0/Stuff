from __future__ import annotations
from .groups import GROUPS

def esc(s): return (s or '').replace('"',"'").replace('\n',' ').replace('\r',' ')

def render(channels):
    lines=['#EXTM3U']
    for gid,g in sorted(GROUPS.items(),key=lambda x:x[1]['order']):
        members=[c for c in channels.values() if gid in c.groups and c.streams]
        members.sort(key=lambda c:(c.order,c.name.casefold(),c.id))
        for c in members:
            for idx,s in enumerate(c.streams[:2]):
                suffix='' if idx==0 else ' [Dự phòng]'
                attrs=[f'tvg-id="{esc(c.tvg_id)}"',f'tvg-name="{esc(c.name)}"',f'group-title="{esc(g["name"])}"']
                if c.logo: attrs.append(f'tvg-logo="{esc(c.logo)}"')
                lines.append(f'#EXTINF:-1 {" ".join(attrs)},{esc(c.name+suffix)}')
                lines.extend(s.extras); lines.append(s.url)
    return '\n'.join(lines)+'\n'
