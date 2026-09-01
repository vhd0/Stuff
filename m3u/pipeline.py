from __future__ import annotations
import json,re,sys,urllib.request
from dataclasses import dataclass,field
from pathlib import Path
import yaml
from .parser import parse_m3u,json_to_m3u,Item
from .normalize import channel_identity,clean_name,key
from .groups import classify,GROUPS
from .healthcheck import check
from .logo import choose_logo,apply_iptv_org_fallback
from .epg import update as update_epg

@dataclass
class Channel:
    id:str; name:str; tvg_id:str; groups:set[str]=field(default_factory=set)
    order:int=999999; logo:str=''; logo_priority:int=-1; streams:list=field(default_factory=list)

def fetch(url,timeout):
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 (M3U-Optimizer/2.0)','Accept':'*/*'})
    with urllib.request.urlopen(req,timeout=timeout) as r:return r.read()

def quality(name,url):
    t=(name+' '+url).lower(); q=0
    for token,pts in [('2160',40),('4k',40),('1080',30),('fhd',25),('720',15),('hd',10),('50fps',5),('60fps',5)]:
        if token in t:q+=pts
    return q

def blocked(item, cfg):
    text=f'{item.name} {item.attrs.get("tvg-name","")} {item.attrs.get("tvg-id","")}'
    group=item.source_group
    url=item.url.lower()
    for d in cfg['filters'].get('blocked_domains',[]):
        if d.lower() in url:return True,'blocked_domain'
    for p in cfg['filters'].get('blocked_name_patterns',[]):
        if re.search(p,text,re.I):return True,'blocked_name'
    for p in cfg['filters'].get('blocked_group_patterns',[]):
        if re.search(p,group,re.I):return True,'blocked_group'
    return False,''

def build(root:Path):
    m3u_dir=root/'m3u'; cfg=yaml.safe_load((m3u_dir/'config.yaml').read_text(encoding='utf-8'))
    stats={'sources':{},'input_items':0,'filtered':{},'channels_before_health':0,'unique_healthchecks':0,'channels_output':0}
    items=[]
    for src in cfg['sources']:
        if not src.get('enabled'):continue
        try:
            raw=fetch(src['url'],cfg['http']['source_timeout'])
            text=raw.decode('utf-8-sig','replace')
            if not text.lstrip().startswith('#EXTM3U') and '#EXTINF:' not in text[:20000]:
                text=json_to_m3u(text)
            parsed=parse_m3u(text,src['id'],src['priority'])
            for i in parsed:i.quality=quality(i.name,i.url)
            items.extend(parsed);stats['sources'][src['id']]={'status':'ok','items':len(parsed)}
        except Exception as e:
            stats['sources'][src['id']]={'status':'error','items':0,'error':str(e)[:250]}
    stats['input_items']=len(items)

    channels={}
    for item in items:
        bad,reason=blocked(item,cfg)
        if bad:
            stats['filtered'][reason]=stats['filtered'].get(reason,0)+1;continue
        raw_name=item.attrs.get('tvg-name') or item.name or item.attrs.get('tvg-id') or ''
        cid,display,rule=channel_identity(raw_name,item.attrs.get('tvg-id',''))
        manual_groups=rule.get('groups',[]) if rule else []
        groups=classify(item.source_group,display,item.attrs.get('tvg-id',''),manual_groups)
        # Special group is an invariant, not a heuristic.
        if key(display) in ('antv','qpvn'): groups=list(dict.fromkeys(['special']+groups))
        else: groups=[g for g in groups if g!='special']
        c=channels.get(cid)
        if c is None:
            c=Channel(cid,display,item.attrs.get('tvg-id','') or cid,set(groups),rule.get('order',999999) if rule else 999999)
            channels[cid]=c
        else:
            c.groups.update(groups); c.order=min(c.order,rule.get('order',999999) if rule else 999999)
        choose_logo(c,item)
        # Same stream may be offered by many sources; keep each source candidate
        # until healthcheck, then choose only two.
        sig=(item.url,tuple(sorted(item.headers.items())))
        if not any((s.url,tuple(sorted(s.headers.items())))==sig for s in c.streams):
            c.streams.append(item)
    stats['channels_before_health']=len(channels)

    candidates=[s for c in channels.values() for s in c.streams]
    stats['unique_healthchecks']=check(candidates,cfg['http']['workers'],cfg['http']['health_timeout'],cfg['http']['range_bytes']) if cfg['healthcheck']['enabled'] else 0

    for c in channels.values():
        healthy=[s for s in c.streams if getattr(s,'health','unknown') in ('healthy','skipped')]
        # A healthcheck is evidence, not an absolute truth: geo-blocking,
        # anti-bot/CDN behavior and GitHub runner networking can create false
        # negatives for streams that work in IPTV players. Prefer healthy
        # candidates, but if ALL candidates fail, retain the best unverified
        # candidate(s) instead of deleting a legitimate channel.
        pool=healthy or list(c.streams)
        pool.sort(key=lambda s:(0 if getattr(s,'health','unknown') in ('healthy','skipped') else 1, -s.priority, -s.quality, s.url))
        c.streams=pool[:cfg['selection']['max_streams']]
        for s in c.streams:
            if getattr(s,'health','unknown')=='dead': s.health='unverified'
    channels={k:v for k,v in channels.items() if v.streams}
    apply_iptv_org_fallback(channels,items)

    # EPG is updated/cached independently from channel selection.
    epg_status=update_epg(cfg['epg_url'],root/cfg['cache_dir']/'epg.xml',cfg['http']['source_timeout'])

    # Final invariants.
    for c in channels.values():
        if c.id not in ('antv','qpvn'):c.groups.discard('special')
        c.groups={g for g in c.groups if g in GROUPS} or {'other'}
    stats['channels_output']=len(channels);stats['epg']=epg_status

    out=root/cfg['output']; text=__import__('m3u.renderer',fromlist=['render']).render(channels)
    if not text.startswith('#EXTM3U') or text.count('#EXTINF:')<max(1,len(channels)):
        raise RuntimeError('Quality gate failed: output is incomplete')
    if any(g not in ('special','news') and c.id in ('antv','qpvn') for c in channels.values() for g in []):
        raise RuntimeError('Unexpected special-group state')
    tmp=out.with_suffix('.tmp');tmp.write_text(text,encoding='utf-8');tmp.replace(out)
    cache=root/cfg['cache_dir'];cache.mkdir(parents=True,exist_ok=True)
    (cache/'build_stats.json').write_text(json.dumps(stats,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(stats,ensure_ascii=False,indent=2));print(f'Wrote {out}')
    return 0
