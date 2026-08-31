from __future__ import annotations
import concurrent.futures, gzip, json, os, re, sys, time, unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parent.parent
CFG=ROOT/'m3u'
OUT=ROOT/'listtivi.m3u'
CACHE=CFG/'cache'; CACHE.mkdir(exist_ok=True)
EPG_URL='https://lichphatsong.io.vn/epg.xml'
SOURCES=[
 ('dltivi','DLTivi','https://raw.githubusercontent.com/DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl',100,True),
 ('vmttv','VMTTV','https://raw.githubusercontent.com/vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv',90,True),
 ('tinhlagi','TinhLaGi','https://tinhlagi.pro/tv.json',70,True),
 ('iptv_org','IPTV-org','https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/vn.m3u',50,True),
 ('easport','EaSport','https://livesport.s.gy/easport',30,True),
 # Explicitly disabled: adult endpoint must never enter the clean playlist.
 ('vietanhtv_sex','VietAnhTV / sex','https://tv.vietanhtv.top/sex/',20,False),
]
GROUPS=[
 ('vtv','📺 VTV'),('htv','📺 HTV'),('vtvcab','📡 VTVCab'),
 ('special','⭐ Kênh đặc biệt'),('htvc','📡 HTVC'),('sctv','📡 SCTV'),
 ('local','🏠 Địa phương'),('news','📰 Tin tức'),('movies','🎬 Phim & Giải trí'),
 ('music','🎵 Âm nhạc'),('sports','⚽ Thể thao'),('kids','👶 Thiếu nhi'),
 ('radio','📻 Radio'),('other','📦 Khác')]
GORDER={k:i for i,(k,_) in enumerate(GROUPS)}
ALIASES={
 'antv':('ANTV',['ANTV HD','An Ninh TV','Truyền hình Công an Nhân dân'],['special','news'],1),
 'qpvn':('QPVN',['QPVN HD','Quốc phòng Việt Nam','Kênh QPVN'],['special','news'],2),
 'vtv1':('VTV1',['VTV 1','VTV1 HD','VTV1 FHD','VTV1 1080P'],['vtv'],10),
 'vtv2':('VTV2',['VTV 2','VTV2 HD'],['vtv'],20), 'vtv3':('VTV3',['VTV 3','VTV3 HD'],['vtv'],30),
 'vtv4':('VTV4',['VTV 4','VTV4 HD'],['vtv'],40), 'vtv5':('VTV5',['VTV 5','VTV5 HD'],['vtv'],50),
 'vtv8':('VTV8',['VTV 8','VTV8 HD'],['vtv'],80), 'vtv9':('VTV9',['VTV 9','VTV9 HD'],['vtv'],90),
 'htv7':('HTV7',['HTV 7','HTV7 HD'],['htv'],10), 'htv9':('HTV9',['HTV 9','HTV9 HD'],['htv'],20),
 'vtv5_tay_nguyen':('VTV5 Tây Nguyên',['VTV5 Tây Nguyên HD'],['vtv','local'],51),
 'vtv5_tay_nam_bo':('VTV5 Tây Nam Bộ',['VTV5 Tây Nam Bộ HD'],['vtv','local'],52),
}
DROP_RE=re.compile(r'\b(?:porn|xxx|adult|casino|gambling|betting|test(?:\s+channel)?|demo(?:\s+channel)?|no\s*signal|offline|placeholder|self[ -]?promo|tự\s+quảng\s+cáo)\b',re.I)
VOD_RE=re.compile(r'\b(?:full\s*movie|movie|episode|trailer)\b',re.I)
UNOFFICIAL_RE=re.compile(r'\b(?:blv|cola\s*tv)\b',re.I)
TECH_RE=re.compile(r'\s*[\(\[\-]?\s*(?:UHD|4K|8K|FHD|FULL\s*HD|HD|SD|2160P?|1440P?|1080P?|720P?|576P?|480P?|50FPS|60FPS)\s*[\)\]]?\s*$',re.I)
ATTR_RE=re.compile(r'([\w-]+)="([^"]*)"')
URL_RE=re.compile(r'^(?:https?|rtmp|rtsp|udp)://',re.I)

def norm(s):
 s=''.join(c for c in unicodedata.normalize('NFD',s or '') if unicodedata.category(c)!='Mn')
 return re.sub(r'\s+',' ',re.sub(r'[|_/]+',' ',s)).strip().lower()
def clean(s): return re.sub(r'\s+',' ',TECH_RE.sub('',(s or '').strip()).strip(' -–—')).strip()
def esc(s): return (s or '').replace('"',"'").replace('\n',' ').replace('\r',' ')

def fetch(url, timeout=25):
 req=Request(url,headers={'User-Agent':'Mozilla/5.0 (M3U-Optimizer/1.0)','Accept':'*/*'})
 with urlopen(req,timeout=timeout) as r: return r.read()

def parse_extinf(line):
 body=line[len('#EXTINF:'):]; left,name=(body.split(',',1)+[''])[:2] if ',' in body else (body,'')
 return dict(ATTR_RE.findall(left)),name.strip()

def parse_m3u(text, sid, pri):
 lines=text.decode('utf-8-sig','replace').splitlines() if isinstance(text,bytes) else text.splitlines()
 out=[]; cur=None
 for raw in lines:
  s=raw.strip()
  if not s: continue
  if s.startswith('#EXTINF:'):
   a,n=parse_extinf(s); cur={'a':a,'name':n,'url':'','extra':[],'kodi':[],'headers':{},'sid':sid,'pri':pri}; continue
  if cur is None: continue
  if s.startswith('#KODIPROP:'): cur['kodi'].append(s); cur['extra'].append(s); continue
  if s.startswith('#EXTVLCOPT:'):
   cur['extra'].append(s)
   if '=' in s:
    k,v=s.split(':',1)[1].split('=',1); k=k.lower();
    if 'http-user-agent' in k: cur['headers']['User-Agent']=v.strip()
    elif 'http-referrer' in k or 'http-referer' in k: cur['headers']['Referer']=v.strip()
   continue
  if s.startswith('#'): cur['extra'].append(s); continue
  if URL_RE.match(s):
   cur['url']=s; out.append(cur); cur=None
 return out

def json_as_m3u(b):
 try:o=json.loads(b.decode('utf-8-sig','replace'))
 except Exception:return b
 arr=o if isinstance(o,list) else o.get('channels',o.get('streams',[])) if isinstance(o,dict) else []
 if not isinstance(arr,list): return b
 lines=['#EXTM3U']
 for x in arr:
  if not isinstance(x,dict):continue
  u=x.get('url') or x.get('stream') or x.get('stream_url'); n=x.get('name') or x.get('title') or ''
  if not u:continue
  attrs=[]
  for k,m in [('tvg_id','tvg-id'),('tvg_name','tvg-name'),('tvg_logo','tvg-logo'),('group','group-title')]:
   if x.get(k):attrs.append(f'{m}="{esc(str(x[k]))}"')
  lines.append('#EXTINF:-1 '+ ' '.join(attrs)+','+str(n)); lines.append(str(u))
 return '\n'.join(lines)

def source_items():
 all_items=[]; stats={}
 for sid,name,url,pri,en in SOURCES:
  if not en: stats[sid]={'status':'disabled','items':0}; continue
  try:
   b=fetch(url); txt=b.decode('utf-8-sig','replace')
   if not txt.lstrip().startswith('#EXTM3U') and not '#EXTINF:' in txt[:10000]:
    txt=json_as_m3u(b); txt=txt.decode('utf-8','replace') if isinstance(txt,bytes) else txt
   items=parse_m3u(txt,sid,pri)
   for x in items:
    x['candidate']=candidate(x)
    all_items.append(x)
   stats[sid]={'status':'ok','items':len(items)}
  except Exception as e: stats[sid]={'status':'error','error':str(e)[:250],'items':0}
 return all_items,stats

def candidate(x):
 u=x['url'].lower(); kind='dash' if '.mpd' in u else 'hls' if 'm3u8' in u else 'http'; q=0
 for token,pts in [('2160',40),('4k',40),('1080',30),('fhd',25),('720',15),('hd',10),('50fps',5),('60fps',5)]:
  if token in (x['name']+' '+x['url']).lower():q+=pts
 return {'url':x['url'],'sid':x['sid'],'pri':x['pri'],'headers':x['headers'],'extra':x['extra'],'kodi':x['kodi'],'kind':kind,'q':q,'health':'unknown'}

def known(name,tvg):
 n,t=norm(clean(name)),norm(tvg)
 for cid,(disp,als,groups,order) in ALIASES.items():
  vals=[norm(cid),norm(disp),*(norm(a) for a in als)]
  if n in vals or t in vals:return cid,ALIASES[cid]
 return norm(tvg) or n,None

def classify(name,raw,tvg,explicit=None):
 text=norm(f'{name} {raw} {tvg}'); gs=set(explicit or [])
 if norm(name) in ('antv','qpvn') or norm(tvg) in ('antv','qpvn'): gs.add('special')
 pats={'radio':r'\b(?:radio|vov|giao thong)\b','sports':r'\b(?:sport|the thao|football|soccer|k\+)\b','kids':r'\b(?:kids?|children|thieu nhi|cartoon|babytv|nickelodeon)\b','music':r'\b(?:music|mtv|ca nhac)\b','news':r'\b(?:news|tin tuc|vtc1|vtc14)\b','movies':r'\b(?:phim|movie|cinema|film|drama)\b'}
 for g,p in pats.items():
  if re.search(p,text,re.I):gs.add(g)
 for g,p in {'vtv':r'\bvtv\s*[1-9]\b','htv':r'\bhtv\b','vtvcab':r'\bvtvcab\b|\bon\s+(?:sports|football)\b','htvc':r'\bhtvc\b','sctv':r'\bsctv\b'}.items():
  if re.search(p,text,re.I):gs.add(g)
 if any(x in text for x in ('an giang','bac ninh','cao bang','dien bien','ha tinh','hai phong','hung yen','lai chau','lang son','lao cai','nghe an','quang ninh','thanh hoa','thai nguyen','yen bai','binh duong','dong nai','can tho','da nang','khanh hoa','binh dinh','phu yen','dak lak','gia lai','lam dong')):gs.add('local')
 gs.discard('international')
 return sorted(gs or {'other'},key=lambda x:GORDER.get(x,999))

def build_channels(items):
 channels={}; rejected=0
 for x in items:
  name=clean(x['name'] or x['a'].get('tvg-name') or x['a'].get('tvg-id') or 'Unknown'); raw=x['a'].get('group-title',''); blob=f'{name} {raw} {x["url"]}'
  if DROP_RE.search(norm(blob)) or VOD_RE.search(norm(name)) or UNOFFICIAL_RE.search(norm(blob)):
   rejected+=1;continue
  cid,rule=known(name,x['a'].get('tvg-id','')); disp=rule[0] if rule else name; explicit=rule[2] if rule else []
  gs=classify(disp,raw,x['a'].get('tvg-id',''),explicit); order=rule[3] if rule else 999999
  c=channels.setdefault(cid,{'id':cid,'name':disp,'tvg':x['a'].get('tvg-id','') or cid,'logo':'','logo_pri':-1,'groups':set(gs),'order':order,'streams':[]})
  c['groups'].update(gs); c['order']=min(c['order'],order); cand=x['candidate']
  if all(s['url']!=cand['url'] or s['headers']!=cand['headers'] for s in c['streams']):c['streams'].append(cand)
  logo=x['a'].get('tvg-logo','').strip()
  if logo and cand['pri']>c['logo_pri']:c['logo']=logo;c['logo_pri']=cand['pri']
 return list(channels.values()),rejected

def health_one(c,timeout=8,max_bytes=8192):
 if c['kodi']:
  c['health']='skipped'; c['reason']='KODIPROP'; return c
 h={'User-Agent':'Mozilla/5.0 (M3U-Optimizer/1.0)','Range':f'bytes=0-{max_bytes-1}'};h.update(c['headers'])
 try:
  with urlopen(Request(c['url'],headers=h),timeout=timeout) as r:
   data=r.read(max_bytes); c['health']='healthy' if data and getattr(r,'status',200) in (200,206) else 'dead';c['reason']=f'HTTP {getattr(r,"status",200)}'
 except Exception as e:c['health']='dead';c['reason']=str(e)[:120]
 return c

def healthcheck(channels):
 unique={}
 for ch in channels:
  for c in ch['streams']:unique.setdefault((c['url'],tuple(sorted(c['headers'].items()))),c)
 with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:list(ex.map(health_one,unique.values()))
 for ch in channels:
  usable=[c for c in ch['streams'] if c['health'] in ('healthy','skipped')]
  usable.sort(key=lambda c:(-c['pri'],-c['q'],c['url']))
  ch['streams']=usable[:2]
 return channels,len(unique)

def epg_fetch():
 cache=CACHE/'epg.xml'
 try:
  b=fetch(EPG_URL,30)
  if b[:2]==b'\x1f\x8b':b=gzip.decompress(b)
  text=b.decode('utf-8-sig','replace');ET.fromstring(text);cache.write_text(text,encoding='utf-8');return text,True
 except Exception:
  return (cache.read_text(encoding='utf-8',errors='replace'),False) if cache.exists() else ('',False)

def epg_ids(text):
 if not text:return {}
 root=ET.fromstring(text);d={}
 for c in root.findall('channel'):
  cid=c.get('id','').strip()
  for n in c.findall('display-name'):
   if n.text:d[n.text.strip().lower()]=cid
 return d

def render(channels):
 lines=['#EXTM3U']
 by={g:[] for g,_ in GROUPS}
 for c in channels:
  if not c['streams']:continue
  for g in c['groups']:
   if g in by:by[g].append(c)
 for g,_ in GROUPS:
  for c in sorted(by[g],key=lambda x:(x['order'],x['name'].casefold(),x['id'])):
   for i,s in enumerate(c['streams'][:2]):
    suffix='' if i==0 else ' [Dự phòng]';attrs=[f'tvg-id="{esc(c["tvg"])}"',f'tvg-name="{esc(c["name"])}"',f'group-title="{esc(dict(GROUPS)[g])}"']
    if c['logo']:attrs.append(f'tvg-logo="{esc(c["logo"])}"')
    lines.append('#EXTINF:-1 '+' '.join(attrs)+','+esc(c['name']+suffix));lines.extend(s for s in s['extra']);lines.append(s['url'])
 return '\n'.join(lines)+'\n'

def main():
 items,stats=source_items(); channels,rejected=build_channels(items); channels,nhealth=healthcheck(channels);channels=[c for c in channels if c['streams']]
 # IPTV-org logo fallback only.
 fb={}
 for x in items:
  if x['sid']=='iptv_org' and x['a'].get('tvg-logo'):
   logo=x['a']['tvg-logo'].strip();fb[x['a'].get('tvg-id','').lower()]=logo;fb[norm(x['name'])]=logo
 for c in channels:
  if not c['logo']:
   for k in (c['tvg'].lower(),norm(c['name']),c['id']):
    if fb.get(k):c['logo']=fb[k];break
 # EPG exact mapping is deliberately conservative.
 epg, fresh=epg_fetch(); em=epg_ids(epg)
 for c in channels:
  if c['tvg'].lower() in em:c['epg']=em[c['tvg'].lower()]
  elif norm(c['name']) in em:c['epg']=em[norm(c['name'])]
  else:c['epg']=''
 # Hard invariant: special contains only ANTV/QPVN.
 for c in channels:
  if c['id'] not in ('antv','qpvn'):c['groups'].discard('special')
  if not c['groups']:c['groups'].add('other')
 text=render(channels)
 if len(channels)<50 or text.count('#EXTINF:')<50:raise RuntimeError(f'Quality gate failed: channels={len(channels)} entries={text.count("#EXTINF:")}')
 tmp=OUT.with_suffix('.tmp');tmp.write_text(text,encoding='utf-8');os.replace(tmp,OUT)
 result={'channels':len(channels),'output_entries':text.count('#EXTINF:'),'unique_streams_checked_or_skipped':nhealth,'rejected':rejected,'epg_fresh':fresh,'sources':stats}
 (CACHE/'build_stats.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result,ensure_ascii=False,indent=2));return 0

if __name__=='__main__':raise SystemExit(main())
