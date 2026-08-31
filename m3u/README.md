# M3U final v1

- Root: `optimize_m3u.py`, `update_m3u.yml`.
- EPG: `https://lichphatsong.io.vn/epg.xml` only, cached fallback.
- Max 2 streams/channel: primary + backup.
- KODIPROP streams skip healthcheck and remain eligible.
- Logos: source priority first, IPTV-org fallback only.
- Multi-group channels are supported.
- Group order: VTV, HTV, VTVCab, Kênh đặc biệt, HTVC, SCTV, Địa phương, Tin tức, Phim & Giải trí, Âm nhạc, Thể thao, Thiếu nhi, Radio, Khác.
- Kênh đặc biệt is restricted to ANTV and QPVN.
- No International group; international channels are classified by content.
- Adult, junk, self-promo, VOD-like, test/offline and unofficial BLV/community sports are filtered.
