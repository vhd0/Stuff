M3U OPTIMIZER — DESIGN PHILOSOPHY v3 (RECOVERY DOCUMENT)
==========================================================

Tài liệu này là bản kế thừa và THAY THẾ `guidelines.txt` gốc. Nếu toàn bộ
code bị mất, coder chỉ cần đọc file này là viết lại được từ đầu, đúng ý
định thiết kế. Mọi quyết định "tại sao" quan trọng đều được giải thích ở
đây, không chỉ liệt kê "làm gì".

MỤC LỤC
-------
1. Mục đích và triết lý tổng quát
2. Lịch sử thay đổi: vì sao có v3 (đọc trước khi code lại bất cứ thứ gì)
3. Cấu trúc repo
4. Nguồn dữ liệu đầu vào
5. Fetch dữ liệu — mô phỏng OTT app thật
6. EPG
7. Lọc dữ liệu (filtering) — nguyên tắc bảo thủ
8. Chuẩn hoá tên kênh
9. Định danh kênh (channel identity)
10. Phân nhóm (grouping) — nguyên tắc và thứ tự ưu tiên
11. Nhóm đặc biệt (ANTV/QPVN)
12. Nhóm địa phương — vì sao cần "guard" nghiêm ngặt
13. Multi-group membership
14. Nhóm "Khác" — luôn là phương án cuối
15. Chọn stream — KHÔNG còn primary/backup
16. Logo
17. Output format
18. Quality gate
19. build_stats.json
20. GitHub Actions
21. Các bất biến (invariants) cần giữ nguyên khi viết lại
22. Ví dụ chấp nhận (acceptance examples)


1. MỤC ĐÍCH VÀ TRIẾT LÝ TỔNG QUÁT
---------------------------------
Xây dựng 1 playlist M3U cá nhân, sạch, thân thiện với TV, gộp từ nhiều
nguồn IPTV công khai trên GitHub/web. Nguyên tắc cốt lõi xuyên suốt toàn
bộ pipeline:

  a. TIN TƯỞNG group-title của nguồn làm tín hiệu phân loại CHÍNH khi
     nguồn đó được đánh dấu đáng tin (`trust_group_title: True`). Không
     dùng keyword-classifier tổng quát để GHI ĐÈ lên group-title đã có,
     trừ khi có bằng chứng cụ thể group-title đó sai (xem mục 12).

  b. OTT content classifier (theo tên kênh) chỉ là tín hiệu PHỤ, dùng khi
     không có group-title đáng tin, hoặc group-title không khớp bất kỳ
     alias nào đã biết.

  c. Lọc dữ liệu phải BẢO THỦ: chỉ loại bỏ khi có bằng chứng rõ ràng
     ("high-confidence junk"). Không bao giờ dùng từ khoá nội dung chung
     chung (movie/sport/news/event/live/channel...) làm lý do xoá — những
     từ này hoàn toàn có thể là tên kênh tuyến tính hợp lệ.

  d. KHÔNG BAO GIỜ để một bản build lỗi/rỗng ghi đè lên bản build tốt
     trước đó (xem mục 18 QUALITY GATE).

  e. Đơn giản hơn thà bỏ sót một chút còn hơn phức tạp hoá và tự bắn vào
     chân — lịch sử dự án này (xem mục 2) cho thấy mọi cơ chế "thông
     minh" thêm vào (healthcheck, primary/backup, keyword classifier quá
     tay) đều từng gây ra mất kênh hợp lệ nhiều hơn giá trị nó mang lại.


2. LỊCH SỬ THAY ĐỔI: VÌ SAO CÓ V3
----------------------------------
Đọc kỹ mục này trước khi "cải tiến" lại bất kỳ phần nào — các cơ chế bị
loại bỏ dưới đây đã được thử và THẤT BẠI trong thực tế, không phải chưa
được nghĩ tới.

### 2.1. TẠI SAO BỎ HEALTHCHECK (rất quan trọng — ĐỪNG thêm lại)

Bản v2 có cơ chế: trước khi ghi vào output, gọi GET+Range tới từng URL để
kiểm tra "còn sống" hay "chết", loại bỏ URL chết.

Vấn đề thực tế xảy ra: nhiều CDN Việt Nam (mytvnet, vtvdigital, fptplay,
vtvprime...) có hành vi khác nhau tuỳ theo nơi request đến từ đâu. Khi
GitHub Actions runner (đặt tại datacenter Mỹ/châu Âu) gọi trực tiếp các
URL này, nhiều stream hoàn toàn hợp lệ (đang được người dùng Việt Nam xem
bình thường) bị coi là "dead" — do bị chặn theo vùng địa lý, rate-limit
riêng cho IP ngoài Việt Nam, hoặc phản hồi khác thường cho HEAD/Range
request từ IP lạ. Hậu quả: NHIỀU KÊNH HOÀN TOÀN HỢP LỆ (ví dụ: ANTV,
SCTV1) biến mất khỏi playlist cuối cùng một cách ngẫu nhiên, tuỳ vào may
rủi của lần chạy — đây là lỗi ĐÃ XẢY RA THẬT trong sản xuất, không phải
giả thuyết.

Ngoài ra, healthcheck còn làm chậm đáng kể thời gian build (hàng trăm-
hàng nghìn request GET) và không thực tế: 1 URL "chết" lúc build (00:00)
có thể sống lại lúc người dùng thực sự mở playlist (07:00), và ngược lại
— kiểm tra tại thời điểm build không đảm bảo gì cho thời điểm xem thực
tế.

QUYẾT ĐỊNH: BỎ HOÀN TOÀN healthcheck. Mọi URL tìm được từ nguồn (sau khi
qua các bộ lọc bảo thủ ở mục 7) đều được đưa thẳng vào output, không kiểm
tra sống/chết. Nếu 1 URL thực sự chết, người dùng chỉ cần đợi lần cập
nhật tiếp theo hoặc dùng bản URL thay thế từ 1 nguồn khác đã được liệt kê
song song (xem mục 15).

Nếu trong tương lai có ý định "healthcheck lại", PHẢI đảm bảo: (a) chạy
healthcheck TỪ MỘT IP VIỆT NAM thật (self-hosted runner hoặc proxy VN),
không dùng runner mặc định của GitHub, và (b) không bao giờ để 1 kênh mất
HOÀN TOÀN chỉ vì mọi ứng viên URL của nó bị coi là "dead" — luôn có
fallback giữ ít nhất 1 URL "chưa xác minh" thay vì xoá sạch.

### 2.2. TẠI SAO BỎ CƠ CHẾ PRIMARY/BACKUP

Bản v2 giới hạn mỗi kênh tối đa 2 URL: 1 "primary" + 1 "backup" (nhãn
"[Dự phòng]"), chọn ra dựa theo kết quả healthcheck + source priority.

Vấn đề: cơ chế này gắn chặt với healthcheck (chọn URL "khoẻ nhất" cần
biết cái nào "khoẻ"). Khi bỏ healthcheck, khái niệm "primary/backup" mất
hết ý nghĩa — không còn cách nào để biết URL nào "tốt hơn" tại thời điểm
build. Ép 1 nhãn "primary" lên 1 URL không được xác minh gì hơn URL khác
là đánh lừa người dùng.

QUYẾT ĐỊNH: BỎ nhãn "primary/backup" và giới hạn cứng 2 URL/kênh. Thay
vào đó: mỗi kênh giữ TẤT CẢ URL duy nhất (đã dedup) từ mọi nguồn, sắp xếp
theo SOURCE PRIORITY (thứ tự khai báo trong `config.SOURCES`) làm gợi ý
thứ tự thử, giới hạn bởi `ALT_STREAM_SOFT_CAP` (mặc định 6) CHỈ để chống
phình to bất thường — không phải phân tầng chất lượng. Mỗi URL được ghi
thành 1 dòng `#EXTINF` riêng dùng CHUNG 1 TÊN KÊNH (không thêm hậu tố nào)
— đây là quy ước M3U phổ biến, đa số player tự động thử lần lượt các mục
trùng tên khi 1 nguồn lỗi.

### 2.3. Các thay đổi nhỏ hơn qua các lần lặp (tham khảo nhanh)
  - TinhLaGi (`tv.json`) thực tế trả về M3U chứ không phải JSON dù tên
    file gợi ý ngược lại → parser phải TỰ NHẬN DIỆN định dạng từ nội dung
    thật, không tin tên file/config khai báo.
  - Nhóm "In The Box" của các nguồn khác nhau có Ý NGHĨA KHÁC NHAU (có
    nguồn dùng cho sự kiện, có nguồn dùng cho bundle phim/nhạc lặt vặt) →
    KHÔNG được đưa các bundle mơ hồ này vào `group_alias` cố định, phải để
    rơi qua content classifier để tự phân loại theo từng kênh.
  - `vietanhtv.top/sex/` từng bị nghi là nội dung người lớn chỉ vì tên
    đường dẫn — đã XÁC MINH TRỰC TIẾP nội dung thật và xác nhận đây là 1
    aggregator M3U hợp lệ (VTV/HTV/SCTV/ANTV/QPVN/địa phương/radio đầy
    đủ). Bài học: KHÔNG suy diễn nội dung từ tên miền/đường dẫn, luôn xác
    minh nội dung thật trước khi kết luận.
  - Cùng nguồn `vietanhtv` có kèm 1 khối lớn nhóm "Socolive" — thương hiệu
    web lậu bóng đá gắn cá độ đã biết tại Việt Nam (đặc trưng: hàng trăm
    URL trùng 1 trận đấu, chỉ khác nhau bằng biệt danh "BLV <tên>"). Đây
    LÀ trường hợp lọc theo TÊN THƯƠNG HIỆU CỤ THỂ đã xác minh, KHÁC với
    lọc từ khoá chung chung ("sport"/"live") — xem `BLOCKED_STREAM_BRAND_GROUPS`
    ở mục 7.


3. CẤU TRÚC REPO
-----------------
    /scripts/optimize_m3u.py   - entry point, chạy: python3 scripts/optimize_m3u.py
    /m3u/config.py             - toàn bộ hằng số cấu hình (nguồn, filter, group order...)
    /m3u/parser.py             - parse M3U/NDL và JSON, tự nhận diện định dạng thật
    /m3u/normalize.py          - chuẩn hoá tên kênh, bỏ dấu, khoá định danh
    /m3u/channel_registry.py   - nạp channels.yaml, tra cứu canonical identity
    /m3u/channels.yaml         - alias kênh + extra_groups + whitelist đặc biệt
    /m3u/grouping.py           - GroupResolver: toàn bộ logic phân nhóm
    /m3u/groups.yaml           - group_alias, content_keywords, local_province_*
    /m3u/logo.py               - chọn logo theo source priority
    /m3u/iptvorg.py            - fallback logo từ iptv-org (KHÔNG dùng gì khác từ đó)
    /m3u/epg.py                - fetch + validate EPG XML, giữ cache cũ nếu lỗi
    /m3u/quality_gate.py       - kiểm tra trước khi cho phép ghi đè output
    /m3u/stats.py              - ghi build_stats.json
    /m3u/cache/                - build_stats.json, epg.xml (cache)
    /.github/workflows/*.yml   - workflow GitHub Actions

Chạy LUÔN từ repo root: `python3 scripts/optimize_m3u.py`. Script tự thêm
repo root vào `sys.path` để `import m3u` hoạt động dù chạy bằng đường dẫn
tương đối.


4. NGUỒN DỮ LIỆU ĐẦU VÀO
--------------------------
Khai báo tại `m3u/config.py` → `SOURCES`, là 1 LIST CÓ THỨ TỰ (thứ tự =
SOURCE PRIORITY, dùng cho chọn logo và sắp xếp URL thay thế). Mỗi nguồn là
1 dict:

    {
        "name": "<tên ngắn, dùng trong log/stats>",
        "url": "<URL>",
        "format": "m3u" | "json",   # chỉ là GỢI Ý fallback, xem mục 5
        "trust_group_title": True | False,
        "default_content_hint": "sports",  # tuỳ chọn, xem mục 10
    }

Danh sách hiện tại (thứ tự ưu tiên giảm dần):
  1. vmttv        - m3u, trust_group_title=True (nhóm rõ ràng theo genre)
  2. vietanhtv    - m3u, trust_group_title=True (aggregator lớn, có kèm
                    khối "Socolive" cần chặn riêng — xem mục 7)
  3. dltivi       - m3u, trust_group_title=False (không có group-title hữu ích)
  4. tinhlagi     - thực tế là m3u dù tên file .json, trust_group_title=True
  5. iptv-org-vn  - m3u, trust_group_title=False (group-title đồng loạt
                    "Vietnam", không có giá trị phân loại)
  6. easport      - m3u, trust_group_title=False, default_content_hint="sports"

Khi thêm nguồn mới: LUÔN xác minh nội dung THẬT bằng cách fetch trực tiếp
trước khi cấu hình `format`/`trust_group_title` — không suy diễn từ tên
file hay tên miền (bài học từ TinhLaGi và vietanhtv, xem mục 2.3).


5. FETCH DỮ LIỆU — MÔ PHỎNG OTT APP THẬT
------------------------------------------
Nhiều CDN IPTV Việt Nam yêu cầu User-Agent kiểu app di động (quan sát thấy
"Dalvik/2.1.0" xuất hiện lặp lại rất nhiều trong dữ liệu thực tế của nhiều
nguồn khác nhau), từ chối hoặc phản hồi chậm/timeout với User-Agent trình
duyệt thông thường hoặc không có User-Agent. Đây là nguyên nhân từng gây
timeout cho nguồn EaSport.

`scripts/optimize_m3u.py` → `fetch_source_text()`:
  - User-Agent giả lập app Android thật (`config.FETCH_USER_AGENT`).
  - Timeout dài hơn mặc định (`config.FETCH_TIMEOUT`, hiện 25s).
  - Retry với backoff tăng dần (`config.FETCH_RETRIES`,
    `config.FETCH_RETRY_BACKOFF`) khi gặp lỗi mạng tạm thời.

Hàm `parse_source(format, text)` trong `m3u/parser.py` LUÔN tự soi nội
dung thật (bắt đầu bằng `#EXTM3U`/`#EXTINF` → coi là M3U; bắt đầu bằng `{`
hoặc `[` → coi là JSON) TRƯỚC KHI dùng `format` khai báo làm fallback.
KHÔNG BAO GIỜ tin `format` khai báo một cách tuyệt đối — bài học từ
TinhLaGi.

Một số nguồn (như vietanhtv) là aggregator ghép nhiều playlist con lại,
có thể xuất hiện NHIỀU dòng `#EXTM3U` giữa file — parser phải bỏ qua an
toàn các dòng `#` không khớp `#EXTINF`/`#EXTVLCOPT`/`#KODIPROP` mà KHÔNG
reset ngữ cảnh đang xử lý.


6. EPG
------
Nguồn EPG duy nhất: `https://lichphatsong.io.vn/epg.xml` (đặt tại
`config.EPG_URL`). Không dùng API cũ nào khác.

`m3u/epg.py` → `fetch_and_validate_epg()`:
  - Fetch, validate cấu trúc XML bằng `xml.etree.ElementTree.fromstring`
    (chỉ kiểm tra parse được, không parse sâu nội dung).
  - Nếu OK: ghi đè `m3u/cache/epg.xml`.
  - Nếu lỗi: GIỮ NGUYÊN cache cũ, KHÔNG raise exception — lỗi EPG là
    non-fatal, không được làm mất bất kỳ kênh hợp lệ nào khác trong
    playlist.
  - Header output LUÔN tham chiếu `url-tvg="https://lichphatsong.io.vn/epg.xml"`
    bất kể fetch thành công hay không (player sẽ tự thử load).


7. LỌC DỮ LIỆU (FILTERING) — NGUYÊN TẮC BẢO THỦ
--------------------------------------------------
Áp dụng theo đúng thứ tự trong `scripts/optimize_m3u.py` → vòng lặp
`main()`, MỖI bộ lọc có lý do RIÊNG, không gộp chung:

  a. `is_blocked_entry()` — lọc theo TỪ KHOÁ CHUNG trong tên kênh/tvg-id:
     người lớn (porn/xxx/adult), cờ bạc (casino/gambling/bet/cá độ/cado),
     kênh test/demo/offline rõ ràng. Đây là danh sách NGẮN, CHỈ những từ
     có độ tin cậy cao. KHÔNG thêm "movie"/"sport"/"news"/"live"/"event"/
     "channel" vào đây — các từ này hoàn toàn hợp lệ trong tên kênh tuyến
     tính thật.

  b. `is_blocked_stream_brand_group()` — lọc theo TÊN THƯƠNG HIỆU CỤ THỂ
     của web lậu bóng đá/cá độ đã xác minh (hiện tại: "Socolive", danh
     sách tại `config.BLOCKED_STREAM_BRAND_GROUPS`). KHÁC HẲN với (a): đây
     là so khớp CHÍNH XÁC/GẦN-CHÍNH-XÁC theo tên NHÓM cụ thể, không phải
     từ khoá chung. Chỉ thêm brand mới vào đây khi đã XÁC MINH TRỰC TIẾP
     nội dung (đặc trưng nhận diện: hàng loạt URL trùng 1 trận đấu, chỉ
     khác biệt danh bình luận viên).

  c. `is_non_stream_url()` — lọc các mục "info/tự quảng cáo" của chính
     nhà cung cấp nguồn, nhận diện qua việc URL trỏ tới 1 FILE ẢNH TĨNH
     (`.jpg/.png/.gif/.webp/.bmp/.svg`) thay vì 1 stream thật. Đây là dấu
     hiệu KỸ THUẬT khách quan, không cần đoán từ khoá tiêu đề (ví dụ thực
     tế: "Địa Chỉ IP Của Bạn", "Cập Nhật Danh Sách" của TinhLaGi).

  d. `is_vod_episode_entry()` — lọc phim bộ/tập lẻ (VOD), CHỈ giữ lại
     channel phim tuyến tính. 2 điều kiện (khớp 1 trong 2 là loại):
       1. Có tag rõ ràng: "Tập \d+", "Phần \d+", "Episode \d+", "SS\d+",
          năm phát hành trong ngoặc "(2019)", "Vietsub", "Thuyết minh" —
          áp dụng cho MỌI nguồn vì đây là dấu hiệu mạnh, không phụ thuộc
          ngữ cảnh nhóm.
       2. Group-title gốc của nguồn gợi ý đây là bucket PHIM (chứa từ
          "phim") VÀ mục này KHÔNG có tvg-id VÀ tên KHÔNG khớp bất kỳ dấu
          hiệu "là 1 channel" nào (360/HTVC/SCTV/kênh/tv/channel/box/
          cine...). CHỈ áp dụng điều kiện 2 khi group thực sự gợi ý
          "phim" — TUYỆT ĐỐI KHÔNG áp dụng tràn lan cho mọi nguồn, vì sẽ
          bắt nhầm kênh địa phương không có tvg-id (đã xảy ra thật:
          "Sơn La", "Cần Thơ 1" — sửa bằng cách thu hẹp điều kiện).

  QUAN TRỌNG: nếu phát hiện phim lẻ vẫn lọt qua (ví dụ tên phim không có
  tag rõ ràng và nằm ngoài bucket "phim"), KHÔNG mở rộng điều kiện 2 áp
  dụng cho MỌI nguồn — sẽ lặp lại lỗi cũ. Thay vào đó cân nhắc thêm
  channels.yaml entry cụ thể, hoặc mở rộng `_CHANNEL_LIKE_RE`.


8. CHUẨN HOÁ TÊN KÊNH
-----------------------
`m3u/normalize.py` → `clean_display_name()`. Loại bỏ:
  - Nhãn kỹ thuật: HD, FHD, UHD, 4K, 8K, SD, 720P, 1080P, 1440P, 2160P,
    50FPS, 60FPS, HEVC, H264, H265.
  - Chú thích nguồn: [Geo-blocked], (Geo-blocked), [Not 24/7], [Backup],
    [TEST].

Ví dụ: "VTV7 HD [Geo-blocked]" → "VTV7"; "VTV5 HD 50FPS" → "VTV5".

KHÔNG động vào từ ngữ có ý nghĩa định danh kênh (ví dụ không được xoá số
kênh, tên đài, tên thương hiệu).


9. ĐỊNH DANH KÊNH (CHANNEL IDENTITY)
--------------------------------------
Một kênh KHÔNG PHẢI là 1 stream — nhiều URL từ nhiều nguồn có thể cùng là
1 kênh. Thứ tự ưu tiên xác định danh tính (`scripts/optimize_m3u.py`):

  1. Tra `m3u/channels.yaml` qua `ChannelRegistry.resolve()` — khớp theo
     tvg-id, tvg-name, hoặc tên đã chuẩn hoá với danh sách alias đã khai
     báo thủ công. Nếu khớp → dùng canonical_id/name đã định nghĩa sẵn
     (đảm bảo tên hiển thị nhất quán tuyệt đối).
  2. Nếu không khớp entry nào → `identity_key()` trong `normalize.py`:
     ưu tiên tvg-id, sau đó tvg-name, cuối cùng mới dùng tên đã chuẩn hoá
     (bỏ dấu, nối liền khoảng trắng) làm khoá.

Ví dụ "VTV1", "VTV 1", "VTV1 HD", "VTV1 FHD", "VTV1 1080P" đều phải gộp
về cùng 1 canonical_id (nhờ bước 2 dùng chung khoá sau khi qua
`clean_display_name` + `identity_key`).


10. PHÂN NHÓM (GROUPING) — NGUYÊN TẮC VÀ THỨ TỰ ƯU TIÊN
----------------------------------------------------------
Toàn bộ logic nằm trong `m3u/grouping.py` → `GroupResolver.resolve_primary_group()`.
Thứ tự kiểm tra (dừng ngay khi có kết quả):

  1. NHÓM ĐẶC BIỆT (ANTV/QPVN) — kiểm tra TRƯỚC TIÊN, ở tầng pipeline
     (`scripts/optimize_m3u.py`), KHÔNG nằm trong GroupResolver — xem mục
     11. Đây là override tuyệt đối, bỏ qua mọi group-title nguồn.

  2. group-title nguồn (nếu `trust_group_title=True` và có group-title):
     chuẩn hoá qua `group_match_key()` (bỏ dấu, chữ thường, chỉ giữ chữ/
     số), so khớp CHÍNH XÁC với `group_alias` trong `groups.yaml`.
     - NẾU kết quả là "🏠 Địa phương": phải qua GUARD bổ sung — xem mục 12.
       Không tin mù group-title nói là địa phương.
     - NẾU KHÔNG khớp bất kỳ alias nào (ví dụ nhóm mơ hồ như "Quốc Tế",
       "In The Box", "Rạp Phim", hoặc tên quốc gia) → rơi tiếp xuống các
       bước dưới, KHÔNG coi là lỗi.

  3. Fallback theo TIỀN TỐ tvg-id (`_tvg_id_brand_group()`): `htvc*` →
     HTVC, `htv*` → HTV, `vtvcab*` → VTVCab, `vtv*` → VTV, `sctv*` → SCTV.
     Dùng khi group-title của nguồn GỘP CHUNG nhiều thương hiệu (ví dụ
     TinhLaGi từng đặt chung "HTV & HTVC" cho cả 2 loại kênh) mà
     `group_alias` không tách được. tvg-id đáng tin hơn vì nó thường theo
     đúng quy ước brand-prefix xuyên suốt nhiều nguồn khác nhau.

  4. Nhận diện ĐỊA PHƯƠNG độc lập (`_is_local_province()`): khớp theo
     danh sách 63 tỉnh/thành (`local_province_keywords`), id kênh địa
     phương đã biết (`local_province_ids`), hoặc brand địa phương không
     có tên tỉnh (`local_extra_keywords`, ví dụ "HiTV"). Bắt đúng kênh địa
     phương THẬT ngay cả khi nguồn không khai group-title gì cả (tránh
     rơi vào "Khác").

  5. OTT content classifier (`_content_classify()`): so khớp từ khoá
     trong `content_keywords` (groups.yaml) với tên kênh đã chuẩn hoá.

  6. `default_content_hint` của nguồn (nếu có, ví dụ EaSport → thể thao).

  7. Cuối cùng → `config.OTHER_GROUP` ("📦 Khác") — luôn là phương án
     cuối, xem mục 14.

KHÔNG BAO GIỜ có nhóm "🌍 Quốc tế" trong output. Kênh quốc tế được phân
loại theo NỘI DUNG (BBC News → Tin tức, ESPN → Thể thao, Cartoon Network
→ Thiếu nhi...), không theo việc "là kênh nước ngoài".


11. NHÓM ĐẶC BIỆT (ANTV/QPVN)
--------------------------------
`⭐ Kênh đặc biệt` CHỈ ĐƯỢC PHÉP chứa đúng 2 canonical_id: `antv`, `qpvn`
(`config.SPECIAL_GROUP_WHITELIST`). Không kênh thứ 3 nào được phép vào
nhóm này DÙ channels.yaml có khai báo sai.

Cơ chế enforce ở 2 TẦNG (an toàn kép, không phụ thuộc 1 điểm lỗi):
  1. `scripts/optimize_m3u.py`: nếu `registry.is_special_whitelisted(canonical_id)`
     → gán thẳng `primary_group = config.SPECIAL_GROUP`, BỎ QUA hoàn toàn
     `GroupResolver` (không quan tâm nguồn nói group-title là gì).
  2. Khi áp dụng `extra_groups` từ channels.yaml cho các kênh KHÁC: nếu
     entry nào lỡ khai `extra_groups: ["⭐ Kênh đặc biệt"]` mà canonical_id
     không nằm trong whitelist → bị BỎ QUA (không thêm vào groups_for_entry).

ANTV/QPVN cũng xuất hiện ĐỒNG THỜI ở "📰 Tin tức" qua `extra_groups` khai
trong `channels.yaml` (multi-group membership, xem mục 13).


12. NHÓM ĐỊA PHƯƠNG — VÌ SAO CẦN "GUARD" NGHIÊM NGẶT
--------------------------------------------------------
Đã xảy ra thật trong sản xuất: một số nguồn gán NHẦM group-title "Địa
Phương" cho các kênh hoàn toàn không liên quan (CNN, Cartoon Network,
CCTV4, thậm chí kênh tiếng Nga). Nếu tin mù group-title, nhóm Địa phương
sẽ bị ô nhiễm nặng.

Giải pháp: `GroupResolver._confirm_local()` — khi group-title nguồn nói
là "Địa phương", KHÔNG chấp nhận ngay mà kiểm chứng lại theo thứ tự:

  1. Khớp danh sách tỉnh/thành/id đã biết (`_is_local_province`) → XÁC
     NHẬN là địa phương, chấp nhận ngay.
  2. Có ký tự Cyrillic (chữ Nga/vùng Trung Á) trong tên → CHẮC CHẮN KHÔNG
     phải kênh địa phương Việt Nam, TỪ CHỐI ngay (bất kể còn gì khác).
  3. KHÔNG có dấu tiếng Việt nào trong tên gốc (trước khi bỏ dấu) → nghi
     ngờ là kênh nước ngoài không xác định được thể loại, TỪ CHỐI (an
     toàn hơn là nhận liều).
  4. Còn lại (có dấu tiếng Việt, không khớp Cyrillic) → CHẤP NHẬN là địa
     phương — đây là trường hợp kênh Việt Nam thật nhưng tên đài chưa kịp
     liệt kê vào danh sách tỉnh/thành (ví dụ biến thể tên viết tắt mới).

Khi bị TỪ CHỐI ở bước 2/3, kênh đó vẫn được thử qua content classifier
(`_content_classify`) trước khi rơi vào "Khác" — không mất trắng, chỉ
không được gắn nhầm nhãn địa phương.

NẾU PHÁT HIỆN THÊM TRƯỜNG HỢP KHÔNG NHẬN ĐÚNG (ví dụ kênh địa phương thật
nhưng không có dấu tiếng Việt trong tên hiển thị, như dùng toàn chữ viết
tắt không dấu): thêm vào `local_extra_keywords` trong `groups.yaml`, ĐỪNG
nới lỏng logic Cyrillic/diacritic-check chung — nới lỏng sẽ mở lại lỗ
hổng ban đầu.


13. MULTI-GROUP MEMBERSHIP
----------------------------
Một kênh được phép thuộc NHIỀU nhóm cùng lúc. Khai báo qua
`channels.yaml` → `extra_groups` (cộng thêm vào nhóm chính đã tính ở mục
10), ví dụ:

    vtv7:
      extra_groups: ["👶 Thiếu nhi", "🎓 Giáo dục & Khám phá"]

Ví dụ khác: ANTV/QPVN → nhóm chính "⭐ Kênh đặc biệt" + extra "📰 Tin tức".
1 kênh thể thao của VTVCab → nhóm chính "📡 VTVCab" + extra "⚽ Thể thao"
(nếu cần, thêm entry channels.yaml tương ứng).

Kênh xuất hiện ở nhiều nhóm dùng CHUNG 1 danh sách stream đã dedup — không
lặp lại việc dedup/sắp xếp cho từng nhóm riêng.


14. NHÓM "KHÁC" — LUÔN LÀ PHƯƠNG ÁN CUỐI
--------------------------------------------
`📦 Khác` chỉ nên NHỎ DẦN qua mỗi lần cải tiến, không được phép LỚN LÊN.
Một kênh KHÔNG được xếp vào Khác chỉ vì: là kênh quốc tế, là kênh phim, là
kênh thể thao, là kênh địa phương, hay thuộc 1 nhóm đặc thù của nguồn.

`build_stats.json` → `khac_channel_list` liệt kê MỌI kênh rơi vào Khác
(chỉ thuộc đúng 1 nhóm là Khác), kèm nguồn gốc, để dễ tra và bổ sung quy
tắc phân loại tốt hơn ở lần sau — xem mục 19.


15. CHỌN STREAM — KHÔNG CÒN PRIMARY/BACKUP
----------------------------------------------
(Xem lý do đầy đủ ở mục 2.2). Quy trình thực tế trong
`scripts/optimize_m3u.py`:

  1. Gom mọi candidate URL cho 1 canonical_id từ mọi nguồn.
  2. Dedup theo URL (giữ bản ghi đầu tiên gặp — do SOURCES đã theo đúng
     thứ tự priority nên bản đầu tiên tự nhiên có priority cao nhất).
  3. Sắp xếp theo `(source_priority tăng dần, quality_score giảm dần)`.
     `quality_score` chỉ là TIE-BREAKER PHỤ (đọc số độ phân giải nếu có
     trong tên/URL: 2160/1440/1080/720/576/480) — priority nguồn vẫn quan
     trọng hơn.
  4. Giữ tối đa `config.ALT_STREAM_SOFT_CAP` (mặc định 6) bản ghi — CHỈ
     để chống phình to bất thường khi 1 kênh xuất hiện ở quá nhiều nguồn,
     KHÔNG phải phân tầng chất lượng.
  5. Ghi MỖI URL còn lại thành 1 dòng `#EXTINF` riêng, dùng CHUNG 1 TÊN
     KÊNH — KHÔNG thêm hậu tố "[Dự phòng]" hay bất kỳ nhãn thứ hạng nào.

Các thẻ `#EXTVLCOPT`/`#KODIPROP` đi kèm mỗi URL PHẢI được giữ nguyên và
ghi đúng ngay trước URL tương ứng của nó (không được lẫn giữa các URL
khác nhau) — nhiều stream cần user-agent riêng hoặc DRM key riêng để phát
được.


16. LOGO
--------
`m3u/logo.py` → `choose_logo()`. Ưu tiên:
  1. Logo do CHÍNH nguồn cung cấp, theo đúng SOURCE PRIORITY (nguồn ưu
     tiên cao hơn thắng nếu có logo).
  2. IPTV-org (`m3u/iptvorg.py` → `load_iptvorg_logo_fallback()`) CHỈ dùng
     khi KHÔNG nguồn nào có logo — không bao giờ ghi đè logo đã có từ
     nguồn ưu tiên cao hơn.

Lỗi mạng khi fetch iptv-org logos.json KHÔNG được làm sập build — trả về
dict rỗng và tiếp tục.


17. OUTPUT FORMAT
------------------
  - Dòng đầu: `#EXTM3U url-tvg="https://lichphatsong.io.vn/epg.xml"`.
  - Nhóm xuất theo đúng thứ tự `config.FINAL_GROUP_ORDER` (17 nhóm, xem
    file `config.py`).
  - Trong mỗi nhóm: sắp theo tên kênh, so sánh "tự nhiên" (tách số ra so
    sánh dạng int để "VTV2" đứng trước "VTV10", không theo thứ tự chữ cái
    thuần tuý).
  - Mỗi kênh: `tvg-id` (nếu có), `tvg-logo` (nếu có), `group-title`, tên
    kênh SẠCH (đã qua `clean_display_name`, không thêm nhãn kỹ thuật/thứ
    hạng nào).
  - File cuối cùng: `listtivi.m3u` tại repo root.


18. QUALITY GATE
-----------------
`m3u/quality_gate.py` → `validate_output()`. Chạy TRƯỚC KHI ghi đè
`listtivi.m3u`. Kiểm tra tối thiểu:
  - Có dòng `#EXTM3U`.
  - Có ít nhất 1 dòng `#EXTINF`.
  - Số kênh đạt ngưỡng tối thiểu (`config.MIN_CHANNEL_COUNT`, mặc định
    50) — tránh ghi đè bằng 1 bản build gần như rỗng do lỗi mạng/nguồn.
  - Nhóm đặc biệt CHỈ chứa đúng ANTV/QPVN.
  - KHÔNG tồn tại nhóm "Quốc tế" nào.
  - KHÔNG có domain bị chặn (`config.BLOCKED_DOMAINS`) lọt vào output.

NẾU gate THẤT BẠI: KHÔNG ghi file `listtivi.m3u` (giữ nguyên bản cũ), vẫn
ghi `build_stats.json` với `gate_passed: false` và lý do cụ thể, và
`sys.exit(1)` để workflow GitHub Actions hiển thị rõ trạng thái lỗi.


19. build_stats.json
---------------------
Ghi tại `m3u/cache/build_stats.json` sau MỖI lần chạy (kể cả khi gate
thất bại), gồm:
  - `gate_passed`, `gate_reasons`
  - `source_item_counts`, `source_errors` (theo từng nguồn)
  - `filtered_counts` (theo từng LÝ DO lọc — xem mục 7)
  - `canonical_channel_count`, `channel_count_with_stream`
  - `total_alt_streams` (tổng số dòng EXTINF, không còn primary/backup)
  - `channels_per_group`
  - `khac_channel_list` (để rà soát và bổ sung quy tắc phân loại)
  - `epg_status`

Đây là công cụ CHẨN ĐOÁN chính khi có báo cáo lỗi ("thiếu kênh X", "nhóm Y
sai") — luôn kiểm tra file này trước khi sửa code.


20. GITHUB ACTIONS
-------------------
Workflow tối thiểu:

    - uses: actions/checkout@v5
    - uses: actions/setup-python@v6
      with: { python-version: '3.12' }
    - run: python -m pip install -r m3u/requirements.txt
    - run: python3 scripts/optimize_m3u.py

KHÔNG dùng `path: source-repo` / `working-directory: source-repo` — script
chạy trực tiếp từ repo root.

Nếu có bước deploy sang repo Pages riêng (ví dụ `vhd0.github.io`), đó là
bước RIÊNG BIỆT, không thuộc phạm vi thiết kế pipeline này — chỉ copy
`listtivi.m3u` sau khi build xong.


21. CÁC BẤT BIẾN (INVARIANTS) CẦN GIỮ NGUYÊN KHI VIẾT LẠI
--------------------------------------------------------------
  A. group-title nguồn là bằng chứng phân loại CHÍNH khi nguồn đáng tin.
  B. OTT taxonomy (content_keywords) là bằng chứng phân loại PHỤ.
  C. Từ khoá tên kênh KHÔNG BAO GIỜ trở thành bộ lọc CỨNG mạnh tay (chỉ
     lọc từ khoá có độ tin cậy cao: người lớn/cờ bạc/test-demo/info-card).
  D. Tên hiển thị luôn bỏ nhãn kỹ thuật/chú thích nguồn.
  E. 1 kênh có thể thuộc nhiều nhóm.
  F. Nhóm đặc biệt = CHỈ ANTV + QPVN, enforce ở tầng pipeline, không phụ
     thuộc GroupResolver.
  G. Địa phương LUÔN cần "guard" xác nhận trước khi tin group-title nói
     là địa phương (mục 12) — KHÔNG tin mù.
  H. KHÔNG BAO GIỜ có nhóm output "Quốc tế".
  I. Khác là phương án CUỐI CÙNG, chỉ nên nhỏ dần.
  J. KHÔNG healthcheck. KHÔNG primary/backup. Chỉ dedup + soft-cap.
  K. Thẻ #EXTVLCOPT/#KODIPROP luôn đi kèm đúng URL của nó.
  L. Logo: ưu tiên nguồn, iptv-org chỉ fallback.
  M. EPG = lichphatsong.io.vn/epg.xml, lỗi EPG không được xoá kênh nào.
  N. Quality gate bảo vệ bản build tốt cuối cùng — không bao giờ ghi đè
     bằng bản build rõ ràng bị lỗi/rỗng.
  O. Parser LUÔN tự nhận diện định dạng thật từ nội dung, không tin tên
     file/config khai báo.
  P. Chỉ chặn theo TÊN THƯƠNG HIỆU cụ thể đã xác minh (vd "Socolive"),
     KHÔNG chặn theo từ khoá nội dung chung chung.


22. VÍ DỤ CHẤP NHẬN (ACCEPTANCE EXAMPLES)
--------------------------------------------
Các trường hợp sau PHẢI cho kết quả đúng như mô tả:

    VTV7 HD [Geo-blocked]
      -> tên hiển thị: VTV7
      -> nhóm: VTV + Thiếu nhi + Giáo dục & Khám phá (qua channels.yaml)

    ANTV (bất kể group-title nguồn nói gì)
      -> Kênh đặc biệt + Tin tức

    QPVN (bất kể group-title nguồn nói gì)
      -> Kênh đặc biệt + Tin tức

    SCTV1 HD, group-title="SCTV"
      -> SCTV (PHẢI xuất hiện trong output — không được biến mất vì bất
         kỳ lý do "kiểm tra sức khoẻ" nào, vì healthcheck đã bị bỏ)

    HTVC Phim HD, group-title="HTV & HTVC" (nguồn gộp chung 2 thương hiệu)
      -> HTVC (nhờ fallback tvg-id prefix "htvc")

    CNN, group-title="Địa Phương" (nguồn gán nhầm)
      -> Tin tức, TUYỆT ĐỐI KHÔNG phải Địa phương

    "7 канал (Красноярск)", group-title="Địa Phương" (nguồn gán nhầm,
    tiếng Nga)
      -> Khác, TUYỆT ĐỐI KHÔNG phải Địa phương (guard Cyrillic)

    Sơn La, Cần Thơ 1 (không có tvg-id, group-title không rõ ràng)
      -> Địa phương (nhờ local_province_keywords, KHÔNG bị lọc VOD)

    "Tử Chiến Trên Không", group-title="Rạp Phim", không tvg-id
      -> BỊ LOẠI (VOD/phim lẻ), không xuất hiện trong output

    "360 Phim Việt", group-title="Rạp Phim", không tvg-id
      -> Phim (GIỮ LẠI — có dấu hiệu "360" nhận là channel-like)

    Nhóm "Socolive" (nhiều URL trùng 1 trận đấu, khác biệt danh BLV)
      -> BỊ LOẠI HOÀN TOÀN (blocked_stream_brand_group)

    Kênh trỏ URL .../logo.jpg
      -> BỊ LOẠI (non_stream_url), không phải 1 kênh thật
