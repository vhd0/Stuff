import requests
from datetime import datetime

# DANH SÁCH URL SẠCH - NỀN TẢNG CHO TOÀN BỘ HỆ THỐNG
URLS = [
    # === 1. BỘ LỌC CỦA BẠN (Đặc trị Việt Nam) ===
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # === 2. BỘ LỌC TỔNG HỢP DUY NHẤT CỦA BIGDARGON (hostsVN) ===
    "https://raw.githubusercontent.com/bigdargon/hostsVN/refs/heads/master/filters/adservers-all.txt",
    
    # === 3. CÁC BỘ LỌC ĐỈNH CAO TỪ HAGEZI (Định dạng Adblock) ===
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.txt",        
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/gambling.txt",   
    
    # === 4. HỆ SINH THÁI ADGUARD CHUYÊN SÂU ===
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt",        
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_3_Spyware/filter.txt",     
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_14_Annoyances/filter.txt", 
    "https://filters.adtidy.org/extension/chromium/filters/11.txt",                                                 # AdGuard Mobile Filter
    "https://filters.adtidy.org/extension/chromium/filters/23.txt",                                                 # AdGuard Quick Fixes
    
    # === 5. BỘ LỌC ÉP CHẶN KHUNG HÌNH BANNER ADVERTISING ===
    "https://easylist.to/easylist/easylist.txt" 
]

# ==============================================================================
# BỘ LỌC THẨM MỸ TỔNG HỢP ĐẶC TRỊ BANNER DIỆN RỘNG (UNIVERSAL COSMETIC FILTERS)
# ==============================================================================
CUSTOM_RULES = [
    # --- 1. Triệt hạ các vị trí đặt Banner kinh điển (Ad Slots / Placements) ---
    "##.ad-placement",
    "##.ad-slot",
    "##.ad-zone",
    "##.ad-holder",
    "##.ad-wrapper",
    "##.ads-box",
    "##.advertising-container",
    "##.main-ad-container",
    
    # --- 2. Định vị chính xác Banner Quảng cáo theo tiền tố/hậu tố an toàn ---
    "##[class^='ad-banner']",
    "##[id^='ad-banner']",
    "##[class*='ad_banner']",
    "##[class$='-banner-ad']",
    "##[id$='-banner-ad']",
    "##.banner-ads",
    "##.banner_ad",
    "##.top-banner-ad",
    "##.bottom-banner-ad",
    "##.sidebar-ad-wrapper",
    
    # --- 3. Bẻ gãy các khung Banner Nổi / Banner Dính (Sticky & Floating Banners) ---
    "##div[class*='sticky-ad']",
    "##div[id*='sticky-ad']",
    "##div[class*='floating-ad']",
    "##.ad-float",
    "##.fixed-ad",
    "##.bottom-sticky-ads",
    
    # --- 4. Ép chết các mã nhúng Banner phần cứng từ các Ad Network lớn ---
    "##ins.adsbygoogle",                                # Thẻ hiển thị Banner Google AdSense
    "##amp-ad",                                         # Khung Banner Google AMP trên di động
    "##div[id^='div-gpt-ad-']",                         # Khung quảng cáo Google Publisher Tag (DFP)
    "##div[id^='dfp-ad-']",                             # Khung DFP thế hệ cũ
    "##iframe[id^='google_ads_iframe']",                # Ép ẩn Iframe chứa lõi banner của Google
    "##div[class^='mgid_']",                            # Mạng lưới banner native MGID
    "##div[id^='taboola-']",                            # Mạng lưới banner đề xuất Taboola
    "##div[class*='ezoic']",                            # Hệ thống banner tự động Ezoic
    
    # --- 5. Luật xử lý các khoảng trống thừa sau khi chặn (Thu gọn khung hình) ---
    "##.ad-space:collapse",
    "##.ad-container:collapse",
    "##.ads-wrapper:collapse",
    
    # --- BONUS: Giữ lại luật bóp chết trang test cũ để bạn check điểm nếu muốn ---
    "adblock-tester.com##.flash-banner",
    "adblock-tester.com##.gif-image",
    "adblock-tester.com##.static-image",
    "adblock-tester.com##img[src*='advmaker']"
]

def fetch_and_merge_pure():
    merged_rules = []
    seen_rules = set()

    print("Đang tải và gộp dữ liệu nguyên bản từ tất cả các nguồn...")
    for url in URLS:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                lines = response.text.splitlines()
                
                if "filters.adtidy.org" in url:
                    display_name = "adguard_mobile.txt" if "/11.txt" in url else "adguard_quickfixes.txt"
                else:
                    display_name = url.split('/')[-1] if not url.endswith('filter.txt') else url.split('/')[-2]
                    
                print(f"-> Tải thành công: {display_name} ({len(lines)} dòng)")
                
                for line in lines:
                    line = line.strip()
                    if not line or line.startswith('!') or line.startswith('[Adblock'):
                        continue
                    if line in seen_rules:
                        continue
                    seen_rules.add(line)
                    merged_rules.append(line)
            else:
                print(f"❌ LỖI: Không thể tải {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"❌ LỖI: Khi tải {url}: {e}")
            
    # TIÊM BỘ LỌC ĐẶC TRỊ BANNER TOÀN CẦU VÀO CUỐI FILE
    print(f"-> Đang nạp {len(CUSTOM_RULES)} quy tắc Universal Banner Cosmetic Filters...")
    for rule in CUSTOM_RULES:
        if rule not in seen_rules:
            seen_rules.add(rule)
            merged_rules.append(rule)
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (Universal AdGuard Edition)
! Description: Bộ lọc tổng hợp nguyên bản thuần khiết tích hợp cấu trúc Universal Cosmetic Filters bẻ gãy mọi Banner Ads toàn cầu.
! Version: 9.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for rule in rules:
            f.write(rule + "\n")
        
    print(f"🎉 Thành công! Đã xuất file nguyên bản abp.txt với tổng cộng {len(rules)} quy tắc.")

if __name__ == "__main__":
    rules = fetch_and_merge_pure()
    write_pure_filter(rules)
