import requests
from datetime import datetime

# DANH SÁCH URL NỀN TẢNG CHUẨN
URLS = [
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    "https://raw.githubusercontent.com/bigdargon/hostsVN/refs/heads/master/filters/adservers-all.txt",
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.txt",        
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt",        
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_3_Spyware/filter.txt",     
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_14_Annoyances/filter.txt", 
    "https://filters.adtidy.org/extension/chromium/filters/11.txt",                                                 
    "https://filters.adtidy.org/extension/chromium/filters/23.txt",                                                 
    "https://easylist.to/easylist/easylist.txt" 
]

# ==============================================================================
# BỘ LỌC THẨM MỸ CAO CẤP - ĐẶC TRỊ LỚP PHỦ TÀNG HÌNH & CLICK-CATCHER
# ==============================================================================
CUSTOM_RULES = [
    # --- 1. Vũ khí mới: Tối ưu hóa từ phần tử bạn vừa bắt được xung quanh Video Player ---
    "##[id*='underplayer-adx']",
    "##[id*='top-adx']",
    "##.catfish-top",
    "##.catfish-bottom",
    "##.banner-preload",
    "##.banner-preload-container",
    "##a.bna",

    # --- 2. Triệt hạ các Lớp phủ tàng hình bắt Click (Invisible Overlays / Popunders) ---
    "##div[class*='player-overlay']",
    "##div[id*='player-overlay']",
    "##div[class*='click-overlay']",
    "##div[class*='popunder']",
    "##div[id*='popunder']",
    "##div[class*='wrapper-click']",
    "##div[class*='video-ads']",
    "##div[class*='player-ads']",
    "##div[id*='player-ad']",
    "##.ads-overlay",
    "##.popup-overlay",
    "##[id^='popup-overlay']",
    
    # --- 3. Hệ thống phòng thủ diện rộng chống Banner cứng đầu ---
    "##.ad-placement",
    "##.ad-slot",
    "##.ad-holder",
    "##[class^='ad-banner']",
    "##[id^='ad-banner']",
    "##[class*='ad_banner']",
    "##.banner-ads",
    "##.banner_ad",
    "##div[class*='sticky-ad']",
    "##div[id*='sticky-ad']",
    "##div[class*='floating-ad']",
    
    # --- 4. Khóa cấu trúc CSS cố định chứa liên kết bẩn ---
    "##div[style*='position: fixed'][style*='z-index'] > a > img",
    "##div[style*='position:fixed'][style*='z-index'] a img",
    
    # --- 5. Ép thu gọn diện tích bị chiếm dụng ẩn danh (:collapse) ---
    "##[id*='underplayer-adx']:collapse",
    "##.catfish-top:collapse",
    "##.catfish-bottom:collapse",
    "##.banner-preload:collapse"
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
            
    # TIÊM BỘ LỌC ĐẶC TRỊ LỚP PHỦ VÀO CUỐI FILE
    print(f"-> Đang nạp {len(CUSTOM_RULES)} quy tắc Thẩm mỹ bẻ gãy Overlay & Click-Catcher...")
    for rule in CUSTOM_RULES:
        if rule not in seen_rules:
            seen_rules.add(rule)
            merged_rules.append(rule)
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (Anti-Overlay Edition)
! Description: Bộ lọc tổng hợp thuần khiết. Đã tích hợp lõi diệt lớp phủ tàng hình bọc Video Player và Anti-Popunder.
! Version: 12.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for rule in rules:
            f.write(rule + "\n")
        
    print(f"🎉 Thành công! Đã xuất file abp.txt chống overlay với tổng cộng {len(rules)} quy tắc.")

if __name__ == "__main__":
    rules = fetch_and_merge_pure()
    write_pure_filter(rules)
