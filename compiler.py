import requests
from datetime import datetime

# DANH SÁCH URL SẠCH - ĐÃ LOẠI BỎ HAGEZI GAMBLING THEO YÊU CẦU
URLS = [
    # === 1. BỘ LỌC CỦA BẠN (Đặc trị Việt Nam) ===
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # === 2. BỘ LỌC TỔNG HỢP DUY NHẤT CỦA BIGDARGON (hostsVN) ===
    "https://raw.githubusercontent.com/bigdargon/hostsVN/refs/heads/master/filters/adservers-all.txt",
    
    # === 3. BỘ LỌC ĐỈNH CAO TỪ HAGEZI (Định dạng Adblock) ===
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.txt",        
    
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
# BỘ LỌC ĐẶC TRỊ POPUP & STACK BANNER CỜ BẠC TRÊN WEB LẬU VIỆT NAM (COSMETIC)
# ==============================================================================
CUSTOM_RULES = [
    # --- 1. Ép ẩn khung Banner đè giữa màn hình (Popup Interstitials như tấm HBET) ---
    "##div[class*='popup-ads']",
    "##div[class*='popup-banner']",
    "##div[id*='popup-ad']",
    "##div[class*='modal-ads']",
    "##div[class*='adv-popup']",
    "##.ads-overlay",
    "##.popup-overlay",
    "##[id^='popup-overlay']",
    
    # --- 2. Quét sạch các chuỗi Banner xếp chồng dính ở đáy/đỉnh (Catfish / Sticky Ads như cụm MAN88) ---
    "##.catfish-ad",
    "##.sticky-bottom-ads",
    "##.sticky-top-ads",
    "##div[class*='bottom-bar-ads']",
    "##div[class*='top-bar-ads']",
    "##div[class*='floating-banner']",
    "##div[id*='floating-banner']",
    "##[class*='sticky-banner']",
    "##div[class*='ads-desktop']",
    "##div[class*='ads-mobile']",
    "##div[class*='floating-left']",
    "##div[class*='floating-right']",
    
    # --- 3. Nhắm mục tiêu cấu trúc CSS ép vị trí (Fixed/Absolute) chứa liên kết ảnh bẩn ---
    "##div[style*='position: fixed'][style*='z-index'] > a > img",
    "##div[style*='position:fixed'][style*='z-index'] a img",
    
    # --- 4. Thu gọn triệt để vùng không gian bị chiếm dụng sau khi ẩn khung ---
    "##div[class*='popup-']:collapse",
    "##div[class*='sticky-']:collapse",
    "##div[class*='ads-']:collapse"
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
            
    # TIÊM BỘ LỌC ĐẶC TRỊ POPUP CỜ BẠC HTML VÀO CUỐI FILE
    print(f"-> Đang nạp {len(CUSTOM_RULES)} quy tắc Đặc trị HTML Popups Web lậu...")
    for rule in CUSTOM_RULES:
        if rule not in seen_rules:
            seen_rules.add(rule)
            merged_rules.append(rule)
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (Anti-Popup Edition)
! Description: Bộ lọc tổng hợp loại bỏ hoàn toàn Gambling, tập trung Cosmetic Filter bẻ gãy các lớp đè HTML Popup/Sticky lậu.
! Version: 10.0.{datetime.utcnow().strftime('%Y%m%d')}
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
