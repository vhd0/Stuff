import requests
from datetime import datetime

# DANH SÁCH URL NỀN TẢNG SẠCH
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
# BỘ LỌC THẨM MỸ ĐÃ SỬA LỖI CÚ PHÁP & TỐI ƯU HÓA DIỆN RỘNG (PURE COSMETIC RULES)
# ==============================================================================
CUSTOM_RULES = [
    # --- 1. Khôi phục & Tối ưu hóa các phần tử thực tế bạn bắt được (Tuyệt đối không dùng :collapse) ---
    "##.catfish-top",
    "##.catfish-bottom",
    "##[id*='-adx']",                        # Quét sạch cả vl-top-adx, vl-underplayer-adx, v.v.
    "##.banner-preload",
    "##.banner-preload-container",
    "##a.bna",

    # --- 2. Đặc trị lớp phủ tàng hình bắt Click & Popup đè màn hình (HBET, MAN88, Lu88) ---
    "##div[class*='popup-ads']",
    "##div[class*='popup-banner']",
    "##div[id*='popup-ad']",
    "##div[class*='modal-ads']",
    "##div[class*='adv-popup']",
    "##.ads-overlay",
    "##.popup-overlay",
    "##[id^='popup-overlay']",
    "##div[class*='player-overlay']",
    "##div[id*='player-overlay']",
    "##div[class*='click-overlay']",
    "##div[class*='popunder']",
    "##div[id*='popunder']",
    "##div[class*='wrapper-click']",
    "##div[class*='video-ads']",
    "##div[class*='player-ads']",
    
    # --- 3. Đánh chặn hoán vị thuộc tính CSS của các Hộp chứa ẩn danh cấp gốc (Root DIVs) ---
    "##div[style*='position: fixed'][style*='z-index']",
    "##div[style*='position:fixed'][style*='z-index']",
    "##div[style*='z-index'][style*='position: fixed']",
    "##div[style*='z-index'][style*='position:fixed']",
    "##div[style*='position: fixed'][style*='top'][style*='left']",
    "##div[style*='position: fixed'][style*='bottom'][style*='left']",
    "##div[style*='width: 100%'][style*='height: 100%'][style*='position: fixed']",
    "##div[style*='width:100%'][style*='height:100%'][style*='position:fixed']",
    
    # --- 4. Hệ thống quét cấu trúc Banner chuẩn hóa toàn cầu ---
    "##.ad-placement",
    "##.ad-slot",
    "##.ad-holder",
    "##[class^='ad-banner']",
    "##[id^='ad-banner']",
    "##[class*='ad_banner']",
    "##.banner-ads",
    "##.banner_ad"
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
            
    # TIÊM BỘ LỌC ĐÃ ĐƯỢC VÁ LỖI CÚ PHÁP
    print(f"-> Đang nạp {len(CUSTOM_RULES)} quy tắc Thẩm mỹ chuẩn hóa (Fixed Syntax)...")
    for rule in CUSTOM_RULES:
        if rule not in seen_rules:
            seen_rules.add(rule)
            merged_rules.append(rule)
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (Fixed Cosmetic Edition)
! Description: Bộ lọc tổng hợp đã sửa đổi toàn bộ lỗi cú pháp bổ trợ, tối ưu hóa triệt để cấu trúc banner lậu.
! Version: 14.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for rule in rules:
            f.write(rule + "\n")
        
    print(f"🎉 Thành công! Đã sửa lỗi và xuất lại file abp.txt chuẩn với {len(rules)} quy tắc.")

if __name__ == "__main__":
    rules = fetch_and_merge_pure()
    write_pure_filter(rules)
