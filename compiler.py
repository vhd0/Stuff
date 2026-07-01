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
# BỘ LỌC ĐẶC TRỊ KHÔNG GIAN QUẢNG CÁO VIETADX & POPUNDER TÀNG HÌNH (GENERIC)
# ==============================================================================
CUSTOM_RULES = [
    # --- 1. CHẶN MẠNG (NETWORK RULE): Khóa nguồn cấp Script tạo quảng cáo ---
    "||vietadx.net^",                       # Bẻ gãy v-main-adx.js và v-top-adx.js ngay từ vòng gửi xe

    # --- 2. THẨM MỸ (COSMETIC): Triệt hạ các khung chứa Banner (Catfish & Preload) ---
    "##.catfish-top",
    "##.catfish-bottom",
    "##.banner-catfish-top",
    "##.banner-catfish-bottom",
    "##[class*='catfish-']",                 # Gom sạch mọi biến thể class có chứa từ khóa catfish
    "##.banner-preload",
    "##.banner-preload-container",
    "##.banner-preload-close",
    "##.catfish-top-close",
    "##.catfish-bottom-close",

    # --- 3. ĐẶC TRỊ POPUNDER: Vô hiệu hóa bẫy click tàng hình (1x1px / opacity:0) ---
    "##a[id^='bb'][onclick]",                # Diệt các thẻ <a> có id bắt đầu bằng 'bb' và chứa lệnh click (bb0, bb1)
    "##a[onclick^='oc()']",                  # Chặn đứng hàm kích hoạt mở tab ngầm oc() của hệ thống này
    "##a[style*='opacity:0'][style*='width:1px']",   # Lọc tất cả các thẻ liên kết cố tình ẩn tàng hình 1px
    "##a[style*='opacity:0'][style*='width: 1px']",  # Dự phòng trường hợp có khoảng trắng trong CSS inline

    # --- 4. Hệ thống phòng thủ chiều sâu phòng khi Script đổi tên Class ---
    "##[id*='-adx']",
    "##div[style*='position: fixed'][style*='z-index']",
    "##div[style*='position:fixed'][style*='z-index']",
    "##div[style*='z-index'][style*='position: fixed']",
    "##div[style*='z-index'][style*='position:fixed']"
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
            
    # TIÊM BỘ LỌC ĐẶC TRỊ TOÀN DIỆN VÀO CUỐI FILE
    print(f"-> Đang nạp {len(CUSTOM_RULES)} quy tắc Đặc trị cấu trúc mã VietAdx...")
    for rule in CUSTOM_RULES:
        if rule not in seen_rules:
            seen_rules.add(rule)
            merged_rules.append(rule)
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (Anti-VietAdx Edition)
! Description: Bộ lọc tối ưu hóa triệt tiêu hoàn toàn mã nhúng mạng lưới VietAdx và các bẫy click ngầm Popunder.
! Version: 15.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for rule in rules:
            f.write(rule + "\n")
        
    print(f"🎉 Thành công! Đã xuất file abp.txt tối ưu hóa diện rộng với tổng cộng {len(rules)} quy tắc.")

if __name__ == "__main__":
    rules = fetch_and_merge_pure()
    write_pure_filter(rules)
