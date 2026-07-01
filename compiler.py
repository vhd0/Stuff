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

# ĐƯỜNG DẪN ĐẾN FILE CUSTOM RULES TỪ XA CỦA BẠN
CUSTOM_RULES_URL = "https://raw.githubusercontent.com/vhd0/Stuff/refs/heads/main/customrules.txt"

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
                    if not line or line.startswith('!') or line.startswith('# ') or line.startswith('[Adblock'):
                        continue
                    if line in seen_rules:
                        continue
                    seen_rules.add(line)
                    merged_rules.append(line)
            else:
                print(f"❌ LỖI: Không thể tải {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"❌ LỖI: Khi tải {url}: {e}")
            
    # TIẾN HÀNH TẢI CÁC QUY TẮC TÙY CHỈNH TỪ GITHUB CỦA BẠN
    print(f"\n-> Đang đồng bộ quy tắc Custom từ xa: {CUSTOM_RULES_URL.split('/')[-1]}...")
    try:
        response = requests.get(CUSTOM_RULES_URL, timeout=30)
        if response.status_code == 200:
            custom_lines = response.text.splitlines()
            count = 0
            for line in custom_lines:
                line = line.strip()
                if not line or line.startswith('!') or line.startswith('# ') or line.startswith('[Adblock'):
                    continue
                if line in seen_rules:
                    continue
                seen_rules.add(line)
                merged_rules.append(line)
                count += 1
            print(f"🎉 Nạp thành công {count} quy tắc Custom đặc trị từ dữ liệu đám mây.")
        else:
            print(f"❌ LỖI: Không thể kết nối tới file Custom Rules (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ LỖI: Trong quá trình xử lý Custom Rules: {e}")
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (Decoupled Cloud Edition)
! Description: Bộ lọc tổng hợp tối ưu diện rộng. Logic xử lý Python tách biệt hoàn toàn với Custom Rules.
! Version: 17.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for rule in rules:
            f.write(rule + "\n")
        
    print(f"\n🎉 Thành công! File đầu ra abp.txt đã sẵn sàng với tổng cộng {len(rules)} quy tắc.")

if __name__ == "__main__":
    rules = fetch_and_merge_pure()
    write_pure_filter(rules)
