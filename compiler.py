import requests
import re
from datetime import datetime

# Danh sách các nguồn dữ liệu raw
URLS = [
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_3_Spyware/filter.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_17_TrackParam/filter.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_4_Social/filter.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_14_Annoyances/filter.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_11_Mobile/filter.txt",
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt"
]

def fetch_and_parse():
    network_rules = set()
    cosmetic_rules = set()
    exception_rules = set()
    
    print("Đang tải dữ liệu từ các nguồn...")
    for url in URLS:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                lines = response.text.splitlines()
                for line in lines:
                    line = line.strip()
                    
                    # Bỏ các dòng trống, comment hoặc header cũ
                    if not line or line.startswith('!') or line.startswith('[Adblock'):
                        continue
                        
                    # Phân loại dựa trên cú pháp tối ưu trong file abp.json
                    if line.startswith('@@'):
                        exception_rules.add(line)
                    elif '##' in line or '#@#' in line:
                        # Nếu chứa #@# thì nó là ngoại lệ của cosmetic, cho vào exception_rules
                        if '#@#' in line:
                            exception_rules.add(line)
                        else:
                            cosmetic_rules.add(line)
                    else:
                        network_rules.add(line)
            else:
                print(f"Không thể tải: {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"Lỗi khi tải {url}: {e}")
            
    return sorted(list(network_rules)), sorted(list(cosmetic_rules)), sorted(list(exception_rules))

def write_filter_file(network, cosmetic, exception):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    header = f"""[Adblock Plus 2.0]
! Title: My Custom Adblock Filter List
! Description: Bộ lọc tổng hợp tối ưu hóa từ AdGuard và ABPVN.
! Version: 1.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0_/my-adblock-filter
"""

    with open("my_custom_filter.txt", "w", encoding="utf-8") as f:
        # 1. Header & Metadata
        f.write(header + "\n")
        
        # 2. Network Filters
        f.write("! === SECTION 1: NETWORK FILTERS ===\n")
        for rule in network:
            f.write(rule + "\n")
        f.write("\n")
        
        # 3. Cosmetic Filters
        f.write("! === SECTION 2: COSMETIC FILTERS / ELEMENT HIDING ===\n")
        for rule in cosmetic:
            f.write(rule + "\n")
        f.write("\n")
        
        # 4. Exception Rules
        f.write("! === SECTION 3: EXCEPTION RULES / WHITELISTING ===\n")
        for rule in exception:
            f.write(rule + "\n")
        f.write("\n")
        
    print(f"Đã tạo thành công file my_custom_filter.txt với {len(network)} luật mạng, {len(cosmetic)} luật giao diện và {len(exception)} luật ngoại lệ.")

if __name__ == "__main__":
    net, cos, exc = fetch_and_parse()
    write_filter_file(net, cos, exc)
