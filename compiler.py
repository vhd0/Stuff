import requests
import re
from datetime import datetime

# Danh sách đầy đủ và mạnh mẽ nhất từ cộng đồng + ABPVN
URLS = [
    # 1. Bộ lọc của bạn (Đặc trị Việt Nam)
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # 2. Bộ lọc quảng cáo CORE toàn diện toàn cầu (Đầy đủ CSS và Script Injection)
    "https://raw.githubusercontent.com/easylist/easylist/master/easylist/easylist_general_block.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt",
    
    # 3. Bộ lọc bảo mật, chống bám đuôi và mã độc chuyên sâu
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/privacy.txt",
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/badware.txt",
    
    # 4. Chặn yếu tố phiền nhiễu (Pop-up, Cookie, Quảng cáo ẩn)
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/annoyances.txt",
    
    # 5. Sửa lỗi vỡ trang và chống chặn Adblock (Anti-Adblock)
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/unbreak.txt"
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
                    
                    # CHỈ bỏ qua dòng trống và dòng comment thực sự
                    if not line or line.startswith('!') or line.startswith('[Adblock'):
                        continue
                    
                    # 1. XỬ LÝ LUẬT NGOẠI LỆ (Whitelisting)
                    if line.startswith('@@') or '#@#' in line:
                        exception_rules.add(line)
                        
                    # 2. XỬ LÝ LUẬT ẨN PHẦN TỬ (Cosmetic Filters)
                    elif re.search(r'(?<!#)##', line) or '#?#' in line or '#$#' in line:
                        cosmetic_rules.add(line)
                        
                    # 3. XỬ LÝ LUẬT MẠNG (Network Filters)
                    else:
                        network_rules.add(line)
            else:
                print(f"Không thể tải: {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"Lỗi khi tải {url}: {e}")
            
    # Sắp xếp thông minh: Đẩy các quy tắc tối ưu, quảng cáo cốt lõi, ads.js, pagead.js lên đầu bảng
    def network_priority_key(rule):
        # Mức 0 (Ưu tiên cao nhất): Các từ khóa ngắn và file script nguy hiểm hay bị lọt
        if rule in ['ads.js', 'pagead.js', 'ads', 'tracker'] or rule.endswith('.js') or rule.endswith('.js^'):
            return (0, len(rule), rule)
        # Mức 1: Các quy tắc tối ưu hóa hiệu năng cao bắt đầu bằng ||
        elif rule.startswith('||'):
            return (1, len(rule), rule)
        # Mức 2: Các quy tắc thông thường khác
        return (2, len(rule), rule)

    sorted_network = sorted(list(network_rules), key=network_priority_key)
    sorted_cosmetic = sorted(list(cosmetic_rules))
    sorted_exception = sorted(list(exception_rules))
            
    return sorted_network, sorted_cosmetic, sorted_exception

def write_filter_file(network, cosmetic, exception):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Combined Filter
! Description: Bộ lọc tối ưu hóa cấu trúc dòng và thứ tự ưu tiên. Chặn triệt để ads.js, pagead.js và tracker.
! Version: 2.2.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        
        f.write("! === SECTION 1: NETWORK FILTERS (PRIORITIZED) ===\n")
        for rule in network:
            f.write(rule + "\n")
        f.write("\n")
        
        f.write("! === SECTION 2: COSMETIC FILTERS / ELEMENT HIDING ===\n")
        for rule in cosmetic:
            f.write(rule + "\n")
        f.write("\n")
        
        f.write("! === SECTION 3: EXCEPTION RULES / WHITELISTING ===\n")
        for rule in exception:
            f.write(rule + "\n")
        f.write("\n")
        
    print(f"Thành công! Đã tối ưu thứ tự và xuất ra abp.txt: {len(network)} luật mạng, {len(cosmetic)} luật giao diện.")

if __name__ == "__main__":
    net, cos, exc = fetch_and_parse()
    write_filter_file(net, cos, exc)
