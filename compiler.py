import requests
from datetime import datetime

# Danh sách tối ưu: Giữ lại duy nhất ABPVN của bạn + Các bộ lọc lõi chuẩn cộng đồng toàn cầu
URLS = [
    # 1. Bộ lọc của riêng bạn (Đặc trị cho các trang web Việt Nam)
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # 2. Bộ lọc quảng cáo cốt lõi (EasyList phiên bản tối ưu hóa cho Adblock Plus)
    "https://v.firebog.net/hosts/AdguardDNS.txt",
    
    # 3. Các bộ lọc bảo mật, chống bám đuôi và sửa lỗi từ uBlock Origin (Cộng đồng khuyên dùng)
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/privacy.txt",      # Chặn theo dõi ngầm (Trackers)
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/annoyances.txt",   # Chặn thông báo cookie, pop-up phiền nhiễu
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/badware.txt",      # Chặn phần mềm độc hại, quảng cáo lừa đảo
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/unbreak.txt"       # Sửa lỗi hiển thị và chống Anti-Adblock
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
                    
                    # Bỏ các dòng trống, dòng chú thích cũ của các tác giả khác
                    if not line or line.startswith('!') or line.startswith('[Adblock'):
                        continue
                        
                    # Phân loại dựa trên cú pháp tối ưu
                    if line.startswith('@@'):
                        exception_rules.add(line)
                    elif '##' in line or '#@#' in line:
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
    
    # Đã cập nhật Homepage chính xác theo yêu cầu của bạn
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Combined Filter
! Description: Bộ lọc tối ưu kết hợp giữa ABPVN và tinh hoa từ uBlock Origin, EasyList. Tự động cập nhật hàng ngày.
! Version: 1.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
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
        
    print(f"Đã tạo thành công file abp.txt với {len(network)} luật mạng, {len(cosmetic)} luật giao diện và {len(exception)} luật ngoại lệ.")

if __name__ == "__main__":
    net, cos, exc = fetch_and_parse()
    write_filter_file(net, cos, exc)
