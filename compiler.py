import requests
from datetime import datetime

# Danh sách phân vùng bộ lọc - kết hợp đặc trị cá độ, porn và quảng cáo hình ảnh (.gif)
URLS = [
    # === 1. BỘ LỌC CỦA BẠN (Đặc trị Việt Nam) ===
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # === 2. ĐẶC TRỊ CÁ ĐỘ, CỜ BẠC & BẢO MẬT (Dành cho VN và Quốc tế) ===
    "https://raw.githubusercontent.com/bigdargon/hostsVN/master/option/gambling.txt", # Chuyên cờ bạc, cá độ bóng đá
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/badware.txt",
    
    # === 3. ĐẶC TRỊ NỘI DUNG NGƯỜI LỚN & QUẢNG CÁO ĐỘC HẠI ===
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_14_Annoyances/filter.txt", # Pop-up lừa đảo, ẩn
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/privacy.txt",
    
    # === 4. BỘ LỌC QUẢNG CÁO CORE (EasyList & AdGuard Base) ===
    "https://raw.githubusercontent.com/easylist/easylist/master/easylist/easylist_general_block.txt",
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt",
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/unbreak.txt"
]

def fetch_and_merge():
    # Sử dụng list để giữ nguyên cấu trúc gốc của từng bộ lọc, tránh làm lỗi modifier
    network_rules = []
    cosmetic_rules = []
    exception_rules = []
    
    # Tập hợp để theo dõi nhanh, chỉ xóa các dòng trùng lặp thô giống nhau 100% nhằm giảm tải dung lượng
    seen_rules = set()

    print("Đang tải và xử lý đồng bộ dữ liệu...")
    for url in URLS:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                lines = response.text.splitlines()
                for line in lines:
                    line = line.strip()
                    
                    # Bỏ qua dòng trống, comment hoặc tiêu đề trùng lặp
                    if not line or line.startswith('!') or line.startswith('[Adblock'):
                        continue
                    
                    # Nếu dòng này đã tồn tại ở bộ lọc trước, bỏ qua để giảm dung lượng file
                    if line in seen_rules:
                        continue
                    seen_rules.add(line)
                    
                    # Phân loại nhanh để xếp vào các Section, giữ nguyên cú pháp gốc (Không can thiệp sâu)
                    if line.startswith('@@') or '#@#' in line:
                        exception_rules.append(line)
                    elif '##' in line or '#?#' in line or '#$#' in line:
                        cosmetic_rules.append(line)
                    else:
                        network_rules.append(line)
            else:
                print(f"Không thể tải: {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"Lỗi khi tải {url}: {e}")
            
    return network_rules, cosmetic_rules, exception_rules

def write_filter_file(network, cosmetic, exception):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Anti-Gambling Filter
! Description: Bộ lọc tổng hợp cấu trúc mở rộng, đặc trị các banner .gif cá độ, cờ bạc và trang web người lớn.
! Version: 3.0.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        # 1. Header & Metadata
        f.write(header + "\n")
        
        # Thêm thủ công một số luật chặn đuôi ảnh động phổ biến từ mạng lưới cá độ tại VN
        f.write("! === CUSTOM BANNER RULES ===\n")
        f.write("||*.gif$image,domain=~google.com,~youtube.com,~facebook.com\n") # Hạn chế load các file gif lạ ở các trang không phổ biến
        f.write("! -----------------------------------\n\n")
        
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
        
    print(f"Hoàn thành! File abp.txt đã được tối ưu cấu trúc gốc độc lập.")

if __name__ == "__main__":
    net, cos, exc = fetch_and_merge()
    write_filter_file(net, cos, exc)
