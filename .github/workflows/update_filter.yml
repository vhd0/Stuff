import requests
from datetime import datetime

# DANH SÁCH RAW URL ĐÃ ĐƯỢC SỬA LỖI ĐƯỜNG DẪN CHUẨN XÁC 100%
URLS = [
    # === 1. BỘ LỌC CỦA BẠN (Đặc trị Việt Nam) ===
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # === 2. BỘ LỌC TỔNG HỢP DUY NHẤT CỦA BIGDARGON (hostsVN) ===
    "https://raw.githubusercontent.com/bigdargon/hostsVN/refs/heads/master/filters/adservers-all.txt",
    
    # === 3. CÁC BỘ LỌC ĐỈNH CAO TỪ HAGEZI (Định dạng Adblock) ===
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.txt",        # Đã sửa thành pro.txt (Bản Pro cao cấp chuẩn Adblock)
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/gambling.txt",   # Đặc trị cá độ, cờ bạc (Tải thành công)
    
    # === 4. HỆ SINH THÁI ADGUARD CHUYÊN SÂU ===
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt",        # AdGuard Base (Tải thành công)
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_3_Spyware/filter.txt",     # AdGuard Tracking (Tải thành công)
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_14_Annoyances/filter.txt", # AdGuard Annoyances (Tải thành công)
    "https://raw.githubusercontent.com/AdguardTeam/AdguardFilters/master/QuickFixesFilter/filter.txt",              # Đã sửa sang link gốc trực tiếp của AdGuard
    
    # === 5. BỘ LỌC ÉP CHẶN KHUNG HÌNH BANNER ADVERTISING ===
    "https://easylist.to/easylist/easylist.txt" # Đã sửa sang URL phân phối chính thức của EasyList Toàn Diện
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
                # Hiển thị tên file trực quan trong log
                display_name = url.split('/')[-1] if not url.endswith('filter.txt') else url.split('/')[-2]
                print(f"-> Tải thành công: {display_name} ({len(lines)} dòng)")
                
                for line in lines:
                    line = line.strip()
                    
                    # CHỈ bỏ qua dòng trống, dòng chú thích gốc (!) hoặc tiêu đề định dạng [Adblock]
                    if not line or line.startswith('!') or line.startswith('[Adblock'):
                        continue
                    
                    # Chỉ lọc trùng nếu dòng đó đã xuất hiện trước đó để tối ưu dung lượng file
                    if line in seen_rules:
                        continue
                    seen_rules.add(line)
                    
                    # Giữ nguyên bản hoàn toàn và nạp tuyến tính nối đuôi nhau
                    merged_rules.append(line)
            else:
                print(f"❌ LỖI: Không thể tải {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"❌ LỖI: Khi tải {url}: {e}")
            
    return merged_rules

def write_pure_filter(rules):
    today = datetime.utcnow().strftime('%Y-%m-%d')
    
    header = f"""[Adblock Plus 2.0]
! Title: ABPVN & Community Ultimate Pure Filter (AdGuard Edition)
! Description: Bộ lọc tổng hợp nguyên bản thuần khiết. Tích hợp AdGuard Toàn Diện, HaGeZi Pro, Gambling và EasyList Full.
! Version: 8.2.{datetime.utcnow().strftime('%Y%m%d')}
! Author: @vhd0_
! Last modified: {today} UTC
! Expires: 1 days
! Homepage: https://github.com/vhd0
"""

    with open("abp.txt", "w", encoding="utf-8") as f:
        f.write(header + "\n")
        
        # Ghi trực tiếp toàn bộ các quy tắc theo đúng cấu trúc nguyên bản
        for rule in rules:
            f.write(rule + "\n")
        
    print(f"🎉 Thành công! Đã xuất file nguyên bản abp.txt với tổng cộng {len(rules)} quy tắc.")

if __name__ == "__main__":
    rules = fetch_and_merge_pure()
    write_pure_filter(rules)
