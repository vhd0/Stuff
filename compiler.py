import requests
from datetime import datetime

# DANH SÁCH RAW URL ĐÃ ĐƯỢC TỐI ƯU HÓA TOÀN DIỆN VỀ BANNER VÀ CỜ BẠC
URLS = [
    # === 1. BỘ LỌC CỦA BẠN (Đặc trị Việt Nam) ===
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # === 2. BỘ LỌC TỔNG HỢP DUY NHẤT CỦA BIGDARGON (hostsVN) ===
    "https://raw.githubusercontent.com/bigdargon/hostsVN/refs/heads/master/filters/adservers-all.txt",
    
    # === 3. CÁC BỘ LỌC ĐỈNH CAO TỪ HAGEZI (Định dạng Adblock) ===
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/multi-proplus.txt", # Nâng cấp lên Multi PRO++ (Chặn Ad/Tracker cực mạnh)
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/gambling.txt",      # Bổ sung bộ lọc Gambling (Đặc trị cá độ, cờ bạc)
    
    # === 4. CÁC BỘ LỌC CORE QUỐC TẾ (Sửa đổi để chặn đứng Banner Advertising) ===
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/filters.txt",      # Bộ lọc LÕI của uBlock (Bắt buộc phải có để diệt Banner)
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/badware.txt",      # Chặn malware, phần mềm độc hại
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/privacy.txt",      # Chặn theo dõi ngầm quốc tế
    "https://raw.githubusercontent.com/easylist/easylist/master/easylist/easylist_combined.txt", # CHUYỂN SANG BẢN COMBINED (Gộp cả chặn mạng + ẩn khung hình banner)
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt", # AdGuard Base nâng cao
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/unbreak.txt"       # Sửa lỗi vỡ trang quốc tế
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
                print(f"-> Tải thành công: {url.split('/')[-1]} ({len(lines)} dòng)")
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
! Title: ABPVN & Community Pure Filter Pro++
! Description: Bộ lọc tổng hợp nguyên bản hiệu năng cao, tích hợp HaGeZi Multi PRO++, Gambling, Bigdargon All-in-One và EasyList Combined.
! Version: 7.0.{datetime.utcnow().strftime('%Y%m%d')}
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
