import requests
from datetime import datetime

# Danh sách URL tối ưu: ABPVN + Link tổng hợp của Bigdargon + Bộ lọc HaGeZi PRO + Lõi Quốc tế
URLS = [
    # === 1. BỘ LỌC CỦA BẠN (Đặc trị Việt Nam) ===
    "https://raw.githubusercontent.com/abpvn/abpvn/refs/heads/master/filter/abpvn.txt",
    
    # === 2. BỘ LỌC TỔNG HỢP DUY NHẤT CỦA BIGDARGON (hostsVN) ===
    "https://raw.githubusercontent.com/bigdargon/hostsVN/refs/heads/master/filters/adservers-all.txt",
    
    # === 3. BỘ LỌC HAGEZI PRO (Định dạng Adblock - Ổn định và an toàn nhất) ===
    "https://raw.githubusercontent.com/hagezi/dns-blocklists/main/adblock/pro.txt",
    
    # === 4. CÁC BỘ LỌC CỐT LÕI TỪ UBLOCK ORIGIN & EASYLIST TOÀN CẦU ===
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/badware.txt",     # Chặn malware, phần mềm độc hại
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/privacy.txt",     # Chặn theo dõi ngầm quốc tế
    "https://raw.githubusercontent.com/easylist/easylist/master/easylist/easylist_general_block.txt", # EasyList Core quảng cáo toàn cầu
    "https://raw.githubusercontent.com/AdguardTeam/FiltersRegistry/master/filters/filter_2_Base/filter.txt", # AdGuard Base nâng cao
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/unbreak.txt"      # Sửa lỗi vỡ trang quốc tế
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
                    
                    # Chỉ lọc trùng nếu dòng đó đã xuất hiện trước đó để tối ưu dung lượng
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
! Title: ABPVN & Community Ultimate Pure Filter
! Description: Bộ lọc tổng hợp nguyên bản hiệu năng cao, kết hợp ABPVN, Bigdargon All-in-One, HaGeZi Pro và lõi quốc tế.
! Version: 6.0.{datetime.utcnow().strftime('%Y%m%d')}
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
