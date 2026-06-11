import re
import requests

def fetch_country_sub(session, country):
    url = f"https://www.v2nodes.com/country/{country}/"
    print(f"[*] Đang xử lý khu vực: {country.upper()} - {url}")
    
    try:
        # Tải trang gốc của quốc gia
        response = session.get(url, timeout=15)
        response.raise_for_status()
        html_content = response.text
        
        # Regex tìm link sub động dựa theo mã quốc gia
        pattern = rf'https://www\.v2nodes\.com/subscriptions/country/{country}/\?key=[a-zA-Z0-9]+'
        match = re.search(pattern, html_content)
        
        if not match:
            # Dự phòng nếu web đổi sang link tương đối
            pattern_relative = rf'/subscriptions/country/{country}/\?key=[a-zA-Z0-9]+'
            match = re.search(pattern_relative, html_content)
            if match:
                sub_url = "https://www.v2nodes.com" + match.group(0)
            else:
                print(f"[!] Không tìm thấy link subscription cho {country.upper()}.")
                return
        else:
            sub_url = match.group(0)
            
        print(f"[+] Tìm thấy link sub {country.upper()}: {sub_url}")
        
        # Tải nội dung file node từ link sub
        sub_response = session.get(sub_url, timeout=15)
        sub_response.raise_for_status()
        
        content = sub_response.text.strip()
        if content:
            # Lưu ra file riêng cho từng quốc gia
            file_name = f"{country}_sub.txt"
            with open(file_name, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"[➔] Đã cập nhật thành công: {file_name}")
        else:
            print(f"[!] Dữ liệu từ link sub {country.upper()} bị trống.")
            
    except requests.RequestException as e:
        print(f"[!] Lỗi kết nối khi xử lý {country.upper()}: {e}")

def main():
    # Danh sách mã quốc gia cần lấy (bạn có thể thêm us, kr... vào đây nếu muốn)
    countries = ['hk', 'jp', 'sg']
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    print("=== BẮT ĐẦU CÀO DỮ LIỆU NODE ===")
    
    # Sử dụng Session để tái sử dụng TCP connection, tăng tốc độ request
    with requests.Session() as session:
        session.headers.update(headers)
        
        for country in countries:
            fetch_country_sub(session, country)
            print("-" * 45)
            
    print("=== HOÀN TẤT ===")

if __name__ == "__main__":
    main()
