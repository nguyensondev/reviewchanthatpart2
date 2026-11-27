# Quy Tắc Vận Hành & Tương Thích 2 Máy (Mac M1 Dev <-> MacBook Pro 2015 Production)

Dự án này (Kênh #2 — **Đồ vệ sinh & Gia dụng mini**) được phát triển trên **Mac M1 (Dev)** và chạy ngầm tự động 24/7 trên **MacBook Pro Early 2015 (`macOS Big Sur 11.4`, Dual-Core Intel Core i5 `x86_64`, 16GB RAM)** thông qua GitHub Private Repo (`https://github.com/nguyensondev/reviewchanthatpart2.git`).

Mọi thay đổi trong dự án BẮT BUỘC phải tuân thủ nghiêm ngặt các quy tắc sau:

## 1. Báo Trước Khi `git push` Nếu Ảnh Hưởng Tới Môi Trường / Package Trên Mac 2015
- **Ưu tiên tối đa** sử dụng thư viện chuẩn của Python và các gói đã có sẵn trong `requirements.txt` để máy Mac 2015 chỉ cần `git pull` là chạy ngay.
- **Nếu bắt buộc phải thêm/nâng cấp thư viện trong `requirements.txt`, thêm binary ngoài, hoặc sửa `setup_macos.sh` / `.env` / `launchd`:**
  1. Phải kiểm tra trước trên máy Dev bằng `--only-binary=:all:` cho nền tảng `macosx_11_0_x86_64` (Python 3.11) và kiểm tra header nhị phân Mach-O bằng `otool -l` đảm bảo `minos <= 11.4` (tuyệt đối không dùng wheel chứa `.dylib` đòi `minos 12.0+` như `opencv-python-headless >= 4.8` trên Intel Mac).
  2. **BẮT BUỘC thông báo rõ ràng cho người dùng TRƯỚC KHI `git push`**, nêu rõ bên máy Mac 2015 cần chạy lệnh gì sau khi `git pull` (ví dụ: `./.venv/bin/python -m pip install --only-binary=:all: -r requirements.txt` hoặc `bash setup_macos.sh`).

## 2. Bảo Vệ Tài nguyên, SSL & Tương Thích Trên `macOS Big Sur 11.4 (x86_64)`
- **SSL Root CA (`config/cacert.pem`):** `macOS Big Sur 11.4` có kho chứng chỉ hệ thống cũ (tháng 5/2021). Mọi kết nối HTTPS (`curl`, `urllib`, `httpx`) phải duy trì sử dụng `config/cacert.pem` (hoặc `certifi`) thông qua `MacOSLegacyRuntime.resolve_ca_bundle_path()`.
- **Đường dẫn linh hoạt (Portable Paths):** Không bao giờ giả định đường dẫn tuyệt đối cố định giữa các máy; luôn giải mã đường dẫn video qua `MacOSLegacyRuntime.resolve_portable_video_path()`.
- **Giới hạn CPU/RAM cho Dual-Core i5:** Giữ nguyên giới hạn luồng FFmpeg (`MacOSLegacyRuntime.optimal_ffmpeg_threads()` -> `2` luồng trên máy `<= 4` logical cores) và độ ưu tiên nền `nice +10`.

## 3. Kỷ Luật Đồng Bộ Git & Cloudflare Giữa 2 Máy
- **Tuyệt đối không commit** `.env`, `config/platform_credentials.json`, các file trạng thái runtime (`data/video_registry.json`, `data/sku_registry.json`, `data/daily_quota_state.json`, `data/m5_publish_state.json`, `data/short_links_registry.json`) hoặc thư mục video (`output/`, `data/m4_video_cache/`, `poc/`).
- **Đồng bộ Cloudflare Worker (`giadung.gocreviewchanthuc.workers.dev`):** Luôn giữ cơ chế đọc `ROUTES` hiện có trên Cloudflare về gộp (`merge`) trước khi `PUT` trong `pipeline/cloudflare_shortener.py` (tách riêng `CLOUDFLARE_WORKER_NAME=giadung` độc lập với `go` của Kênh #1) để máy Dev không bao giờ ghi đè làm mất link rút gọn do máy Mac 2015 tạo ra.
- **Bảo vệ tài khoản Shopee Affiliate (`17337280644`):** Tuyệt đối không gửi bất kỳ HTTP request tự động nào tới `shopee.vn` hoặc `s.shopee.vn` có gắn `affiliate_id=17337280644` (khi test link rút gọn Cloudflare bắt buộc đặt `follow_redirects=False`).
