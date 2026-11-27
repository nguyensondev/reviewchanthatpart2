# 🖥️ HƯỚNG DẪN CHUYỂN & CÀI ĐẶT CHẠY NGẦM 24/7 TRÊN MACBOOK PRO EARLY 2015 (`macOS Big Sur 11.4`)

Tài liệu này được kiểm chứng thực tế cho đúng cấu hình **MacBook Pro (Retina, 13-inch, Early 2015) — `macOS Big Sur 11.4` — Dual-Core Intel Core i5 2.7 GHz (`x86_64`) — RAM 16 GB**:

- **Tương thích chứng chỉ SSL (`Big Sur 11.4` tháng 5/2021):** Bản `macOS 11.4` ra đời trước sự kiện chứng chỉ gốc *DST Root CA X3* hết hạn (tháng 9/2021). Dự án đã đóng gói sẵn bộ chứng chỉ gốc 2026 tại `config/cacert.pem` nên `curl`, `uv`, `yt-dlp`, `edge-tts`, `Telegram` và `Cloudflare` không bao giờ bị lỗi hết hạn SSL.
- **Tương thích Wheel & Mach-O `x86_64` (`minos <= 11.0`) 100% không biên dịch:** Toàn bộ mã máy `.so/.dylib` trong [requirements.txt](requirements.txt) và [setup_macos.sh](setup_macos.sh) đã được kiểm tra header nhị phân bằng `otool -l` cho `macOS Big Sur 11.4` (`uv` `minos 10.12`, `Python 3.11` `minos 10.15`, `ffmpeg/ffprobe` `minos 10.13`, `opencv-python-headless==4.7.0.72` `minos 11.0` — tránh bản OpenCV `4.8/4.9+` vốn chứa dylib `minos 12.0` của Monterey).
- **Đường dẫn linh hoạt (Portable Path):** Dù tên user trên máy Mac 2015 khác với `sonnguyen`, hàm `resolve_portable_video_path` tự động nhận diện toàn bộ video đã render sẵn trong `output/videos/`.

---

## Bước 1: Chép thư mục `giadungmini` sang MacBook Pro 2015 (giữ nguyên file ẩn `.env`)

Trên máy hiện tại, mở Terminal chạy lệnh nén này để không bị sót file ẩn `.env` và `config/platform_credentials.json`:

```bash
cd /Users/sonnguyen/Projects
zip -r giadungmini_mac2015.zip giadungmini -x "giadungmini/.venv/*" "giadungmini/bin/*" "giadungmini/logs/*" "giadungmini/data/m4_work/*" "giadungmini/data/m4_video_cache/*" "giadungmini/pipeline/__pycache__/*"
```

Chép `giadungmini_mac2015.zip` qua USB / ổ cứng / AirDrop sang MacBook Pro 2015 (hoặc `git clone https://github.com/nguyensondev/reviewchanthatpart2.git giadungmini`), rồi kiểm tra chắc chắn đủ 2 file cấu hình mật:

```bash
cd ~/Projects/giadungmini
ls -la .env config/platform_credentials.json config/cacert.pem
```

---

## Bước 2: Chạy 1 lệnh cài đặt tự động trên MacBook Pro 2015

```bash
bash setup_macos.sh
```

*(Thời gian cài đặt khoảng **1 – 2 phút**: tự tải `ffmpeg/ffprobe` tĩnh cho macOS 10.13+ Intel, cài Python 3.11 + wheels, kiểm định 5/5 tiêu chí, bật `launchd` riêng biệt `com.giadungmini.autopipeline` lệch pha 38 phút để không bao giờ đụng FFmpeg với Kênh #1, và bắn tin nhắn xác nhận về Bot Telegram mới của Kênh #2).*

---

## Bước 3: Cấu hình chống mất Wi-Fi & chống ngủ đông khi tắt màn hình (CỰC KỲ QUAN TRỌNG)

Trên dòng **MacBook Pro Intel Early 2015 chạy `macOS Big Sur 11.4`**, có **4 nguyên tắc vật lý & hệ điều hành** bạn cần làm đúng để Wi-Fi không bao giờ rớt khi màn hình tắt:

### 1. Chạy 1 dòng lệnh khóa cứng cấu hình nguồn AC (`pmset`)
Chỉ chỉnh bằng chuột trong *System Preferences* trên máy Intel 2015 là **chưa đủ**, vì thông số ẩn `standby` và `autopoweroff` của Apple vẫn sẽ tự đưa máy vào chế độ ngủ đông (Hibernate) và ngắt Wi-Fi sau 3 tiếng không ai chạm phím!

Hãy chạy đúng lệnh này trên Terminal của MacBook Pro 2015 (nhập mật khẩu mở máy Mac khi được hỏi):

```bash
sudo pmset -c sleep 0 displaysleep 5 disksleep 0 standby 0 autopoweroff 0 powernap 1 tcpkeepalive 1 womp 1
```

**Ý nghĩa chính xác từng thông số:**
- `-c`: Chỉ áp dụng khi **đang cắm sạc MagSafe** (nếu cúp điện chạy pin, máy vẫn tự ngủ để bảo vệ pin).
- `sleep 0` + `standby 0` + `autopoweroff 0`: Cấm tuyệt đối máy rơi vào System Sleep hay Hibernate sâu 24/7.
- `displaysleep 5`: Tự động **tắt màn hình Retina sau 5 phút** để màn hình tối đen hoàn toàn, mát máy và không tốn điện.
- `tcpkeepalive 1` + `womp 1`: Giữ kết nối Wi-Fi (`en0`) luôn sống và trao đổi gói tin giữ phiên với cục Router kể cả khi màn hình đang tắt đen.

Kiểm tra lại xem đã nhận chưa bằng lệnh:
```bash
pmset -g
```
*(Nếu thấy `sleep 0`, `standby 0`, `autopoweroff 0`, `tcpkeepalive 1` là chuẩn 100%).*

### 2. KHÔNG gập kín nắp máy (Clamshell) — Hãy mở nắp và giảm sáng màn hình về `0`
- **Lý do 1 (Mất mạng):** Khi không cắm màn hình rời, gập kín nắp sẽ kích hoạt cảm biến từ trường (Hall sensor) cưỡng bức tắt card Wi-Fi của MacBook Pro 2015.
- **Lý do 2 (Tản nhiệt):** Khe thoát gió nóng của MacBook Pro 2015 nằm dọc bản lề bàn phím; gập kín nắp khi FFmpeg render sẽ làm nóng màn hình Retina và chai pin.
- **Cách chuẩn nhất:** Mở nắp máy bình thường (hoặc hé 60–90 độ nơi thoáng mát), **bấm nút giảm độ sáng `F1` trên bàn phím về kịch kim (`0` vạch)** để đèn nền màn hình tắt tối đen ngay lập tức (sau 5 phút `displaysleep 5` cũng tự ngắt tín hiệu hình).

### 3. Chỉnh trong giao diện `System Preferences` của `macOS Big Sur 11.4`
1. **System Preferences -> Battery -> chọn tab `Power Adapter` (cột trái):**
   - ✅ Tích chọn: **`Prevent computer from sleeping automatically when the display is off`**
   - ⬜ Bỏ tích: **`Put hard disks to sleep when possible`**
   - ✅ Tích chọn: **`Wake for network access`**
2. **System Preferences -> Desktop & Screen Saver -> tab `Screen Saver`:**
   - Chọn *Start after*: **`Never`** (Không chạy trình bảo vệ màn hình 3D gây nóng GPU Intel Iris 6100, để màn hình tắt đen luôn).
3. **System Preferences -> Security & Privacy -> General:**
   - **Bỏ tích** *Require password ... after sleep or screen saver begins* (nếu máy để trong phòng riêng), tránh kẹt khóa phiên đồ họa.
4. **System Preferences -> Network -> Wi-Fi -> Advanced...:**
   - Xóa bớt các mạng Wi-Fi cũ yếu/chập chờn trong danh sách *Preferred Networks*, chỉ giữ mạng Wi-Fi chính của nhà bạn ở trên cùng để máy không tự nhảy sang sóng yếu.
5. **System Preferences -> Software Update -> Advanced...:**
   - **Bỏ tích** *Automatically keep my Mac up to date* để máy không tự khởi động lại lúc nửa đêm đòi nâng cấp macOS!
