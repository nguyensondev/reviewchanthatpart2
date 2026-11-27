#!/usr/bin/env bash
# Autonomous Installer & LaunchAgent Setup for macOS
# Byte-verified for MacBook Pro Early 2015 (Intel x86_64, macOS Big Sur 11.4, GNU bash 3.2.57) & Apple Silicon
set -euo pipefail

VERIFY_ONLY=0
if [ "${1:-}" = "--verify-install-only" ]; then
    VERIFY_ONLY=1
fi

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

ARCH="${FORCE_TEST_ARCH:-$(uname -m)}"
MACOS_VER="$(sw_vers -productVersion 2>/dev/null || echo unknown)"
HOSTNAME_SHORT="$(scutil --get ComputerName 2>/dev/null || hostname -s 2>/dev/null || echo Mac)"
BIN_DIR="$PROJECT_DIR/bin"
LOG_DIR="$PROJECT_DIR/logs"
VENV_DIR="$PROJECT_DIR/.venv"
CA_BUNDLE="$PROJECT_DIR/config/cacert.pem"
PLIST_LABEL="com.giadungmini.autopipeline"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS_DIR/${PLIST_LABEL}.plist"

mkdir -p "$BIN_DIR" "$LOG_DIR" "$LAUNCH_AGENTS_DIR"
if [ "${FORCE_BARE_MAC:-0}" = "1" ]; then
    export PATH="$BIN_DIR:/usr/bin:/bin:/usr/sbin:/sbin"
else
    export PATH="$BIN_DIR:/usr/local/bin:/opt/homebrew/bin:$HOME/.local/bin:$PATH"
fi

if [ -f "$CA_BUNDLE" ]; then
    export SSL_CERT_FILE="$CA_BUNDLE"
    export REQUESTS_CA_BUNDLE="$CA_BUNDLE"
    export CURL_CA_BUNDLE="$CA_BUNDLE"
fi

# Safe curl wrapper compatible with macOS /bin/bash 3.2.57 (no empty-array nounset bug)
# and macOS Big Sur 11.4 (passes bundled 2026 Root CA store explicitly via --cacert)
safe_curl() {
    if [ -f "$CA_BUNDLE" ]; then
        curl --cacert "$CA_BUNDLE" "$@"
    else
        curl "$@"
    fi
}

echo "======================================================================"
echo "  CAI DAT HE THONG GIA DUNG MINI & DO VE SINH SHORT VIDEO AUTOPILOT"
echo "  May: $HOSTNAME_SHORT | CPU: $ARCH | macOS: $MACOS_VER"
echo "  Thu muc: $PROJECT_DIR"
echo "======================================================================"

# 1. Clear macOS Gatekeeper quarantine flags if copied via AirDrop/USB/Browser
xattr -dr com.apple.quarantine "$PROJECT_DIR" >/dev/null 2>&1 || true

# 2. Bootstrap standalone uv binary directly from GitHub Releases (LC_VERSION_MIN_MACOSX = 10.12)
# Avoids calling Xcode Command Line Tools python3 or un-certificated curl pipes on Big Sur 11.4!
if ! command -v uv >/dev/null 2>&1 || ! uv --version >/dev/null 2>&1; then
    rm -f "$BIN_DIR/uv"
    echo "[SETUP] Dang tai trinh quan ly Python uv (build san cho macOS $ARCH, minos 10.12)..."
    TMP_UV_DIR="$(mktemp -d /tmp/giadungmini_uv.XXXXXX)"
    if [ "$ARCH" = "arm64" ]; then
        UV_URL="https://github.com/astral-sh/uv/releases/latest/download/uv-aarch64-apple-darwin.tar.gz"
    else
        UV_URL="https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-apple-darwin.tar.gz"
    fi
    safe_curl -fL --retry 3 --connect-timeout 20 "$UV_URL" -o "$TMP_UV_DIR/uv.tar.gz"
    tar -xzf "$TMP_UV_DIR/uv.tar.gz" -C "$TMP_UV_DIR"
    UV_FOUND="$(find "$TMP_UV_DIR" -type f -name uv | head -n 1)"
    if [ -z "$UV_FOUND" ]; then
        rm -rf "$TMP_UV_DIR"
        echo "[ERROR] Khong giai nen duoc uv tu $UV_URL" >&2
        exit 1
    fi
    cp "$UV_FOUND" "$BIN_DIR/uv"
    chmod +x "$BIN_DIR/uv"
    xattr -d com.apple.quarantine "$BIN_DIR/uv" >/dev/null 2>&1 || true
    rm -rf "$TMP_UV_DIR"
fi

echo "[OK] Su dung uv tai: $(command -v uv)"

# 3. Provision standalone Python 3.11 (macos 10.9+ x86_64 / 11.0+ arm64) & install 100% binary wheels
if [ ! -x "$VENV_DIR/bin/python" ] || ! "$VENV_DIR/bin/python" --version >/dev/null 2>&1; then
    rm -rf "$VENV_DIR"
    echo "[SETUP] Dang tai Python 3.11 standalone & khoi tao .venv..."
    uv python install 3.11
    uv venv "$VENV_DIR" --python 3.11
fi

VENV_PY="$VENV_DIR/bin/python"
echo "[SETUP] Dang cai dat thu vien prebuilt wheel (--only-binary=:all:) tu requirements.txt..."
uv pip install --python "$VENV_PY" --only-binary=:all: -r "$PROJECT_DIR/requirements.txt"
echo "[OK] Python trong venv: $("$VENV_PY" --version)"

# Refresh config/cacert.pem from installed certifi
"$VENV_PY" -c '
import certifi, shutil, sys
from pathlib import Path
shutil.copy2(certifi.where(), Path(sys.argv[1]))
' "$CA_BUNDLE"

# 4. Provision static ffmpeg + ffprobe (LC_VERSION_MIN_MACOSX = 10.13; dual mirror: evermeet JSON API + GitHub ffmpeg-static)
ensure_static_ffmpeg_tool() {
    local tool="$1"
    if command -v "$tool" >/dev/null 2>&1 && "$tool" -version >/dev/null 2>&1; then
        echo "[OK] Da co san $tool tai: $(command -v "$tool")"
        return 0
    fi
    rm -f "$BIN_DIR/$tool"

    echo "[SETUP] Chua co $tool -> Dang tai ban static build san cho macOS ($ARCH, minos 10.13)..."
    "$VENV_PY" -c '
import io, os, stat, subprocess, sys, zipfile
from pathlib import Path
import httpx

tool, arch, bin_dir_str, ca_bundle = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
target = Path(bin_dir_str) / tool
verify_arg = ca_bundle if os.path.exists(ca_bundle) else True

downloaded = False
with httpx.Client(timeout=60.0, follow_redirects=True, verify=verify_arg) as client:
    # Primary for Intel x86_64: evermeet.cx official JSON metadata endpoint (minos 10.13)
    if arch != "arm64":
        try:
            meta = client.get(f"https://evermeet.cx/ffmpeg/info/{tool}/release").json()
            zip_url = meta["download"]["zip"]["url"]
            print(f"  -> Dang tai tu evermeet.cx: {zip_url}", flush=True)
            rz = client.get(zip_url)
            rz.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(rz.content)) as zf:
                zf.extract(tool, path=bin_dir_str)
            downloaded = target.exists() and target.stat().st_size > 1_000_000
        except Exception as exc:
            print(f"  [WARN] evermeet.cx tam ban ({exc}), chuyen sang mirror GitHub CDN...", flush=True)

    # Primary for arm64 & Fallback for x86_64: eugeneware/ffmpeg-static on GitHub Releases (minos 10.13)
    if not downloaded:
        gh_arch = "arm64" if arch == "arm64" else "x64"
        gh_url = f"https://github.com/eugeneware/ffmpeg-static/releases/latest/download/{tool}-darwin-{gh_arch}"
        print(f"  -> Dang tai tu GitHub CDN: {gh_url}", flush=True)
        rb = client.get(gh_url)
        rb.raise_for_status()
        target.write_bytes(rb.content)
        downloaded = target.exists() and target.stat().st_size > 1_000_000

if not downloaded:
    raise SystemExit(f"Khong tai duoc {tool}!")

target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
subprocess.run(["xattr", "-d", "com.apple.quarantine", str(target)], capture_output=True)
' "$tool" "$ARCH" "$BIN_DIR" "$CA_BUNDLE"

    if [ "$ARCH" = "$(uname -m)" ]; then
        if ! "$BIN_DIR/$tool" -version >/dev/null 2>&1; then
            echo "[ERROR] Khong the khoi chay $BIN_DIR/$tool!" >&2
            return 1
        fi
    fi
    echo "[OK] Da cai dat $tool thanh cong tai $BIN_DIR/$tool"
}

ensure_static_ffmpeg_tool "ffmpeg"
ensure_static_ffmpeg_tool "ffprobe"

# 5. Re-bind portable video paths for this machine & Run 5-Criteria Comprehensive Verification
echo "[SYNC] Cap nhat duong dan file MP4 tuong thich voi thu muc may nay..."
"$VENV_PY" -m pipeline.run_daily_pipeline --mode pack
echo "[CHECK] Dang chay kiem dinh toan dien 5/5 tieu chi tren may nay..."
"$VENV_PY" -m pipeline.verify_all_5_criteria

if [ "$VERIFY_ONLY" = "1" ]; then
    echo "[VERIFY-ONLY] Hoan tat kiem tra cai dat thuc te (bo qua dang ky launchd tren may dev)."
    exit 0
fi

# 6. Install & Activate Background LaunchAgent (Runs every 38 minutes = 2280s, staggered from Channel #1's 2100s!)
echo "[LAUNCHD] Dang cai dat lich tu chay ngam $PLIST_LABEL (chu ky lech pha 38 phut = 2280s)..."
cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/caffeinate</string>
        <string>-dims</string>
        <string>${VENV_PY}</string>
        <string>-m</string>
        <string>pipeline.run_daily_pipeline</string>
        <string>--mode</string>
        <string>auto</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${PROJECT_DIR}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>${BIN_DIR}:/usr/local/bin:/opt/homebrew/bin:${HOME}/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
        <key>SSL_CERT_FILE</key>
        <string>${CA_BUNDLE}</string>
        <key>REQUESTS_CA_BUNDLE</key>
        <string>${CA_BUNDLE}</string>
        <key>CURL_CA_BUNDLE</key>
        <string>${CA_BUNDLE}</string>
    </dict>
    <key>ProcessType</key>
    <string>Background</string>
    <key>Nice</key>
    <integer>10</integer>
    <key>RunAtLoad</key>
    <false/>
    <key>StartInterval</key>
    <integer>2280</integer>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/launchd_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/launchd_stderr.log</string>
</dict>
</plist>
EOF

plutil -lint "$PLIST_PATH"

USER_UID="$(id -u)"
launchctl bootout "gui/${USER_UID}" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl unload -w "$PLIST_PATH" >/dev/null 2>&1 || true

if ! launchctl bootstrap "gui/${USER_UID}" "$PLIST_PATH" >/dev/null 2>&1; then
    launchctl load -w "$PLIST_PATH"
fi

echo "[OK] Da kich hoat LaunchAgent: $PLIST_PATH"

# 6b. Install & Activate 24/7 Telegram Health Bot Menu Listener (KeepAlive, zero extra deps)
TG_PLIST_LABEL="com.giadungmini.telegrambot"
TG_PLIST_PATH="$LAUNCH_AGENTS_DIR/${TG_PLIST_LABEL}.plist"
cat > "$TG_PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${TG_PLIST_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${VENV_PY}</string>
        <string>-m</string>
        <string>pipeline.telegram_health_bot</string>
        <string>--daemon</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${PROJECT_DIR}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>${BIN_DIR}:/usr/local/bin:/opt/homebrew/bin:${HOME}/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
        <key>SSL_CERT_FILE</key>
        <string>${CA_BUNDLE}</string>
        <key>REQUESTS_CA_BUNDLE</key>
        <string>${CA_BUNDLE}</string>
        <key>CURL_CA_BUNDLE</key>
        <string>${CA_BUNDLE}</string>
    </dict>
    <key>ProcessType</key>
    <string>Background</string>
    <key>Nice</key>
    <integer>10</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/telegram_bot_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/telegram_bot_stderr.log</string>
</dict>
</plist>
EOF

plutil -lint "$TG_PLIST_PATH"
launchctl bootout "gui/${USER_UID}" "$TG_PLIST_PATH" >/dev/null 2>&1 || true
launchctl unload -w "$TG_PLIST_PATH" >/dev/null 2>&1 || true
if ! launchctl bootstrap "gui/${USER_UID}" "$TG_PLIST_PATH" >/dev/null 2>&1; then
    launchctl load -w "$TG_PLIST_PATH"
fi
echo "[OK] Da kich hoat Telegram Health Bot LaunchAgent: $TG_PLIST_PATH"

# 7. Send Telegram confirmation from this machine
"$VENV_PY" -c '
import platform, sys
from pipeline.core_resilience import MacOSLegacyRuntime, TelegramNotifier

host_short, mac_ver, plist_label, proj_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
n = TelegramNotifier()
if n.is_configured():
    ok, info = n.notify_pipeline_event(
        "🖥️ MAY MAC DA SAN SANG CHAY NGAM TU DONG",
        [
            f"💻 May: {host_short} ({platform.machine()} / macOS {mac_ver})",
            f"⚙️ FFmpeg threads gioi han: {MacOSLegacyRuntime.optimal_ffmpeg_threads()} (nice +10)",
            f"⏰ LaunchAgent: {plist_label} (chu ky lech pha 38 phut, chi hoat dong khung gio nguoi that)",
            f"📂 Thu muc: {proj_dir}",
        ],
    )
    print(f"[TELEGRAM] Sent host ready alert: ok={ok} ({info})")
else:
    print("[TELEGRAM] Chua cau hinh bot Telegram, bo qua gui tin xac nhan.")
' "$HOSTNAME_SHORT" "$MACOS_VER" "$PLIST_LABEL" "$PROJECT_DIR"

echo "======================================================================"
echo "  HOAN TAT 100%! HE THONG DA TU CHAY NGAM TREN MAY NAY."
echo "  [QUAN TRONG] Chay them 1 lenh duoi day (nhap mat khau may Mac)"
echo "  de tat ngu he thong & giu Wi-Fi luon song khi tat man hinh:"
echo ""
echo "  sudo pmset -c sleep 0 displaysleep 5 disksleep 0 standby 0 autopoweroff 0 powernap 1 tcpkeepalive 1 womp 1"
echo ""
echo "  - Xem trang thai : ${VENV_PY} -m pipeline.run_daily_pipeline --mode status"
echo "  - Xem log truc tiep: tail -f ${LOG_DIR}/launchd_stdout.log"
echo "======================================================================"
