#!/usr/bin/env bash
# fetch-mtproto Linux setup: Python deps, Xray-core, config, data dirs.
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

ERR=0
PYTHON=""
REQUIREMENTS="$ROOT/requirements.txt"
XRAY_DIR="$ROOT/xray"
XRAY_BIN="$ROOT/xray/xray"
TMP_DIR="$ROOT/.setup_tmp"

fail() {
  echo
  echo "ERROR: $*"
  ERR=1
}

ok() {
  echo "  OK: $*"
}

probe_python() {
  local cmd=("$@")
  local exe
  if ! exe="$("${cmd[@]}" -c "import sys; print(sys.executable)" 2>/dev/null)"; then
    return 1
  fi
  if [[ ! -x "$exe" ]]; then
    return 1
  fi
  if "$exe" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" 2>/dev/null; then
    PYTHON="$exe"
    return 0
  fi
  echo "  Found Python but need 3.10+: $exe"
  return 1
}

find_python() {
  PYTHON=""
  local candidate
  for candidate in python3 python python3.13 python3.12 python3.11 python3.10; do
    if command -v "$candidate" >/dev/null 2>&1; then
      if probe_python "$candidate"; then
        return 0
      fi
    fi
  done
  return 1
}

deps_satisfied() {
  [[ -n "$PYTHON" ]] && "$PYTHON" -c "import telethon, python_socks, TelethonFakeTLS, cryptography" 2>/dev/null
}

find_xray_on_path() {
  local found=""
  found="$(command -v xray 2>/dev/null || true)"
  if [[ -n "$found" && -x "$found" ]]; then
    echo "$found"
    return 0
  fi
  found="$(command -v xray.exe 2>/dev/null || true)"
  if [[ -n "$found" && -x "$found" ]]; then
    echo "$found"
    return 0
  fi
  return 1
}

python_install_hint() {
  echo "  Install Python 3.10+ using your package manager, for example:"
  echo "    Debian/Ubuntu: sudo apt install python3 python3-pip python3-tk"
  echo "    Fedora:          sudo dnf install python3 python3-pip python3-tkinter"
  echo "    Arch:            sudo pacman -S python python-pip tk"
}

xray_asset_for_arch() {
  local arch="$1"
  case "$arch" in
    x86_64|amd64) echo "Xray-linux-64.zip" ;;
    aarch64|arm64) echo "Xray-linux-arm64-v8a.zip" ;;
    armv7l|armv7) echo "Xray-linux-armv7l.zip" ;;
    i686|i386) echo "Xray-linux-32.zip" ;;
    *)
      fail "Unsupported CPU architecture: $arch"
      return 1
      ;;
  esac
}

echo "========================================"
echo " fetch-mtproto - Linux setup"
echo "========================================"
echo
echo "Project: $ROOT"
echo

# ---------- 1) Python ----------
echo "[1/5] Checking for installed Python 3.10+ ..."
if find_python; then
  ok "Using installed Python: $PYTHON"
else
  fail "No suitable Python 3.10+ found on PATH."
  python_install_hint
fi
echo

# ---------- 2) Python packages ----------
echo "[2/5] Installing Python packages ..."
if [[ "$ERR" -eq 0 && ! -f "$REQUIREMENTS" ]]; then
  fail "requirements.txt not found"
elif [[ "$ERR" -eq 0 ]]; then
  if deps_satisfied; then
    ok "Dependencies already installed globally"
  elif ! "$PYTHON" -m pip install --upgrade pip setuptools wheel; then
    fail "pip upgrade failed"
  elif ! "$PYTHON" -m pip install -r "$REQUIREMENTS"; then
    fail "pip install -r requirements.txt failed"
  else
    ok "Dependencies installed globally"
  fi
fi
echo

# ---------- 3) Xray-core ----------
echo "[3/5] Checking Xray-core ..."
if [[ "$ERR" -ne 0 ]]; then
  :
elif XRAY_ON_PATH="$(find_xray_on_path)"; then
  ok "Using xray from PATH: $XRAY_ON_PATH"
elif [[ -x "$XRAY_BIN" ]]; then
  ok "xray already present in xray folder"
elif [[ -x "$ROOT/bin/xray" ]]; then
  mkdir -p "$XRAY_DIR"
  cp "$ROOT/bin/xray" "$XRAY_BIN"
  chmod +x "$XRAY_BIN"
  ok "Copied bin/xray to xray folder"
else
  echo "  Downloading Xray-core for Linux ..."
  if ! command -v curl >/dev/null 2>&1; then
    fail "curl not found (needed to download Xray)"
  elif ! command -v tar >/dev/null 2>&1; then
    fail "tar not found (needed to extract Xray)"
  else
    ARCH="$(uname -m)"
    XRAY_ASSET="$(xray_asset_for_arch "$ARCH")" || XRAY_ASSET=""
    if [[ -n "$XRAY_ASSET" ]]; then
      rm -rf "$TMP_DIR"
      mkdir -p "$TMP_DIR/xray"
      XRAY_URL="https://github.com/XTLS/Xray-core/releases/latest/download/$XRAY_ASSET"
      echo "  URL: $XRAY_URL"
      if ! curl -L --retry 3 --fail -o "$TMP_DIR/$XRAY_ASSET" "$XRAY_URL"; then
        fail "Failed to download $XRAY_ASSET"
        rm -rf "$TMP_DIR"
      elif ! tar -xf "$TMP_DIR/$XRAY_ASSET" -C "$TMP_DIR/xray"; then
        fail "Failed to extract $XRAY_ASSET"
        rm -rf "$TMP_DIR"
      elif [[ ! -f "$TMP_DIR/xray/xray" ]]; then
        fail "xray binary missing from archive"
        rm -rf "$TMP_DIR"
      else
        mkdir -p "$XRAY_DIR"
        cp "$TMP_DIR/xray/xray" "$XRAY_BIN"
        chmod +x "$XRAY_BIN"
        [[ -f "$XRAY_DIR/geoip.dat" || ! -f "$TMP_DIR/xray/geoip.dat" ]] || cp "$TMP_DIR/xray/geoip.dat" "$XRAY_DIR/geoip.dat"
        [[ -f "$XRAY_DIR/geosite.dat" || ! -f "$TMP_DIR/xray/geosite.dat" ]] || cp "$TMP_DIR/xray/geosite.dat" "$XRAY_DIR/geosite.dat"
        rm -rf "$TMP_DIR"
        if [[ ! -x "$XRAY_BIN" ]]; then
          fail "xray was not installed"
        else
          ok "Installed xray in xray folder"
        fi
      fi
    fi
  fi
fi
echo

# ---------- 4) Config ----------
echo "[4/5] Checking config.yaml ..."
if [[ "$ERR" -ne 0 ]]; then
  :
elif [[ -f "$ROOT/config.yaml" ]]; then
  ok "config.yaml already exists (left unchanged)"
elif [[ -f "$ROOT/config.example.yaml" ]]; then
  cp "$ROOT/config.example.yaml" "$ROOT/config.yaml"
  ok "Created config.yaml from config.example.yaml"
  echo
  echo "  IMPORTANT: Edit config.yaml and set telegram.api_id / telegram.api_hash from"
  echo "  https://my.telegram.org/apps"
else
  fail "Neither config.yaml nor config.example.yaml found"
fi
echo

# ---------- 5) Data directories ----------
echo "[5/5] Ensuring data folders ..."
mkdir -p "$ROOT/data/mtproto" "$ROOT/data/v2ray" "$ROOT/sessions" "$ROOT/logs"
ok "data, sessions, and logs folders ready (catalog.db is created on first run)"
echo

# Optional GUI dependency check
if [[ "$ERR" -eq 0 && -n "$PYTHON" ]]; then
  if ! "$PYTHON" -c "import tkinter" 2>/dev/null; then
    echo "NOTE: Tkinter is not available in this Python."
    echo "      Install it to run the GUI, for example:"
    echo "        Debian/Ubuntu: sudo apt install python3-tk"
    echo "        Fedora:          sudo dnf install python3-tkinter"
    echo "        Arch:            sudo pacman -S tk"
    echo
  fi
fi

echo "========================================"
if [[ "$ERR" -eq 0 ]]; then
  echo " Setup finished successfully."
  echo "========================================"
  echo
  echo "Next steps:"
  echo "  1. Edit config.yaml (telegram.api_id, telegram.api_hash, telegram.sources)"
  echo "  2. Optional: seed data/mtproto/proxies.txt with tg://proxy"
  echo "     links for one-time import into data/catalog.db"
  echo "     (bot falls back to direct if none work)"
  echo "  3. Launch the control panel:"
  echo "       $PYTHON app.pyw"
  echo
  echo "All features (scraper, pings, subscription server) run from the GUI."
  echo "On Linux, \"Open top N proxies\" needs Telegram Desktop or a tg:// handler."
else
  echo " Setup failed. See errors above."
  echo "========================================"
fi
echo

exit "$ERR"
