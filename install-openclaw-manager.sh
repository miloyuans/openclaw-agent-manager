#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="openclaw-agent-manager"
INSTALL_DIR="$HOME/.local/share/$PROJECT_NAME"
VENV_DIR="$INSTALL_DIR/venv"
PORT="${OPENCLAW_MANAGER_PORT:-8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DEBIAN_FRONTEND=noninteractive

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Installing/updating $PROJECT_NAME (Ubuntu 24.04 compatible)${NC}\n"

if [ "$(id -u)" -ne 0 ]; then
  echo -e "${RED}Please run this script as root.${NC}"
  exit 1
fi

RUN_USER="${SUDO_USER:-$(id -un)}"

HAS_GUI=0
if [ -n "${DISPLAY:-}" ] || [ -n "${WAYLAND_DISPLAY:-}" ]; then
  HAS_GUI=1
fi
if [ "${OPENCLAW_FORCE_HEADLESS:-0}" = "1" ]; then
  HAS_GUI=0
fi

if [ "$HAS_GUI" -eq 1 ]; then
  echo -e "${GREEN}GUI detected: desktop popup capture enabled.${NC}"
else
  echo -e "${YELLOW}No GUI detected: headless + URL auth mode enabled.${NC}"
fi

repair_apt_state() {
  dpkg --configure -a || true
  apt-get -f install -y || true
}

pick_apt_pkg() {
  for pkg in "$@"; do
    candidate="$(apt-cache policy "$pkg" 2>/dev/null | awk '/Candidate:/ {print $2; exit}')"
    if [ -n "${candidate:-}" ] && [ "$candidate" != "(none)" ]; then
      echo "$pkg"
      return 0
    fi
  done
  return 1
}

echo -e "${YELLOW}Checking system dependencies...${NC}"
repair_apt_state
apt-get update -y

ASOUND_PKG="$(pick_apt_pkg libasound2t64 libasound2 || true)"
ATK_BRIDGE_PKG="$(pick_apt_pkg libatk-bridge2.0-0t64 libatk-bridge2.0-0 || true)"
APPINDICATOR_PKG="$(pick_apt_pkg libappindicator3-1 libayatana-appindicator3-1 || true)"

if [ -z "$ASOUND_PKG" ]; then
  echo -e "${RED}Missing ALSA package candidate (libasound2t64/libasound2).${NC}"
  exit 1
fi
if [ -z "$ATK_BRIDGE_PKG" ]; then
  echo -e "${RED}Missing ATK bridge package candidate.${NC}"
  exit 1
fi

# Avoid NodeSource nodejs <-> apt npm conflicts
if dpkg -l npm >/dev/null 2>&1 || dpkg -s npm >/dev/null 2>&1; then
  echo -e "${YELLOW}Purging apt npm package to avoid conflict with NodeSource nodejs...${NC}"
  apt-mark unhold npm >/dev/null 2>&1 || true
  apt-get purge -y npm || true
fi
repair_apt_state

apt-get install -y --no-install-recommends \
  curl git python3 python3-venv python3-pip build-essential libnss3 \
  "$ATK_BRIDGE_PKG" libdrm2 libxkbcommon0 libgbm1 "$ASOUND_PKG" \
  fonts-liberation xdg-utils ${APPINDICATOR_PKG:+$APPINDICATOR_PKG}

NODE_MAJOR=0
if command -v node >/dev/null 2>&1; then
  NODE_MAJOR="$(node -v | sed -E 's/^v([0-9]+).*/\1/' || echo 0)"
fi

if [ "$NODE_MAJOR" -lt 22 ] || ! command -v npm >/dev/null 2>&1; then
  echo -e "${YELLOW}Installing Node.js 22 LTS...${NC}"
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  repair_apt_state
  apt-get install -y nodejs
fi

if ! command -v npm >/dev/null 2>&1; then
  echo -e "${RED}npm is still unavailable after Node.js installation.${NC}"
  exit 1
fi

if ! command -v openclaw >/dev/null 2>&1; then
  echo -e "${YELLOW}Installing OpenClaw CLI...${NC}"
  npm install -g openclaw@latest
  openclaw onboard --install-daemon || true
fi

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

if [ ! -d "$VENV_DIR" ]; then
  echo -e "${YELLOW}Creating Python venv...${NC}"
  python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip

if [ "$HAS_GUI" -eq 1 ]; then
  pip install pywebview fastapi uvicorn playwright python-dotenv jinja2 psutil
else
  pip install fastapi uvicorn playwright python-dotenv jinja2 psutil
fi

if [ "$HAS_GUI" -eq 1 ]; then
  playwright install chromium --with-deps || playwright install chromium || true
else
  playwright install chromium || true
fi

echo -e "${YELLOW}Syncing project files...${NC}"
if [ ! -f "$SCRIPT_DIR/main.py" ] || [ ! -f "$SCRIPT_DIR/auth_capture.py" ]; then
  echo -e "${RED}main.py or auth_capture.py not found in script directory.${NC}"
  exit 1
fi

cp "$SCRIPT_DIR/main.py" "$INSTALL_DIR/main.py"
cp "$SCRIPT_DIR/auth_capture.py" "$INSTALL_DIR/auth_capture.py"
cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/requirements.txt" || true
mkdir -p "$INSTALL_DIR/templates"
cp "$SCRIPT_DIR/templates/index.html" "$INSTALL_DIR/templates/index.html"
cp "$SCRIPT_DIR/templates/login.html" "$INSTALL_DIR/templates/login.html"

cat > "$INSTALL_DIR/start.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
VENV_DIR="$VENV_DIR"
INSTALL_DIR="$INSTALL_DIR"
PORT="$PORT"
source "\$VENV_DIR/bin/activate"
cd "\$INSTALL_DIR"
RUNTIME_GUI=0
if [ -n "\${DISPLAY:-}" ] || [ -n "\${WAYLAND_DISPLAY:-}" ]; then
  RUNTIME_GUI=1
fi
if [ "\${OPENCLAW_FORCE_HEADLESS:-0}" = "1" ]; then
  RUNTIME_GUI=0
fi
if [ "\$RUNTIME_GUI" -eq 1 ]; then
  export OPENCLAW_HEADLESS=0
else
  export OPENCLAW_HEADLESS=1
  export OPENCLAW_HOST=0.0.0.0
fi
export OPENCLAW_MANAGER_PORT="\$PORT"
python3 main.py
EOF
chmod +x "$INSTALL_DIR/start.sh"

cat > "$INSTALL_DIR/stop.sh" <<EOF
#!/usr/bin/env bash
INSTALL_DIR="$INSTALL_DIR"
pkill -f "\$INSTALL_DIR/main.py" || true
echo "Stopped openclaw-agent-manager"
EOF
chmod +x "$INSTALL_DIR/stop.sh"

cat > "$INSTALL_DIR/status.sh" <<EOF
#!/usr/bin/env bash
PORT="$PORT"
INSTALL_DIR="$INSTALL_DIR"
if pgrep -f "\$INSTALL_DIR/main.py" >/dev/null; then
  echo "Running on port \$PORT"
  echo "URL: http://localhost:\$PORT"
else
  echo "Stopped"
fi
EOF
chmod +x "$INSTALL_DIR/status.sh"

cat > "$INSTALL_DIR/openclaw-manager.service" <<EOF
[Unit]
Description=OpenClaw Agent Manager Web UI
After=network.target

[Service]
User=$RUN_USER
WorkingDirectory=$INSTALL_DIR
Environment=OPENCLAW_HEADLESS=$([ "$HAS_GUI" -eq 1 ] && echo 0 || echo 1)
Environment=OPENCLAW_HOST=0.0.0.0
Environment=OPENCLAW_MANAGER_PORT=$PORT
ExecStart=$VENV_DIR/bin/python3 $INSTALL_DIR/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

SYSTEMD_READY=0
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
  SYSTEMD_READY=1
fi

if [ "$SYSTEMD_READY" -eq 1 ]; then
  cp "$INSTALL_DIR/openclaw-manager.service" /etc/systemd/system/
else
  echo -e "${YELLOW}Non-systemd environment detected (common in containers); service install skipped.${NC}"
fi

echo -e "\n${GREEN}Install complete.${NC}"
echo "Run foreground: cd $INSTALL_DIR && ./start.sh"
if [ "$SYSTEMD_READY" -eq 1 ]; then
  echo "Run service: systemctl enable --now openclaw-manager.service"
fi
echo "Check status: cd $INSTALL_DIR && ./status.sh"
echo "Open UI: http://localhost:$PORT"
