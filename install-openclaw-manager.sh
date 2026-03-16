#!/usr/bin/env bash
set -euo pipefail

# ==================== 配置区 ====================
PROJECT_NAME="openclaw-agent-manager"
INSTALL_DIR="$HOME/.local/share/$PROJECT_NAME"
VENV_DIR="$INSTALL_DIR/venv"
PORT=8080
OPENCLAW_DIR="$HOME/.openclaw"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}开始安装/更新 $PROJECT_NAME (Ubuntu 24.04 支持)${NC}\n"

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
    SUDO="sudo"
fi

HAS_GUI=0
if [ -n "${DISPLAY:-}" ] || [ -n "${WAYLAND_DISPLAY:-}" ]; then
    HAS_GUI=1
fi
if [ "${OPENCLAW_FORCE_HEADLESS:-0}" = "1" ]; then
    HAS_GUI=0
fi
if [ "$HAS_GUI" -eq 1 ]; then
    echo -e "${GREEN}检测到图形环境：将启用桌面弹窗与自动捕获模式${NC}"
else
    echo -e "${YELLOW}未检测到图形环境：将启用 Headless 模式和 URL 鉴权${NC}"
fi

# ==================== 步骤 1: 依赖检查与安装 ====================
echo -e "${YELLOW}检查并安装系统依赖...${NC}"

$SUDO apt update -y

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

ASOUND_PKG="$(pick_apt_pkg libasound2t64 libasound2 || true)"
ATK_BRIDGE_PKG="$(pick_apt_pkg libatk-bridge2.0-0t64 libatk-bridge2.0-0 || true)"
APPINDICATOR_PKG="$(pick_apt_pkg libappindicator3-1 libayatana-appindicator3-1 || true)"

if [ -z "$ASOUND_PKG" ]; then
    echo -e "${RED}未找到可用的 ALSA 依赖包（libasound2t64/libasound2）${NC}"
    exit 1
fi
if [ -z "$ATK_BRIDGE_PKG" ]; then
    echo -e "${RED}未找到可用的 ATK bridge 依赖包${NC}"
    exit 1
fi

$SUDO apt install -y curl git python3 python3-venv python3-pip nodejs npm build-essential libnss3 "$ATK_BRIDGE_PKG" libdrm2 libxkbcommon0 libgbm1 "$ASOUND_PKG" fonts-liberation xdg-utils ${APPINDICATOR_PKG:+$APPINDICATOR_PKG}

# 安装最新 Node.js (OpenClaw 需要 >=22)
if ! command -v node &> /dev/null || [ "$(node -v | cut -d. -f1 | tr -d v)" -lt 22 ]; then
    echo "安装 Node.js 22 LTS..."
    if [ -n "$SUDO" ]; then
        curl -fsSL https://deb.nodesource.com/setup_22.x | $SUDO -E bash -
    else
        curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
    fi
    $SUDO apt install -y nodejs
fi

# 安装 OpenClaw（如果还没装）
if ! command -v openclaw &> /dev/null; then
    echo "安装 OpenClaw..."
    $SUDO npm install -g openclaw@latest
    $SUDO openclaw onboard --install-daemon || true
fi

# ==================== 步骤 2: 创建项目目录 & 虚拟环境 ====================
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# 创建虚拟环境
if [ ! -d "$VENV_DIR" ]; then
    echo "创建 Python 虚拟环境..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# 升级 pip 并安装依赖
pip install --upgrade pip
if [ "$HAS_GUI" -eq 1 ]; then
    pip install pywebview fastapi uvicorn playwright python-dotenv jinja2 psutil
else
    pip install fastapi uvicorn playwright python-dotenv jinja2 psutil
fi

# 安装 Playwright 浏览器
if [ "$HAS_GUI" -eq 1 ]; then
    playwright install chromium --with-deps || playwright install chromium || true
else
    playwright install chromium || true
fi

# ==================== 步骤 3: 同步核心文件 ====================
echo "同步核心文件..."

if [ ! -f "$SCRIPT_DIR/main.py" ] || [ ! -f "$SCRIPT_DIR/auth_capture.py" ]; then
    echo -e "${RED}未在脚本目录找到 main.py 或 auth_capture.py，无法继续。${NC}"
    exit 1
fi

cp "$SCRIPT_DIR/main.py" "$INSTALL_DIR/main.py"
cp "$SCRIPT_DIR/auth_capture.py" "$INSTALL_DIR/auth_capture.py"
cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/requirements.txt" || true

mkdir -p "$INSTALL_DIR/templates"
cp "$SCRIPT_DIR/templates/index.html" "$INSTALL_DIR/templates/index.html"
cp "$SCRIPT_DIR/templates/login.html" "$INSTALL_DIR/templates/login.html"

# ==================== 步骤 4: 创建一键启动/关闭脚本 ====================
cat > start.sh << EOF
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
python main.py
EOF

chmod +x start.sh

cat > stop.sh << EOF
#!/usr/bin/env bash
INSTALL_DIR="$INSTALL_DIR"
pkill -f "python .*openclaw-agent-manager/main.py" || true
echo "已停止 openclaw-agent-manager"
EOF

chmod +x stop.sh

cat > status.sh << EOF
#!/usr/bin/env bash
PORT="$PORT"
if pgrep -f "python .*openclaw-agent-manager/main.py" > /dev/null; then
    echo -e "${GREEN}运行中 (端口 $PORT)${NC}"
    echo "访问: http://localhost:$PORT"
else
    echo -e "${RED}已停止${NC}"
fi
EOF

chmod +x status.sh

# ==================== 步骤 5: 可选 systemd 服务（后台常驻） ====================
cat > openclaw-manager.service << EOF
[Unit]
Description=OpenClaw Agent Manager Web UI
After=network.target

[Service]
User=$USER
WorkingDirectory=$INSTALL_DIR
Environment=OPENCLAW_HEADLESS=$([ "$HAS_GUI" -eq 1 ] && echo 0 || echo 1)
Environment=OPENCLAW_HOST=0.0.0.0
ExecStart=$VENV_DIR/bin/python $INSTALL_DIR/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

$SUDO cp openclaw-manager.service /etc/systemd/system/

echo -e "\n${GREEN}安装完成！${NC}"
echo "使用方式："
echo "  启动（前台）：    cd $INSTALL_DIR && ./start.sh"
echo "  启动（后台服务）： systemctl enable --now openclaw-manager.service"
echo "  关闭后台服务：    systemctl stop openclaw-manager.service"
echo "  查看状态：         ./status.sh"
echo "  访问界面：         http://localhost:$PORT （或你的IP:8080）"
echo "  停止前台运行：     ./stop.sh 或 Ctrl+C"
