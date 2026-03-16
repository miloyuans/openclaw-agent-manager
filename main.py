import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape

try:
    import webview  # type: ignore
except Exception:
    webview = None

load_dotenv()

app = FastAPI(title="OpenClaw Agent Manager")
BASE_DIR = Path(__file__).resolve().parent
OPENCLAW_DIR = Path.home() / ".openclaw"
AGENTS_DIR = OPENCLAW_DIR / "agents"

# 项目本地持久化目录（用户、配置、历史）
DATA_DIR = BASE_DIR / "data"
HISTORY_DIR = DATA_DIR / "history"
CONFIG_FILE = DATA_DIR / "manager_config.json"
USERS_FILE = DATA_DIR / "users.json"

SESSION_COOKIE_NAME = "openclaw_manager_session"
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 7
PASSWORD_ITERATIONS = 260_000
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,39}$")
AUTH_FLOW_TTL_SECONDS = 60 * 10
APP_PORT = int(os.getenv("OPENCLAW_PORT", "8080"))
FORCE_HEADLESS = os.getenv("OPENCLAW_HEADLESS", "").lower() in {
    "1",
    "true",
    "yes",
}
HEADLESS_HOST = os.getenv("OPENCLAW_HOST", "0.0.0.0")
GUI_HOST = os.getenv("OPENCLAW_GUI_HOST", "127.0.0.1")

SESSIONS: Dict[str, Dict[str, Any]] = {}
AUTH_FLOWS: Dict[str, Dict[str, Any]] = {}

template_env = Environment(
    loader=FileSystemLoader(BASE_DIR / "templates"),
    autoescape=select_autoescape(["html", "xml"]),
)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def write_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temp_path.replace(path)


def render_template(name: str, **context: Any) -> HTMLResponse:
    tpl = template_env.get_template(name)
    return HTMLResponse(tpl.render(**context))


def normalize_max_history(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return -1
    return parsed if parsed >= -1 else -1


def load_manager_config() -> Dict[str, int]:
    cfg = read_json_file(CONFIG_FILE, {"max_history": -1})
    if not isinstance(cfg, dict):
        cfg = {"max_history": -1}
    cfg["max_history"] = normalize_max_history(cfg.get("max_history", -1))
    write_json_file(CONFIG_FILE, cfg)
    return cfg


def hash_password(password: str, salt_hex: Optional[str] = None) -> str:
    salt = bytes.fromhex(salt_hex) if salt_hex else os.urandom(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_ITERATIONS,
    )
    return f"{salt.hex()}${digest.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt_hex, digest_hex = stored_hash.split("$", 1)
    except ValueError:
        return False
    recalculated = hash_password(password, salt_hex).split("$", 1)[1]
    return hmac.compare_digest(recalculated, digest_hex)


def load_users() -> Dict[str, Dict[str, Any]]:
    payload = read_json_file(USERS_FILE, {"users": {}})
    raw_users = payload.get("users", {}) if isinstance(payload, dict) else {}
    users: Dict[str, Dict[str, Any]] = {}

    if isinstance(raw_users, list):
        for item in raw_users:
            if not isinstance(item, dict):
                continue
            username = str(item.get("username", "")).strip()
            password_hash = item.get("password_hash")
            if username and isinstance(password_hash, str):
                users[username] = {
                    "password_hash": password_hash,
                    "created_at": item.get("created_at", now_iso()),
                    "updated_at": item.get("updated_at", now_iso()),
                    "must_change_password": bool(
                        item.get("must_change_password", False)
                    ),
                }
    elif isinstance(raw_users, dict):
        for username, item in raw_users.items():
            if not isinstance(item, dict):
                continue
            password_hash = item.get("password_hash")
            if isinstance(password_hash, str) and username:
                users[str(username)] = {
                    "password_hash": password_hash,
                    "created_at": item.get("created_at", now_iso()),
                    "updated_at": item.get("updated_at", now_iso()),
                    "must_change_password": bool(
                        item.get("must_change_password", False)
                    ),
                }

    return users


def save_users(users: Dict[str, Dict[str, Any]]) -> None:
    write_json_file(USERS_FILE, {"users": users})


def ensure_default_admin() -> None:
    users = load_users()
    if "admin" in users:
        return
    timestamp = now_iso()
    users["admin"] = {
        "password_hash": hash_password("admin"),
        "created_at": timestamp,
        "updated_at": timestamp,
        "must_change_password": True,
    }
    save_users(users)


def cleanup_sessions() -> None:
    now = time.time()
    expired_tokens = [
        token
        for token, value in SESSIONS.items()
        if value.get("expires_at", 0) < now
    ]
    for token in expired_tokens:
        SESSIONS.pop(token, None)


def create_session(username: str) -> str:
    cleanup_sessions()
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = {
        "username": username,
        "expires_at": time.time() + SESSION_MAX_AGE_SECONDS,
    }
    return token


def clear_session(request: Request) -> None:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        SESSIONS.pop(token, None)


def get_session_username(request: Request) -> Optional[str]:
    cleanup_sessions()
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    session = SESSIONS.get(token)
    if not session:
        return None
    username = session.get("username")
    if not isinstance(username, str):
        return None
    session["expires_at"] = time.time() + SESSION_MAX_AGE_SECONDS
    return username


def get_current_user(request: Request) -> Optional[Dict[str, Any]]:
    username = get_session_username(request)
    if not username:
        return None
    users = load_users()
    record = users.get(username)
    if not record:
        return None
    result = {"username": username}
    result.update(record)
    return result


def require_api_user(request: Request) -> Dict[str, Any]:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话已过期")
    return user


def is_gui_available() -> bool:
    if FORCE_HEADLESS:
        return False
    if webview is None:
        return False
    if sys.platform.startswith("linux"):
        return bool(os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY"))
    return True


def cleanup_auth_flows() -> None:
    now = time.time()
    expired = [
        state
        for state, item in AUTH_FLOWS.items()
        if item.get("expires_at", 0) < now
    ]
    for state in expired:
        AUTH_FLOWS.pop(state, None)


def create_auth_url(
    login_url: str,
    callback_url: str,
    state: str,
    redirect_param: str = "redirect_uri",
    state_param: str = "state",
) -> str:
    parsed = urlparse(login_url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query[redirect_param] = [callback_url]
    query[state_param] = [state]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def create_manual_auth_flow(
    request: Request,
    login_url: str,
    redirect_param: str = "redirect_uri",
    state_param: str = "state",
) -> Dict[str, Any]:
    cleanup_auth_flows()
    state = secrets.token_urlsafe(20)
    callback_url = str(request.url_for("auth_callback"))
    auth_url = create_auth_url(
        login_url=login_url,
        callback_url=callback_url,
        state=state,
        redirect_param=redirect_param,
        state_param=state_param,
    )
    AUTH_FLOWS[state] = {
        "created_at": time.time(),
        "expires_at": time.time() + AUTH_FLOW_TTL_SECONDS,
        "login_url": login_url,
        "callback_url": callback_url,
        "auth_url": auth_url,
        "captured": {},
        "token": "",
    }
    return {
        "state": state,
        "callback_url": callback_url,
        "auth_url": auth_url,
        "expires_in": AUTH_FLOW_TTL_SECONDS,
    }


def extract_token_from_payload(payload: Dict[str, Any]) -> str:
    priority_keys = [
        "token",
        "access_token",
        "id_token",
        "authToken",
        "authorization",
        "code",
    ]
    for key in priority_keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key, value in payload.items():
        if "token" in key.lower() and isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def save_auth_flow_payload(state: str, payload: Dict[str, Any]) -> None:
    cleanup_auth_flows()
    flow = AUTH_FLOWS.get(state)
    if not flow:
        return
    token = extract_token_from_payload(payload)
    flow["captured"] = payload
    flow["token"] = token
    flow["captured_at"] = now_iso()
    flow["expires_at"] = time.time() + AUTH_FLOW_TTL_SECONDS
    AUTH_FLOWS[state] = flow


def enforce_history_limit() -> None:
    max_history = manager_config.get("max_history", -1)
    if max_history <= 0:
        return
    backups = sorted([p for p in HISTORY_DIR.iterdir() if p.is_dir()], key=lambda p: p.name)
    for old in backups[:-max_history]:
        shutil.rmtree(old, ignore_errors=True)


def backup_current_config() -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    backup_path = HISTORY_DIR / timestamp
    backup_path.mkdir(parents=True, exist_ok=True)

    openclaw_json = OPENCLAW_DIR / "openclaw.json"
    if openclaw_json.exists():
        shutil.copy2(openclaw_json, backup_path / "openclaw.json")
    if AGENTS_DIR.exists():
        shutil.copytree(AGENTS_DIR, backup_path / "agents", dirs_exist_ok=True)

    enforce_history_limit()
    return timestamp


def list_backups() -> list[str]:
    return sorted([p.name for p in HISTORY_DIR.iterdir() if p.is_dir()], reverse=True)


def read_agent_config(agent_dir: Path) -> Dict[str, Any]:
    for config_name in ("agent.json", "config.json"):
        config_path = agent_dir / config_name
        if config_path.exists():
            payload = read_json_file(config_path, {})
            if isinstance(payload, dict):
                return payload
    return {}


def list_agents() -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if not AGENTS_DIR.exists():
        return rows

    for agent_dir in sorted(AGENTS_DIR.iterdir(), key=lambda p: p.name.lower()):
        if not agent_dir.is_dir():
            continue
        payload = read_agent_config(agent_dir)
        rows.append(
            {
                "id": agent_dir.name,
                "model": payload.get("model", "-"),
                "chat_entry": payload.get("chat_entry", "default"),
                "auth_type": payload.get("auth_type", "-"),
                "updated_at": payload.get("updated_at", "-"),
            }
        )
    return rows


def restart_gateway() -> Dict[str, Any]:
    try:
        result = subprocess.run(
            ["openclaw", "gateway", "restart"],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        return {
            "ok": result.returncode == 0,
            "code": result.returncode,
            "stdout": (result.stdout or "").strip()[:300],
            "stderr": (result.stderr or "").strip()[:300],
        }
    except FileNotFoundError:
        return {"ok": False, "code": -1, "stderr": "未找到 openclaw 命令"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "code": -1, "stderr": "重启 Gateway 超时"}


def safe_backup_path(version: str) -> Optional[Path]:
    candidate = (HISTORY_DIR / version).resolve()
    try:
        candidate.relative_to(HISTORY_DIR.resolve())
    except ValueError:
        return None
    return candidate


async def parse_payload(request: Request) -> Dict[str, Any]:
    content_type = request.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        try:
            payload = await request.json()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        form = await request.form()
        return dict(form)
    try:
        payload = await request.json()
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


# 初始化目录和配置
DATA_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)
AGENTS_DIR.mkdir(parents=True, exist_ok=True)

manager_config = load_manager_config()
ensure_default_admin()
backup_current_config()


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if get_current_user(request):
        return RedirectResponse(url="/", status_code=303)
    return render_template("login.html")


@app.post("/api/login")
async def api_login(request: Request) -> JSONResponse:
    payload = await parse_payload(request)
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", ""))

    users = load_users()
    record = users.get(username)
    if not record or not verify_password(password, record.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    token = create_session(username)
    resp = JSONResponse(
        {
            "status": "success",
            "username": username,
            "must_change_password": bool(record.get("must_change_password", False)),
        }
    )
    resp.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
async def api_logout(request: Request) -> JSONResponse:
    clear_session(request)
    resp = JSONResponse({"status": "success"})
    resp.delete_cookie(SESSION_COOKIE_NAME)
    return resp


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    gui_ready = is_gui_available()
    return render_template(
        "index.html",
        username=user["username"],
        must_change_password=bool(user.get("must_change_password", False)),
        agents=list_agents(),
        backups=list_backups(),
        openclaw_dir=str(OPENCLAW_DIR),
        max_history=manager_config.get("max_history", -1),
        gui_available=gui_ready,
        auth_flow_ttl=AUTH_FLOW_TTL_SECONDS,
    )


@app.get("/api/state")
async def api_state(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    return {
        "status": "success",
        "username": user["username"],
        "must_change_password": bool(user.get("must_change_password", False)),
        "agents": list_agents(),
        "backups": list_backups(),
        "max_history": manager_config.get("max_history", -1),
        "openclaw_dir": str(OPENCLAW_DIR),
        "gui_available": is_gui_available(),
        "auth_flow_ttl": AUTH_FLOW_TTL_SECONDS,
    }


@app.post("/api/agents")
async def create_agent(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)

    agent_id = str(payload.get("id", "")).strip()
    model = str(payload.get("model", "")).strip()
    chat_entry = str(payload.get("chat_entry", "default")).strip() or "default"
    auth_type = str(payload.get("auth_type", "token")).strip().lower()
    token_or_pass = str(payload.get("token_or_pass", "")).strip()

    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="Agent ID 仅支持字母数字、_、-，长度 2-40")
    if not model:
        raise HTTPException(status_code=400, detail="模型不能为空")
    if auth_type not in {"token", "password"}:
        raise HTTPException(status_code=400, detail="auth_type 仅支持 token 或 password")

    agent_dir = AGENTS_DIR / agent_id
    if agent_dir.exists():
        raise HTTPException(status_code=409, detail="Agent 已存在")

    backup_version = backup_current_config()
    timestamp = now_iso()
    agent_dir.mkdir(parents=True, exist_ok=False)
    write_json_file(
        agent_dir / "agent.json",
        {
            "id": agent_id,
            "model": model,
            "chat_entry": chat_entry,
            "auth_type": auth_type,
            "token_or_pass": token_or_pass,
            "created_at": timestamp,
            "updated_at": timestamp,
            "created_by": user["username"],
        },
    )

    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"Agent {agent_id} 创建完成",
        "backup_version": backup_version,
        "gateway": gateway,
    }


@app.post("/create-agent")
async def create_agent_legacy(request: Request) -> Dict[str, Any]:
    return await create_agent(request)


@app.post("/api/model/switch")
async def switch_model(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    model = str(payload.get("model", "")).strip()
    if not model:
        raise HTTPException(status_code=400, detail="模型不能为空")

    agent_dirs = [p for p in AGENTS_DIR.iterdir() if p.is_dir()]
    if not agent_dirs:
        raise HTTPException(status_code=400, detail="当前没有可更新的 Agent")

    backup_version = backup_current_config()
    timestamp = now_iso()
    updated = 0
    for agent_dir in agent_dirs:
        current = read_agent_config(agent_dir)
        current["id"] = agent_dir.name
        current["model"] = model
        current["updated_at"] = timestamp
        write_json_file(agent_dir / "agent.json", current)
        updated += 1

    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"已更新 {updated} 个 Agent 的模型",
        "backup_version": backup_version,
        "gateway": gateway,
    }


@app.post("/api/rollback")
async def rollback(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    version = str(payload.get("version", "")).strip()
    if not version:
        raise HTTPException(status_code=400, detail="version 不能为空")

    backup_path = safe_backup_path(version)
    if not backup_path or not backup_path.exists() or not backup_path.is_dir():
        raise HTTPException(status_code=404, detail="指定版本不存在")

    rollback_backup = backup_current_config()

    backup_openclaw_json = backup_path / "openclaw.json"
    target_openclaw_json = OPENCLAW_DIR / "openclaw.json"
    if backup_openclaw_json.exists():
        shutil.copy2(backup_openclaw_json, target_openclaw_json)
    elif target_openclaw_json.exists():
        target_openclaw_json.unlink()

    backup_agents = backup_path / "agents"
    shutil.rmtree(AGENTS_DIR, ignore_errors=True)
    if backup_agents.exists():
        shutil.copytree(backup_agents, AGENTS_DIR)
    else:
        AGENTS_DIR.mkdir(parents=True, exist_ok=True)

    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"已回滚到 {version}",
        "rollback_backup": rollback_backup,
        "gateway": gateway,
    }


@app.post("/rollback")
async def rollback_legacy(request: Request) -> Dict[str, Any]:
    return await rollback(request)


@app.post("/api/capture-auth/url-start")
async def capture_auth_url_start(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    login_url = str(payload.get("login_url", "")).strip()
    redirect_param = str(payload.get("redirect_param", "redirect_uri")).strip() or "redirect_uri"
    state_param = str(payload.get("state_param", "state")).strip() or "state"

    if not login_url:
        raise HTTPException(status_code=400, detail="login_url 不能为空")
    parsed = urlparse(login_url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="login_url 必须是 http/https 地址")

    manual = create_manual_auth_flow(
        request=request,
        login_url=login_url,
        redirect_param=redirect_param,
        state_param=state_param,
    )
    return {
        "status": "pending_external_auth",
        "mode": "url",
        "manual": manual,
        "message": "请在外部浏览器打开鉴权 URL，完成登录后会自动回调并捕获凭据",
    }


@app.get("/api/capture-auth/url-result")
async def capture_auth_url_result(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    cleanup_auth_flows()
    state = str(request.query_params.get("state", "")).strip()
    if not state:
        raise HTTPException(status_code=400, detail="state 不能为空")
    flow = AUTH_FLOWS.get(state)
    if not flow:
        raise HTTPException(status_code=404, detail="鉴权会话不存在或已过期")

    token = str(flow.get("token", "")).strip()
    if token:
        return {
            "status": "success",
            "state": state,
            "token": token,
            "captured": flow.get("captured", {}),
            "captured_at": flow.get("captured_at"),
        }
    return {
        "status": "pending_external_auth",
        "state": state,
        "message": "尚未收到回调，请完成外部浏览器鉴权",
        "expires_in": max(0, int(flow.get("expires_at", 0) - time.time())),
    }


@app.post("/api/capture-auth/url-callback-fragment")
async def capture_auth_url_callback_fragment(request: Request) -> Dict[str, Any]:
    payload = await parse_payload(request)
    state = str(payload.get("state", "")).strip()
    fragment = payload.get("fragment", {})
    if not state:
        raise HTTPException(status_code=400, detail="state 不能为空")
    if not isinstance(fragment, dict):
        fragment = {}
    if state not in AUTH_FLOWS:
        raise HTTPException(status_code=404, detail="鉴权会话不存在或已过期")
    save_auth_flow_payload(state, fragment)
    return {"status": "success"}


@app.get("/auth/callback", response_class=HTMLResponse)
async def auth_callback(request: Request) -> HTMLResponse:
    cleanup_auth_flows()
    state = str(request.query_params.get("state", "")).strip()
    payload = {k: v for k, v in request.query_params.items() if k != "state"}
    if state and state in AUTH_FLOWS:
        save_auth_flow_payload(state, payload)

    state_json = json.dumps(state)
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OpenClaw Auth Callback</title>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #091222;
      color: #eef4ff;
      display: grid;
      place-items: center;
      min-height: 100vh;
      padding: 20px;
    }}
    .box {{
      width: min(560px, 100%);
      border: 1px solid rgba(111, 168, 255, 0.34);
      border-radius: 12px;
      background: rgba(18, 31, 58, 0.86);
      padding: 18px;
    }}
    h1 {{ margin: 0 0 10px; font-size: 1.1rem; }}
    p {{ margin: 0; opacity: 0.9; }}
    .ok {{ color: #8df5c5; }}
  </style>
</head>
<body>
  <div class="box">
    <h1>授权回调已接收</h1>
    <p id="msg">你可以返回 OpenClaw Agent Manager 页面，点击“检查回调结果”。</p>
  </div>
  <script>
    (async function() {{
      const state = {state_json};
      if (!state) return;
      const hash = window.location.hash.startsWith("#") ? window.location.hash.slice(1) : "";
      if (!hash) return;
      const params = Object.fromEntries(new URLSearchParams(hash).entries());
      if (!Object.keys(params).length) return;
      try {{
        await fetch("/api/capture-auth/url-callback-fragment", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ state, fragment: params }})
        }});
        const msg = document.getElementById("msg");
        msg.classList.add("ok");
        msg.textContent = "已从 URL 片段捕获凭据，你可以返回管理页面完成绑定。";
      }} catch (e) {{
        // no-op
      }}
    }})();
  </script>
</body>
</html>"""
    return HTMLResponse(html)


@app.post("/api/capture-auth")
async def capture_auth(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    login_url = str(payload.get("login_url", "")).strip()
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", ""))
    prefer_url = str(payload.get("prefer_url", "")).lower() in {"1", "true", "yes"}

    if not login_url:
        raise HTTPException(status_code=400, detail="login_url 不能为空")
    parsed = urlparse(login_url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="login_url 必须是 http/https 地址")

    if prefer_url or not is_gui_available() or not username or not password:
        manual = create_manual_auth_flow(request=request, login_url=login_url)
        return {
            "status": "pending_external_auth",
            "mode": "url",
            "manual": manual,
            "message": "当前环境无图形界面或未提供账号密码，请复制鉴权 URL 到可访问浏览器完成授权",
        }

    script_path = BASE_DIR / "auth_capture.py"
    if not script_path.exists():
        manual = create_manual_auth_flow(request=request, login_url=login_url)
        return {
            "status": "pending_external_auth",
            "mode": "url",
            "manual": manual,
            "message": "未找到自动捕获脚本，已切换到 URL 鉴权模式",
        }

    env = os.environ.copy()
    env["OPENCLAW_CAPTURE_MODE"] = "headful"
    try:
        result = subprocess.run(
            [sys.executable, str(script_path), login_url, username, password],
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
            env=env,
        )
    except subprocess.TimeoutExpired:
        manual = create_manual_auth_flow(request=request, login_url=login_url)
        return {
            "status": "pending_external_auth",
            "mode": "url",
            "manual": manual,
            "message": "自动捕获超时，已切换到 URL 鉴权模式",
        }

    stdout_lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    token = stdout_lines[-1] if stdout_lines else ""
    if result.returncode == 0 and token and token != "NO_TOKEN_FOUND":
        return {"status": "success", "mode": "popup", "token": token}

    manual = create_manual_auth_flow(request=request, login_url=login_url)
    return {
        "status": "pending_external_auth",
        "mode": "url",
        "manual": manual,
        "message": "弹窗自动捕获失败，已切换到 URL 鉴权模式",
    }


@app.post("/capture-auth")
async def capture_auth_legacy(request: Request) -> Dict[str, Any]:
    return await capture_auth(request)


@app.post("/api/change-password")
async def change_password(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    current_password = str(payload.get("current_password", ""))
    new_password = str(payload.get("new_password", ""))
    confirm_password = str(payload.get("confirm_password", ""))

    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="新密码长度至少 6 位")
    if new_password != confirm_password:
        raise HTTPException(status_code=400, detail="两次新密码输入不一致")

    users = load_users()
    current_user = users.get(user["username"])
    if not current_user:
        raise HTTPException(status_code=404, detail="用户不存在")
    if not verify_password(current_password, current_user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="当前密码错误")

    current_user["password_hash"] = hash_password(new_password)
    current_user["updated_at"] = now_iso()
    current_user["must_change_password"] = False
    users[user["username"]] = current_user
    save_users(users)

    return {"status": "success", "message": "密码修改成功"}


@app.post("/api/settings/history")
async def update_history_setting(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    max_history = normalize_max_history(payload.get("max_history", -1))
    manager_config["max_history"] = max_history
    write_json_file(CONFIG_FILE, manager_config)
    enforce_history_limit()
    return {"status": "success", "max_history": max_history}


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    if is_gui_available():
        threading.Thread(
            target=lambda: uvicorn.run(app, host=GUI_HOST, port=APP_PORT),
            daemon=True,
        ).start()
        webview.create_window(
            title="OpenClaw Agent Manager",
            url=f"http://{GUI_HOST}:{APP_PORT}",
            width=1320,
            height=920,
            resizable=True,
        )
        webview.start()
    else:
        print(
            f"[OpenClaw-Agent-Manager] Headless mode enabled, serving on http://{HEADLESS_HOST}:{APP_PORT}"
        )
        uvicorn.run(app, host=HEADLESS_HOST, port=APP_PORT)
