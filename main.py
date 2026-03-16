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
VERSIONS_FILE = DATA_DIR / "history_versions.json"
MODEL_CATALOG_FILE = DATA_DIR / "model_catalog.json"

SESSION_COOKIE_NAME = "openclaw_manager_session"
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 7
PASSWORD_ITERATIONS = 260_000
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,39}$")
SEMVER_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
AUTH_FLOW_TTL_SECONDS = 60 * 10
FORCE_HEADLESS = os.getenv("OPENCLAW_HEADLESS", "").lower() in {
    "1",
    "true",
    "yes",
}
STARTUP_BACKUP_ENABLED = os.getenv("OPENCLAW_STARTUP_BACKUP", "1").lower() not in {"0", "false", "no"}
HEADLESS_HOST = os.getenv("OPENCLAW_HOST", "0.0.0.0")
GUI_HOST = os.getenv("OPENCLAW_GUI_HOST", "127.0.0.1")

SESSIONS: Dict[str, Dict[str, Any]] = {}
AUTH_FLOWS: Dict[str, Dict[str, Any]] = {}

template_env = Environment(
    loader=FileSystemLoader(BASE_DIR / "templates"),
    autoescape=select_autoescape(["html", "xml"]),
)

DEFAULT_MODEL_CATALOG = [
    {"provider": "OpenAI", "model": "gpt-5", "label": "GPT-5"},
    {"provider": "OpenAI", "model": "gpt-5-mini", "label": "GPT-5 Mini"},
    {"provider": "OpenAI", "model": "gpt-4.1", "label": "GPT-4.1"},
    {"provider": "OpenAI", "model": "gpt-4o", "label": "GPT-4o"},
    {"provider": "OpenAI", "model": "gpt-4o-mini", "label": "GPT-4o Mini"},
    {"provider": "Anthropic", "model": "claude-3-7-sonnet-latest", "label": "Claude 3.7 Sonnet"},
    {"provider": "Anthropic", "model": "claude-3-5-sonnet-latest", "label": "Claude 3.5 Sonnet"},
    {"provider": "Anthropic", "model": "claude-3-5-haiku-latest", "label": "Claude 3.5 Haiku"},
    {"provider": "Google", "model": "gemini-2.5-pro", "label": "Gemini 2.5 Pro"},
    {"provider": "Google", "model": "gemini-2.5-flash", "label": "Gemini 2.5 Flash"},
    {"provider": "Google", "model": "gemini-2.0-flash", "label": "Gemini 2.0 Flash"},
    {"provider": "xAI", "model": "grok-3-beta", "label": "Grok 3"},
    {"provider": "xAI", "model": "grok-2-latest", "label": "Grok 2"},
    {"provider": "DeepSeek", "model": "deepseek-chat", "label": "DeepSeek Chat"},
    {"provider": "DeepSeek", "model": "deepseek-reasoner", "label": "DeepSeek Reasoner"},
    {"provider": "Qwen", "model": "qwen-max", "label": "Qwen Max"},
    {"provider": "Qwen", "model": "qwen-plus", "label": "Qwen Plus"},
    {"provider": "Qwen", "model": "qwen2.5:14b", "label": "Qwen2.5 14B"},
    {"provider": "Qwen", "model": "qwen2.5:7b", "label": "Qwen2.5 7B"},
    {"provider": "Mistral", "model": "mistral-large-latest", "label": "Mistral Large"},
    {"provider": "Mistral", "model": "mistral-small-latest", "label": "Mistral Small"},
    {"provider": "Meta", "model": "llama-3.3-70b-instruct", "label": "Llama 3.3 70B"},
    {"provider": "Meta", "model": "llama-3.1-70b-instruct", "label": "Llama 3.1 70B"},
    {"provider": "Meta", "model": "llama-3.1-8b-instruct", "label": "Llama 3.1 8B"},
    {"provider": "OpenRouter", "model": "openrouter/auto", "label": "OpenRouter Auto"},
    {"provider": "OpenRouter", "model": "anthropic/claude-3.5-sonnet", "label": "OR Claude 3.5 Sonnet"},
    {"provider": "OpenRouter", "model": "openai/gpt-4o", "label": "OR GPT-4o"},
    {"provider": "Local", "model": "ollama/llama3.1:8b", "label": "Ollama Llama3.1 8B"},
    {"provider": "Local", "model": "ollama/qwen2.5:14b", "label": "Ollama Qwen2.5 14B"},
]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_port_candidate(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.isdigit():
        port = int(text)
        return port if 1 <= port <= 65535 else None

    try:
        parsed = urlparse(text)
        if parsed.port and 1 <= parsed.port <= 65535:
            return parsed.port
    except Exception:
        pass

    match = re.search(r":(\d{1,5})(?:/|$)", text)
    if match:
        port = int(match.group(1))
        return port if 1 <= port <= 65535 else None
    return None


def resolve_app_port(default: int = 8080) -> int:
    # Prefer dedicated manager port env vars first. In container/link environments,
    # OPENCLAW_PORT can be injected as "tcp://host:port" for another service.
    for key in ("OPENCLAW_MANAGER_PORT", "OPENCLAW_AGENT_MANAGER_PORT", "OPENCLAW_PORT", "PORT"):
        raw = os.getenv(key)
        if key == "OPENCLAW_PORT" and isinstance(raw, str) and raw.strip().lower().startswith("tcp://"):
            continue
        parsed = parse_port_candidate(raw)
        if parsed is not None:
            return parsed
    return default


APP_PORT = resolve_app_port(8080)


def make_object_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(4)}"


def normalize_text(value: Any, fallback: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else fallback


def parse_semver(value: str) -> Optional[tuple[int, int, int]]:
    match = SEMVER_RE.fullmatch(value.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def next_semver_from(labels: list[str]) -> str:
    parsed = [parse_semver(label) for label in labels]
    parsed = [item for item in parsed if item is not None]
    if not parsed:
        return "v1.0.0"
    major, minor, patch = sorted(parsed)[-1]
    return f"v{major}.{minor}.{patch + 1}"


def normalize_version_label(raw: Any, fallback_labels: list[str]) -> str:
    text = normalize_text(raw)
    if not text:
        return next_semver_from(fallback_labels)
    text = text.strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,40}", text):
        raise HTTPException(status_code=400, detail="版本号仅支持字母数字._-，长度 1-40")
    return text


def read_openclaw_model_hints() -> list[Dict[str, str]]:
    rows: list[Dict[str, str]] = []
    openclaw_json = OPENCLAW_DIR / "openclaw.json"
    payload = read_json_file(openclaw_json, {})
    if not isinstance(payload, dict):
        return rows

    model_sections = []
    for key in ("models", "model_providers", "providers"):
        item = payload.get(key)
        if item is not None:
            model_sections.append(item)

    for section in model_sections:
        if isinstance(section, list):
            for entry in section:
                if isinstance(entry, dict):
                    model = normalize_text(entry.get("model") or entry.get("id"))
                    if model:
                        rows.append(
                            {
                                "provider": normalize_text(entry.get("provider"), "OpenClaw"),
                                "model": model,
                                "label": normalize_text(entry.get("name"), model),
                            }
                        )
        elif isinstance(section, dict):
            for provider, entry in section.items():
                provider_name = normalize_text(provider, "OpenClaw")
                if isinstance(entry, list):
                    for model in entry:
                        model_name = normalize_text(model)
                        if model_name:
                            rows.append(
                                {
                                    "provider": provider_name,
                                    "model": model_name,
                                    "label": model_name,
                                }
                            )
                elif isinstance(entry, dict):
                    for model_key, model_value in entry.items():
                        if isinstance(model_value, dict):
                            model_name = normalize_text(
                                model_value.get("model") or model_value.get("id") or model_key
                            )
                            if model_name:
                                rows.append(
                                    {
                                        "provider": provider_name,
                                        "model": model_name,
                                        "label": normalize_text(model_value.get("name"), model_name),
                                    }
                                )
                        else:
                            model_name = normalize_text(model_key)
                            if model_name:
                                rows.append(
                                    {
                                        "provider": provider_name,
                                        "model": model_name,
                                        "label": model_name,
                                    }
                                )
    return rows


def load_model_catalog() -> list[Dict[str, str]]:
    payload = read_json_file(MODEL_CATALOG_FILE, {})
    manual_items = payload.get("models", []) if isinstance(payload, dict) else []
    merged = DEFAULT_MODEL_CATALOG + read_openclaw_model_hints()
    if isinstance(manual_items, list):
        for item in manual_items:
            if not isinstance(item, dict):
                continue
            model = normalize_text(item.get("model"))
            if not model:
                continue
            merged.append(
                {
                    "provider": normalize_text(item.get("provider"), "Custom"),
                    "model": model,
                    "label": normalize_text(item.get("label"), model),
                }
            )

    # 支持通过环境变量追加模型：
    # OPENCLAW_MODELS=provider::model,provider::model,plain-model
    # 兼容 provider:model（provider 需已在已知提供方列表中）
    extra_models = normalize_text(os.getenv("OPENCLAW_MODELS", ""))
    if extra_models:
        known_providers = {item["provider"].lower() for item in DEFAULT_MODEL_CATALOG}
        for token in extra_models.split(","):
            piece = token.strip()
            if not piece:
                continue
            if "::" in piece:
                provider, model = piece.split("::", 1)
                merged.append(
                    {
                        "provider": normalize_text(provider, "Custom"),
                        "model": normalize_text(model, piece),
                        "label": normalize_text(model, piece),
                    }
                )
            elif ":" in piece and "/" not in piece:
                provider, model = piece.split(":", 1)
                if provider.strip().lower() in known_providers:
                    merged.append(
                        {
                            "provider": normalize_text(provider, "Custom"),
                            "model": normalize_text(model, piece),
                            "label": normalize_text(model, piece),
                        }
                    )
                else:
                    merged.append({"provider": "Custom", "model": piece, "label": piece})
            else:
                merged.append({"provider": "Custom", "model": piece, "label": piece})

    seen = set()
    rows: list[Dict[str, str]] = []
    for item in merged:
        provider = normalize_text(item.get("provider"), "Custom")
        model = normalize_text(item.get("model"))
        label = normalize_text(item.get("label"), model)
        if not model:
            continue
        key = model.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append({"provider": provider, "model": model, "label": label})

    rows.sort(key=lambda x: (x["provider"].lower(), x["label"].lower()))
    return rows


def ensure_model_catalog_file() -> None:
    if MODEL_CATALOG_FILE.exists():
        return
    write_json_file(MODEL_CATALOG_FILE, {"models": []})


def load_versions_index() -> list[Dict[str, Any]]:
    payload = read_json_file(VERSIONS_FILE, {"versions": []})
    raw_versions = payload.get("versions", []) if isinstance(payload, dict) else []
    rows: list[Dict[str, Any]] = []
    if isinstance(raw_versions, list):
        for item in raw_versions:
            if not isinstance(item, dict):
                continue
            version = normalize_text(item.get("version"))
            folder = normalize_text(item.get("folder"), version)
            if not version or not folder:
                continue
            rows.append(
                {
                    "version": version,
                    "folder": folder,
                    "created_at": normalize_text(item.get("created_at"), now_iso()),
                    "created_by": normalize_text(item.get("created_by"), "system"),
                    "note": normalize_text(item.get("note"), ""),
                }
            )
    return rows


def save_versions_index(versions: list[Dict[str, Any]]) -> None:
    write_json_file(VERSIONS_FILE, {"versions": versions})


def ensure_versions_index() -> list[Dict[str, Any]]:
    versions = load_versions_index()
    known_folders = {item["folder"] for item in versions}
    existing_dirs = [p for p in HISTORY_DIR.iterdir() if p.is_dir()]

    for folder in sorted(existing_dirs, key=lambda p: p.name):
        if folder.name in known_folders:
            continue
        versions.append(
            {
                "version": folder.name,
                "folder": folder.name,
                "created_at": now_iso(),
                "created_by": "legacy",
                "note": "legacy import",
            }
        )

    if versions:
        versions.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        save_versions_index(versions)
    return versions


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
    return HTMLResponse(
        tpl.render(**context),
        media_type="text/html; charset=utf-8",
    )


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
    versions = ensure_versions_index()
    if len(versions) <= max_history:
        return
    keep = versions[:max_history]
    purge = versions[max_history:]
    keep_folders = {item["folder"] for item in keep}
    for old in purge:
        folder = normalize_text(old.get("folder"))
        if not folder or folder in keep_folders:
            continue
        shutil.rmtree(HISTORY_DIR / folder, ignore_errors=True)
    save_versions_index(keep)


def backup_current_config(
    version_label: Optional[str] = None,
    note: str = "",
    created_by: str = "system",
) -> str:
    versions = ensure_versions_index()
    existing_labels = [item["version"] for item in versions]
    version = normalize_version_label(version_label, existing_labels)
    if version in existing_labels:
        raise HTTPException(status_code=409, detail=f"版本 {version} 已存在")

    folder = version
    backup_path = HISTORY_DIR / folder
    if backup_path.exists():
        raise HTTPException(status_code=409, detail=f"备份目录 {folder} 已存在")
    backup_path.mkdir(parents=True, exist_ok=True)

    openclaw_json = OPENCLAW_DIR / "openclaw.json"
    if openclaw_json.exists():
        shutil.copy2(openclaw_json, backup_path / "openclaw.json")
    if AGENTS_DIR.exists():
        shutil.copytree(AGENTS_DIR, backup_path / "agents", dirs_exist_ok=True)

    versions.insert(
        0,
        {
            "version": version,
            "folder": folder,
            "created_at": now_iso(),
            "created_by": normalize_text(created_by, "system"),
            "note": normalize_text(note),
        },
    )
    save_versions_index(versions)
    enforce_history_limit()
    return version


def list_backups() -> list[Dict[str, Any]]:
    versions = ensure_versions_index()
    rows: list[Dict[str, Any]] = []
    for item in versions:
        folder = normalize_text(item.get("folder"), item.get("version"))
        backup_path = HISTORY_DIR / folder
        rows.append(
            {
                "version": normalize_text(item.get("version"), folder),
                "folder": folder,
                "created_at": normalize_text(item.get("created_at"), "-"),
                "created_by": normalize_text(item.get("created_by"), "system"),
                "note": normalize_text(item.get("note"), ""),
                "exists": backup_path.exists(),
            }
        )
    return rows


def find_backup(version: str) -> Optional[Dict[str, Any]]:
    for item in list_backups():
        if item["version"] == version:
            return item
    return None


def suggest_next_version() -> str:
    labels = [item["version"] for item in list_backups()]
    return next_semver_from(labels)


def read_agent_raw_config(agent_dir: Path) -> Dict[str, Any]:
    for config_name in ("agent.json", "config.json"):
        config_path = agent_dir / config_name
        if config_path.exists():
            payload = read_json_file(config_path, {})
            if isinstance(payload, dict):
                return payload
    return {}


def normalize_agent_models(raw_models: Any, fallback_model: str) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if isinstance(raw_models, list):
        for item in raw_models:
            if isinstance(item, dict):
                model_value = normalize_text(item.get("model"))
                if not model_value:
                    continue
                rows.append(
                    {
                        "id": normalize_text(item.get("id"), make_object_id("model")),
                        "name": normalize_text(item.get("name"), model_value),
                        "provider": normalize_text(item.get("provider"), "Custom"),
                        "model": model_value,
                    }
                )
            elif isinstance(item, str) and item.strip():
                model_value = item.strip()
                rows.append(
                    {
                        "id": make_object_id("model"),
                        "name": model_value,
                        "provider": "Custom",
                        "model": model_value,
                    }
                )

    if not rows:
        fallback = fallback_model or "gpt-4o-mini"
        rows.append(
            {
                "id": make_object_id("model"),
                "name": fallback,
                "provider": "Custom",
                "model": fallback,
            }
        )
    return rows


def normalize_agent_chats(raw_chats: Any, fallback_entry: str, default_model_id: str) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if isinstance(raw_chats, list):
        for item in raw_chats:
            if not isinstance(item, dict):
                continue
            entry = normalize_text(item.get("entry") or item.get("chat_entry"), "")
            if not entry:
                continue
            rows.append(
                {
                    "id": normalize_text(item.get("id"), make_object_id("chat")),
                    "name": normalize_text(item.get("name"), entry),
                    "entry": entry,
                    "model_id": normalize_text(item.get("model_id"), default_model_id),
                }
            )

    if not rows:
        entry = fallback_entry or "default"
        rows.append(
            {
                "id": make_object_id("chat"),
                "name": entry,
                "entry": entry,
                "model_id": default_model_id,
            }
        )
    return rows


def normalize_agent_config(agent_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = now_iso()
    fallback_model = normalize_text(payload.get("model"), "gpt-4o-mini")
    models = normalize_agent_models(payload.get("models"), fallback_model)

    default_model_id = normalize_text(payload.get("default_model_id"))
    model_ids = {m["id"] for m in models}
    if default_model_id not in model_ids:
        default_model_id = models[0]["id"]

    default_model = next((m for m in models if m["id"] == default_model_id), models[0])

    fallback_entry = normalize_text(payload.get("chat_entry"), "default")
    chats = normalize_agent_chats(payload.get("chats"), fallback_entry, default_model_id)
    for chat in chats:
        if chat["model_id"] not in model_ids:
            chat["model_id"] = default_model_id

    default_chat_id = normalize_text(payload.get("default_chat_id"))
    chat_ids = {chat["id"] for chat in chats}
    if default_chat_id not in chat_ids:
        default_chat_id = chats[0]["id"]

    default_chat = next((c for c in chats if c["id"] == default_chat_id), chats[0])

    normalized = {
        "id": agent_id,
        "auth_type": normalize_text(payload.get("auth_type"), "token"),
        "token_or_pass": normalize_text(payload.get("token_or_pass")),
        "models": models,
        "chats": chats,
        "default_model_id": default_model_id,
        "default_chat_id": default_chat_id,
        "model": default_model["model"],
        "chat_entry": default_chat["entry"],
        "created_at": normalize_text(payload.get("created_at"), timestamp),
        "updated_at": normalize_text(payload.get("updated_at"), timestamp),
        "created_by": normalize_text(payload.get("created_by"), "system"),
    }
    return normalized


def read_agent_config(agent_dir: Path) -> Dict[str, Any]:
    payload = read_agent_raw_config(agent_dir)
    return normalize_agent_config(agent_dir.name, payload)


def save_agent_config(agent_dir: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_agent_config(agent_dir.name, payload)
    normalized["updated_at"] = now_iso()
    write_json_file(agent_dir / "agent.json", normalized)
    return normalized


def list_agents() -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if not AGENTS_DIR.exists():
        return rows

    for agent_dir in sorted(AGENTS_DIR.iterdir(), key=lambda p: p.name.lower()):
        if not agent_dir.is_dir():
            continue
        payload = read_agent_config(agent_dir)
        default_chat = next(
            (chat for chat in payload.get("chats", []) if chat.get("id") == payload.get("default_chat_id")),
            None,
        )
        default_model = next(
            (m for m in payload.get("models", []) if m.get("id") == payload.get("default_model_id")),
            None,
        )
        rows.append(
            {
                "id": agent_dir.name,
                "model": (default_model or {}).get("model", payload.get("model", "-")),
                "chat_entry": (default_chat or {}).get("entry", payload.get("chat_entry", "default")),
                "auth_type": payload.get("auth_type", "-"),
                "updated_at": payload.get("updated_at", "-"),
                "models_count": len(payload.get("models", [])),
                "chats_count": len(payload.get("chats", [])),
            }
        )
    return rows


def get_agent_dir_or_404(agent_id: str) -> Path:
    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="非法 Agent ID")
    agent_dir = AGENTS_DIR / agent_id
    if not agent_dir.exists() or not agent_dir.is_dir():
        raise HTTPException(status_code=404, detail="Agent 不存在")
    return agent_dir


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
    backup = find_backup(version)
    folder = normalize_text((backup or {}).get("folder"), version)
    candidate = (HISTORY_DIR / folder).resolve()
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
ensure_model_catalog_file()
ensure_default_admin()
ensure_versions_index()
if STARTUP_BACKUP_ENABLED:
    try:
        backup_current_config(note="startup auto backup", created_by="system")
    except HTTPException:
        pass


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
        model_catalog=load_model_catalog(),
        suggested_version=suggest_next_version(),
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
        "model_catalog": load_model_catalog(),
        "suggested_version": suggest_next_version(),
        "max_history": manager_config.get("max_history", -1),
        "openclaw_dir": str(OPENCLAW_DIR),
        "gui_available": is_gui_available(),
        "auth_flow_ttl": AUTH_FLOW_TTL_SECONDS,
    }


@app.get("/api/models")
async def api_models(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "models": load_model_catalog()}


@app.post("/api/agents")
async def create_agent(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)

    agent_id = normalize_text(payload.get("id"))
    model = normalize_text(payload.get("model"))
    model_name = normalize_text(payload.get("model_name"), model)
    model_provider = normalize_text(payload.get("model_provider"), "Custom")
    chat_entry = normalize_text(payload.get("chat_entry"), "default")
    chat_name = normalize_text(payload.get("chat_name"), chat_entry)
    auth_type = normalize_text(payload.get("auth_type"), "token").lower()
    token_or_pass = normalize_text(payload.get("token_or_pass"))
    version_label = normalize_text(payload.get("version"))

    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="Agent ID 仅支持字母数字、_、-，长度 2-40")
    if not model:
        raise HTTPException(status_code=400, detail="模型不能为空")
    if auth_type not in {"token", "password"}:
        raise HTTPException(status_code=400, detail="auth_type 仅支持 token 或 password")

    agent_dir = AGENTS_DIR / agent_id
    if agent_dir.exists():
        raise HTTPException(status_code=409, detail="Agent 已存在")

    backup_version = backup_current_config(
        version_label=version_label or None,
        note=f"create agent {agent_id}",
        created_by=user["username"],
    )
    timestamp = now_iso()
    model_id = make_object_id("model")
    chat_id = make_object_id("chat")

    agent_dir.mkdir(parents=True, exist_ok=False)
    save_agent_config(
        agent_dir,
        {
            "id": agent_id,
            "auth_type": auth_type,
            "token_or_pass": token_or_pass,
            "models": [
                {
                    "id": model_id,
                    "name": model_name,
                    "provider": model_provider,
                    "model": model,
                }
            ],
            "chats": [
                {
                    "id": chat_id,
                    "name": chat_name,
                    "entry": chat_entry,
                    "model_id": model_id,
                }
            ],
            "default_model_id": model_id,
            "default_chat_id": chat_id,
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
        "agent": read_agent_config(agent_dir),
        "gateway": gateway,
    }


@app.post("/create-agent")
async def create_agent_legacy(request: Request) -> Dict[str, Any]:
    return await create_agent(request)


@app.get("/api/agents")
async def api_agents(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "agents": list_agents()}


@app.get("/api/agents/{agent_id}")
async def api_agent_detail(agent_id: str, request: Request) -> Dict[str, Any]:
    require_api_user(request)
    agent_dir = get_agent_dir_or_404(agent_id)
    return {"status": "success", "agent": read_agent_config(agent_dir)}


@app.patch("/api/agents/{agent_id}")
async def api_update_agent(agent_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = get_agent_dir_or_404(agent_id)
    current = read_agent_config(agent_dir)

    if "auth_type" in payload:
        auth_type = normalize_text(payload.get("auth_type"), current.get("auth_type", "token")).lower()
        if auth_type not in {"token", "password"}:
            raise HTTPException(status_code=400, detail="auth_type 仅支持 token 或 password")
        current["auth_type"] = auth_type
    if "token_or_pass" in payload:
        current["token_or_pass"] = normalize_text(payload.get("token_or_pass"))

    if "default_model_id" in payload:
        wanted = normalize_text(payload.get("default_model_id"))
        model_ids = {item.get("id") for item in current.get("models", [])}
        if wanted not in model_ids:
            raise HTTPException(status_code=400, detail="default_model_id 不存在")
        current["default_model_id"] = wanted

    if "default_chat_id" in payload:
        wanted = normalize_text(payload.get("default_chat_id"))
        chat_ids = {item.get("id") for item in current.get("chats", [])}
        if wanted not in chat_ids:
            raise HTTPException(status_code=400, detail="default_chat_id 不存在")
        current["default_chat_id"] = wanted

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"update agent {agent_id}",
        created_by=user["username"],
    )
    updated = save_agent_config(agent_dir, current)
    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"Agent {agent_id} 已更新",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.post("/api/agents/{agent_id}/models")
async def api_add_agent_model(agent_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = get_agent_dir_or_404(agent_id)
    current = read_agent_config(agent_dir)

    model = normalize_text(payload.get("model"))
    if not model:
        raise HTTPException(status_code=400, detail="model 不能为空")
    model_item = {
        "id": make_object_id("model"),
        "name": normalize_text(payload.get("name"), model),
        "provider": normalize_text(payload.get("provider"), "Custom"),
        "model": model,
    }
    current_models = list(current.get("models", []))
    current_models.append(model_item)
    current["models"] = current_models

    if str(payload.get("set_default", "")).lower() in {"1", "true", "yes"}:
        current["default_model_id"] = model_item["id"]

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"add model for {agent_id}",
        created_by=user["username"],
    )
    updated = save_agent_config(agent_dir, current)
    gateway = restart_gateway()
    return {
        "status": "success",
        "message": "模型配置已新增",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.patch("/api/agents/{agent_id}/models/{model_id}")
async def api_patch_agent_model(agent_id: str, model_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = get_agent_dir_or_404(agent_id)
    current = read_agent_config(agent_dir)

    models = list(current.get("models", []))
    target = next((m for m in models if normalize_text(m.get("id")) == model_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="模型配置不存在")

    if "model" in payload:
        value = normalize_text(payload.get("model"))
        if not value:
            raise HTTPException(status_code=400, detail="model 不能为空")
        target["model"] = value
    if "name" in payload:
        target["name"] = normalize_text(payload.get("name"), target.get("model", "model"))
    if "provider" in payload:
        target["provider"] = normalize_text(payload.get("provider"), "Custom")
    if str(payload.get("set_default", "")).lower() in {"1", "true", "yes"}:
        current["default_model_id"] = model_id

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"edit model for {agent_id}",
        created_by=user["username"],
    )
    updated = save_agent_config(agent_dir, current)
    gateway = restart_gateway()
    return {
        "status": "success",
        "message": "模型配置已更新",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.post("/api/agents/{agent_id}/chats")
async def api_add_agent_chat(agent_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = get_agent_dir_or_404(agent_id)
    current = read_agent_config(agent_dir)

    entry = normalize_text(payload.get("entry") or payload.get("chat_entry"))
    if not entry:
        raise HTTPException(status_code=400, detail="chat entry 不能为空")

    model_ids = {item.get("id") for item in current.get("models", [])}
    chosen_model_id = normalize_text(payload.get("model_id"), current.get("default_model_id"))
    if chosen_model_id not in model_ids:
        raise HTTPException(status_code=400, detail="model_id 不存在")

    new_chat = {
        "id": make_object_id("chat"),
        "name": normalize_text(payload.get("name"), entry),
        "entry": entry,
        "model_id": chosen_model_id,
    }
    chats = list(current.get("chats", []))
    chats.append(new_chat)
    current["chats"] = chats
    if str(payload.get("set_default", "")).lower() in {"1", "true", "yes"}:
        current["default_chat_id"] = new_chat["id"]

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"add chat for {agent_id}",
        created_by=user["username"],
    )
    updated = save_agent_config(agent_dir, current)
    gateway = restart_gateway()
    return {
        "status": "success",
        "message": "聊天配置已新增",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.patch("/api/agents/{agent_id}/chats/{chat_id}")
async def api_patch_agent_chat(agent_id: str, chat_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = get_agent_dir_or_404(agent_id)
    current = read_agent_config(agent_dir)

    chats = list(current.get("chats", []))
    target = next((c for c in chats if normalize_text(c.get("id")) == chat_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="聊天配置不存在")

    if "entry" in payload or "chat_entry" in payload:
        entry = normalize_text(payload.get("entry") or payload.get("chat_entry"))
        if not entry:
            raise HTTPException(status_code=400, detail="chat entry 不能为空")
        target["entry"] = entry
    if "name" in payload:
        target["name"] = normalize_text(payload.get("name"), target.get("entry", "chat"))
    if "model_id" in payload:
        model_id = normalize_text(payload.get("model_id"))
        model_ids = {item.get("id") for item in current.get("models", [])}
        if model_id not in model_ids:
            raise HTTPException(status_code=400, detail="model_id 不存在")
        target["model_id"] = model_id
    if str(payload.get("set_default", "")).lower() in {"1", "true", "yes"}:
        current["default_chat_id"] = chat_id

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"edit chat for {agent_id}",
        created_by=user["username"],
    )
    updated = save_agent_config(agent_dir, current)
    gateway = restart_gateway()
    return {
        "status": "success",
        "message": "聊天配置已更新",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.post("/api/model/switch")
async def switch_model(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    model = normalize_text(payload.get("model"))
    model_name = normalize_text(payload.get("model_name"), model)
    provider = normalize_text(payload.get("provider"), "Custom")
    if not model:
        raise HTTPException(status_code=400, detail="模型不能为空")

    agent_dirs = [p for p in AGENTS_DIR.iterdir() if p.is_dir()]
    if not agent_dirs:
        raise HTTPException(status_code=400, detail="当前没有可更新的 Agent")

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"global switch model {model}",
        created_by=user["username"],
    )
    timestamp = now_iso()
    updated = 0
    for agent_dir in agent_dirs:
        current = read_agent_config(agent_dir)
        model_id = normalize_text(current.get("default_model_id"))
        model_rows = list(current.get("models", []))
        target = next((item for item in model_rows if normalize_text(item.get("id")) == model_id), None)
        if target is None:
            target = {
                "id": make_object_id("model"),
                "name": model_name,
                "provider": provider,
                "model": model,
            }
            model_rows.append(target)
            current["default_model_id"] = target["id"]
        else:
            target["name"] = model_name
            target["provider"] = provider
            target["model"] = model
        current["models"] = model_rows
        current["updated_at"] = timestamp
        save_agent_config(agent_dir, current)
        updated += 1

    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"已更新 {updated} 个 Agent 的模型",
        "backup_version": backup_version,
        "gateway": gateway,
    }


@app.get("/api/backups")
async def api_backups(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "backups": list_backups()}


@app.post("/api/backups")
async def create_backup(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=normalize_text(payload.get("note")),
        created_by=user["username"],
    )
    return {
        "status": "success",
        "message": f"备份 {version} 创建成功",
        "version": version,
        "backups": list_backups(),
    }


@app.post("/api/rollback")
async def rollback(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    version = normalize_text(payload.get("version"))
    if not version:
        raise HTTPException(status_code=400, detail="version 不能为空")

    target_backup = find_backup(version)
    if not target_backup:
        raise HTTPException(status_code=404, detail="指定版本不存在")
    backup_path = safe_backup_path(version)
    if not backup_path or not backup_path.exists() or not backup_path.is_dir():
        raise HTTPException(status_code=404, detail="指定版本不存在")

    rollback_backup = backup_current_config(
        version_label=normalize_text(payload.get("rollback_backup_version")) or None,
        note=f"before rollback to {version}",
        created_by=user["username"],
    )

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
        "target_backup": target_backup,
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
