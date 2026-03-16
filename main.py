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
from core.model_catalog import DEFAULT_MODEL_CATALOG
from core.openclaw_discovery import (
    discover_agent_hints,
    discover_channel_hints,
    discover_model_hints,
    read_openclaw_config,
)
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

# é¡¹ç›®æœ¬åœ°æŒä¹…åŒ–ç›®å½•ï¼ˆç”¨æˆ·ã€é…ç½®ã€åŽ†å²ï¼‰
DATA_DIR = BASE_DIR / "data"
HISTORY_DIR = DATA_DIR / "history"
CONFIG_FILE = DATA_DIR / "manager_config.json"
USERS_FILE = DATA_DIR / "users.json"
VERSIONS_FILE = DATA_DIR / "history_versions.json"
MODEL_CATALOG_FILE = DATA_DIR / "model_catalog.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
MODEL_PROFILES_FILE = DATA_DIR / "model_profiles.json"
SKILLS_FILE = DATA_DIR / "skills.json"
MCP_FILE = DATA_DIR / "mcp_servers.json"

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
        raise HTTPException(status_code=400, detail="ç‰ˆæœ¬å·ä»…æ”¯æŒå­—æ¯æ•°å­—._-ï¼Œé•¿åº¦ 1-40")
    return text


def load_openclaw_config() -> Dict[str, Any]:
    return read_openclaw_config(OPENCLAW_DIR / "openclaw.json")


def read_openclaw_model_hints() -> list[Dict[str, str]]:
    return discover_model_hints(load_openclaw_config())


def read_openclaw_channel_hints() -> list[Dict[str, Any]]:
    return discover_channel_hints(load_openclaw_config())


def read_openclaw_agent_hints() -> Dict[str, Dict[str, Any]]:
    return discover_agent_hints(load_openclaw_config())


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
                    "source": normalize_text(item.get("source"), "manager"),
                }
            )

    # æ”¯æŒé€šè¿‡çŽ¯å¢ƒå˜é‡è¿½åŠ æ¨¡åž‹ï¼š
    # OPENCLAW_MODELS=provider::model,provider::model,plain-model
    # å…¼å®¹ provider:modelï¼ˆprovider éœ€å·²åœ¨å·²çŸ¥æä¾›æ–¹åˆ—è¡¨ä¸­ï¼‰
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
                        "source": "env",
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
                            "source": "env",
                        }
                    )
                else:
                    merged.append({"provider": "Custom", "model": piece, "label": piece, "source": "env"})
            else:
                merged.append({"provider": "Custom", "model": piece, "label": piece, "source": "env"})

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
        source = normalize_text(item.get("source"), "official")
        rows.append({"provider": provider, "model": model, "label": label, "source": source})

    rows.sort(key=lambda x: (x["provider"].lower(), x["label"].lower()))
    return rows


def sync_model_catalog_snapshot() -> list[Dict[str, str]]:
    models = load_model_catalog()
    write_json_file(MODEL_CATALOG_FILE, {"models": models, "synced_at": now_iso()})
    return models


def ensure_model_catalog_file() -> None:
    if MODEL_CATALOG_FILE.exists():
        return
    write_json_file(MODEL_CATALOG_FILE, {"models": []})


def load_manual_model_catalog_items() -> list[Dict[str, Any]]:
    raw = read_json_file(MODEL_CATALOG_FILE, {"models": []})
    models = raw.get("models", []) if isinstance(raw, dict) else []
    if not isinstance(models, list):
        return []
    rows: list[Dict[str, Any]] = []
    for item in models:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def save_manual_model_catalog_items(rows: list[Dict[str, Any]]) -> None:
    write_json_file(MODEL_CATALOG_FILE, {"models": rows, "updated_at": now_iso()})


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
        text = path.read_text(encoding="utf-8")
        if text.startswith("\ufeff"):
            text = text.lstrip("\ufeff")
        return json.loads(text)
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


def ensure_list_file(path: Path, key: str) -> None:
    if path.exists():
        return
    write_json_file(path, {key: []})


def normalize_id_list(raw: Any) -> list[str]:
    rows: list[str] = []
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, str):
        values = [p.strip() for p in raw.split(",")]
    else:
        values = []
    for item in values:
        text = normalize_text(item)
        if text:
            rows.append(text)
    # preserve input order, remove duplicates
    return list(dict.fromkeys(rows))


def normalize_channel_item(item: Dict[str, Any]) -> Dict[str, Any]:
    now = now_iso()
    channel_id = normalize_text(item.get("id"), make_object_id("channel"))
    entry = normalize_text(item.get("entry"), "default")
    name = normalize_text(item.get("name"), entry)
    provider = normalize_text(item.get("provider"), "custom").lower()
    telegram_allow_from = ",".join(normalize_id_list(item.get("telegram_allow_from")))
    telegram_group_allow_from = ",".join(normalize_id_list(item.get("telegram_group_allow_from")))
    return {
        "id": channel_id,
        "name": name,
        "entry": entry,
        "provider": provider,
        "description": normalize_text(item.get("description")),
        "auth_json": normalize_text(item.get("auth_json")),
        "settings_json": normalize_text(item.get("settings_json")),
        "telegram_bot_token": normalize_text(item.get("telegram_bot_token")),
        "telegram_dm_policy": normalize_text(item.get("telegram_dm_policy"), "all"),
        "telegram_allow_from": telegram_allow_from,
        "telegram_group_policy": normalize_text(item.get("telegram_group_policy"), "off"),
        "telegram_group_allow_from": telegram_group_allow_from,
        "telegram_require_mention": bool(item.get("telegram_require_mention", False)),
        "enabled": bool(item.get("enabled", True)),
        "source": normalize_text(item.get("source"), "manager"),
        "created_at": normalize_text(item.get("created_at"), now),
        "updated_at": normalize_text(item.get("updated_at"), now),
    }


def load_local_channels() -> list[Dict[str, Any]]:
    payload = read_json_file(CHANNELS_FILE, {"channels": []})
    raw = payload.get("channels", []) if isinstance(payload, dict) else []
    rows: list[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                normalized = normalize_channel_item(item)
                normalized["source"] = "manager"
                rows.append(normalized)
    return rows


def list_channels() -> list[Dict[str, Any]]:
    rows = load_local_channels()
    local_ids = {item["id"] for item in rows}
    local_entries = {normalize_text(item.get("entry")).lower() for item in rows}
    for hint in read_openclaw_channel_hints():
        normalized = normalize_channel_item(hint)
        if normalized["id"] in local_ids:
            continue
        if normalize_text(normalized.get("entry")).lower() in local_entries:
            continue
        normalized["source"] = "openclaw"
        rows.append(normalized)
    return rows


def load_channels() -> list[Dict[str, Any]]:
    return list_channels()


def save_channels(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for item in rows:
        row = normalize_channel_item(item)
        row["source"] = "manager"
        normalized.append(row)
    write_json_file(CHANNELS_FILE, {"channels": normalized})
    return normalized


def ensure_default_channels() -> None:
    channels = load_local_channels()
    if channels:
        return
    save_channels(
        [
            {
                "id": "channel_default",
                "name": "Default Channel",
                "entry": "default",
                "provider": "custom",
                "description": "Default chat entry",
                "enabled": True,
            }
        ]
    )


def normalize_model_profile_item(item: Dict[str, Any]) -> Dict[str, Any]:
    now = now_iso()
    model = normalize_text(item.get("model"))
    provider = normalize_text(item.get("provider"), "Custom")
    profile_id = normalize_text(item.get("id"), make_object_id("model_profile"))
    if not model:
        raise HTTPException(status_code=400, detail="model cannot be empty")
    return {
        "id": profile_id,
        "name": normalize_text(item.get("name"), model),
        "provider": provider,
        "model": model,
        "auth_mode": normalize_text(item.get("auth_mode"), "shared").lower(),
        "auth_profile": normalize_text(item.get("auth_profile")),
        "auth_value": normalize_text(item.get("auth_value")),
        "base_url": normalize_text(item.get("base_url")),
        "enabled": bool(item.get("enabled", True)),
        "created_at": normalize_text(item.get("created_at"), now),
        "updated_at": normalize_text(item.get("updated_at"), now),
    }


def load_model_profiles() -> list[Dict[str, Any]]:
    payload = read_json_file(MODEL_PROFILES_FILE, {"models": []})
    raw = payload.get("models", []) if isinstance(payload, dict) else []
    rows: list[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                try:
                    rows.append(normalize_model_profile_item(item))
                except HTTPException:
                    continue
    return rows


def save_model_profiles(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for item in rows:
        normalized.append(normalize_model_profile_item(item))
    write_json_file(MODEL_PROFILES_FILE, {"models": normalized})
    return normalized


def ensure_default_model_profile() -> None:
    profiles = load_model_profiles()
    if profiles:
        return
    catalog = load_model_catalog()
    fallback = catalog[0] if catalog else {"provider": "OpenAI", "model": "gpt-4o-mini", "label": "gpt-4o-mini"}
    save_model_profiles(
        [
            {
                "id": "model_default",
                "name": normalize_text(fallback.get("label"), fallback.get("model", "default")),
                "provider": normalize_text(fallback.get("provider"), "Custom"),
                "model": normalize_text(fallback.get("model"), "gpt-4o-mini"),
                "auth_mode": "shared",
                "enabled": True,
            }
        ]
    )


def normalize_skill_item(item: Dict[str, Any]) -> Dict[str, Any]:
    now = now_iso()
    skill_id = normalize_text(item.get("id"), make_object_id("skill"))
    name = normalize_text(item.get("name"), "Unnamed Skill")
    return {
        "id": skill_id,
        "name": name,
        "description": normalize_text(item.get("description")),
        "entry": normalize_text(item.get("entry"), name.lower().replace(" ", "_")),
        "enabled": bool(item.get("enabled", True)),
        "created_at": normalize_text(item.get("created_at"), now),
        "updated_at": normalize_text(item.get("updated_at"), now),
    }


def load_skills() -> list[Dict[str, Any]]:
    payload = read_json_file(SKILLS_FILE, {"skills": []})
    raw = payload.get("skills", []) if isinstance(payload, dict) else []
    rows: list[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                rows.append(normalize_skill_item(item))
    return rows


def save_skills(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    normalized = [normalize_skill_item(item) for item in rows]
    write_json_file(SKILLS_FILE, {"skills": normalized})
    return normalized


def normalize_mcp_item(item: Dict[str, Any]) -> Dict[str, Any]:
    now = now_iso()
    mcp_id = normalize_text(item.get("id"), make_object_id("mcp"))
    name = normalize_text(item.get("name"), "Unnamed MCP")
    return {
        "id": mcp_id,
        "name": name,
        "transport": normalize_text(item.get("transport"), "http"),
        "url": normalize_text(item.get("url")),
        "command": normalize_text(item.get("command")),
        "args": normalize_id_list(item.get("args")),
        "env_json": normalize_text(item.get("env_json")),
        "enabled": bool(item.get("enabled", True)),
        "created_at": normalize_text(item.get("created_at"), now),
        "updated_at": normalize_text(item.get("updated_at"), now),
    }


def load_mcp_servers() -> list[Dict[str, Any]]:
    payload = read_json_file(MCP_FILE, {"mcps": []})
    raw = payload.get("mcps", []) if isinstance(payload, dict) else []
    rows: list[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                rows.append(normalize_mcp_item(item))
    return rows


def save_mcp_servers(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    normalized = [normalize_mcp_item(item) for item in rows]
    write_json_file(MCP_FILE, {"mcps": normalized})
    return normalized


def usage_counter(agents: list[Dict[str, Any]], key: str) -> Dict[str, int]:
    counter: Dict[str, int] = {}
    for agent in agents:
        values = normalize_id_list(agent.get(key))
        for value in values:
            counter[value] = counter.get(value, 0) + 1
        if key.endswith("_ids"):
            single = normalize_text(agent.get(key.replace("_ids", "_id")))
            if single:
                counter[single] = counter.get(single, 0) + 1
    return counter


def summarize_openclaw_basics() -> Dict[str, Any]:
    return {
        "agent_core_required": [
            "id",
            "agents.list",
            "agents.bindings",
            "model",
            "chat entry",
        ],
        "agent_core_recommended": [
            "model_profile_id",
            "channel_ids",
            "skill_ids",
            "mcp_ids",
            "auth profiles",
        ],
        "model_auth": {
            "shared_across_agents": True,
            "reason": "OpenClaw supports shared auth profiles; the same model auth can be reused across agents.",
            "suggested_fields": ["auth_mode", "auth_profile", "auth_value", "base_url", "provider"],
        },
        "channels_design": {
            "independent_management": True,
            "bind_on_agent": True,
            "multi_bind_supported": True,
            "telegram_fields": [
                "botToken",
                "dmPolicy",
                "allowFrom",
                "groupPolicy",
                "groupAllowFrom",
                "groups.requireMention",
            ],
        },
    }

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
        raise HTTPException(status_code=401, detail="æœªç™»å½•æˆ–ä¼šè¯å·²è¿‡æœŸ")
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
        raise HTTPException(status_code=409, detail=f"ç‰ˆæœ¬ {version} å·²å­˜åœ¨")

    folder = version
    backup_path = HISTORY_DIR / folder
    if backup_path.exists():
        raise HTTPException(status_code=409, detail=f"å¤‡ä»½ç›®å½• {folder} å·²å­˜åœ¨")
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
    channel_ids = normalize_id_list(payload.get("channel_ids"))
    if not channel_ids:
        channel_ids = normalize_id_list(payload.get("channels"))
    default_channel_id = normalize_text(payload.get("default_channel_id"))
    if channel_ids and default_channel_id not in channel_ids:
        default_channel_id = channel_ids[0]
    model_profile_id = normalize_text(payload.get("model_profile_id"))
    skill_ids = normalize_id_list(payload.get("skill_ids"))
    mcp_ids = normalize_id_list(payload.get("mcp_ids"))

    normalized = {
        "id": agent_id,
        "auth_type": normalize_text(payload.get("auth_type"), "token"),
        "token_or_pass": normalize_text(payload.get("token_or_pass")),
        "models": models,
        "chats": chats,
        "default_model_id": default_model_id,
        "default_chat_id": default_chat_id,
        "model_profile_id": model_profile_id,
        "channel_ids": channel_ids,
        "default_channel_id": default_channel_id,
        "skill_ids": skill_ids,
        "mcp_ids": mcp_ids,
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
    normalized = sync_agent_bindings(normalize_agent_config(agent_dir.name, payload))
    normalized["updated_at"] = now_iso()
    write_json_file(agent_dir / "agent.json", normalized)
    return normalized


def sync_agent_bindings(agent_payload: Dict[str, Any]) -> Dict[str, Any]:
    current = normalize_agent_config(normalize_text(agent_payload.get("id"), "agent"), agent_payload)

    model_profile_id = normalize_text(current.get("model_profile_id"))
    channel_ids = normalize_id_list(current.get("channel_ids"))
    default_channel_id = normalize_text(current.get("default_channel_id"))

    profiles = {item["id"]: item for item in load_model_profiles() if item.get("enabled", True)}
    channels = {item["id"]: item for item in load_channels() if item.get("enabled", True)}

    # Bind model profile to default model in the agent config.
    if model_profile_id and model_profile_id in profiles:
        profile = profiles[model_profile_id]
        bound_id = f"profile_{model_profile_id}"
        models = list(current.get("models", []))
        target = next((m for m in models if normalize_text(m.get("id")) == bound_id), None)
        if not target:
            target = {
                "id": bound_id,
                "name": normalize_text(profile.get("name"), profile.get("model", "model")),
                "provider": normalize_text(profile.get("provider"), "Custom"),
                "model": normalize_text(profile.get("model"), "gpt-4o-mini"),
            }
            models.insert(0, target)
        else:
            target["name"] = normalize_text(profile.get("name"), profile.get("model", "model"))
            target["provider"] = normalize_text(profile.get("provider"), "Custom")
            target["model"] = normalize_text(profile.get("model"), "gpt-4o-mini")
        current["models"] = models
        current["default_model_id"] = bound_id

    # Bind global channels to chats using stable ids.
    if channel_ids:
        chats = [c for c in list(current.get("chats", [])) if not normalize_text(c.get("id")).startswith("channel_")]
        for channel_id in channel_ids:
            channel = channels.get(channel_id)
            if not channel:
                continue
            chat_id = f"channel_{channel_id}"
            existing = next((c for c in chats if normalize_text(c.get("id")) == chat_id), None)
            if not existing:
                chats.append(
                    {
                        "id": chat_id,
                        "name": normalize_text(channel.get("name"), "channel"),
                        "entry": normalize_text(channel.get("entry"), "default"),
                        "model_id": normalize_text(current.get("default_model_id")),
                    }
                )
            else:
                existing["name"] = normalize_text(channel.get("name"), "channel")
                existing["entry"] = normalize_text(channel.get("entry"), "default")
                if not normalize_text(existing.get("model_id")):
                    existing["model_id"] = normalize_text(current.get("default_model_id"))
        current["chats"] = chats
        if default_channel_id not in channel_ids:
            default_channel_id = channel_ids[0]
        current["default_channel_id"] = default_channel_id
        current["default_chat_id"] = f"channel_{default_channel_id}"

    return normalize_agent_config(normalize_text(current.get("id"), "agent"), current)


def list_agents() -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for payload in list_agent_details():
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
                "id": normalize_text(payload.get("id")),
                "model": (default_model or {}).get("model", payload.get("model", "-")),
                "chat_entry": (default_chat or {}).get("entry", payload.get("chat_entry", "default")),
                "auth_type": payload.get("auth_type", "-"),
                "updated_at": payload.get("updated_at", "-"),
                "models_count": len(payload.get("models", [])),
                "chats_count": len(payload.get("chats", [])),
                "model_profile_id": normalize_text(payload.get("model_profile_id")),
                "channel_count": len(normalize_id_list(payload.get("channel_ids"))),
                "skill_count": len(normalize_id_list(payload.get("skill_ids"))),
                "mcp_count": len(normalize_id_list(payload.get("mcp_ids"))),
                "source": normalize_text(payload.get("source"), "manager"),
            }
        )
    return rows


def build_agent_from_hint(agent_id: str, hint: Dict[str, Any]) -> Dict[str, Any]:
    now = now_iso()
    model_catalog = load_model_catalog()
    fallback_model = normalize_text(
        hint.get("model"),
        normalize_text((model_catalog[0] if model_catalog else {}).get("model"), "gpt-4o-mini"),
    )
    fallback_provider = normalize_text(hint.get("provider"), "OpenClaw")
    fallback_entry = normalize_text(hint.get("chat_entry"), "default")

    channels_by_entry = {
        normalize_text(item.get("entry")).lower(): normalize_text(item.get("id"))
        for item in load_channels()
    }
    channel_ids = normalize_id_list(hint.get("channel_ids"))
    for entry in normalize_id_list(hint.get("channel_entries")):
        mapped = channels_by_entry.get(entry.lower())
        if mapped:
            channel_ids.append(mapped)
    if not channel_ids and fallback_entry.lower() in channels_by_entry:
        channel_ids.append(channels_by_entry[fallback_entry.lower()])
    channel_ids = list(dict.fromkeys(channel_ids))

    model_id = f"hint_model_{agent_id}"
    chat_id = f"hint_chat_{agent_id}"
    payload = {
        "id": agent_id,
        "auth_type": normalize_text(hint.get("auth_type"), "token"),
        "token_or_pass": normalize_text(hint.get("token_or_pass")),
        "models": [
            {
                "id": model_id,
                "name": fallback_model,
                "provider": fallback_provider,
                "model": fallback_model,
            }
        ],
        "chats": [
            {
                "id": chat_id,
                "name": fallback_entry,
                "entry": fallback_entry,
                "model_id": model_id,
            }
        ],
        "default_model_id": model_id,
        "default_chat_id": chat_id,
        "model_profile_id": normalize_text(hint.get("model_profile_id")),
        "channel_ids": channel_ids,
        "default_channel_id": channel_ids[0] if channel_ids else "",
        "skill_ids": normalize_id_list(hint.get("skill_ids")),
        "mcp_ids": normalize_id_list(hint.get("mcp_ids")),
        "created_at": normalize_text(hint.get("created_at"), now),
        "updated_at": normalize_text(hint.get("updated_at"), now),
        "created_by": "openclaw",
        "source": "openclaw",
    }
    normalized = sync_agent_bindings(normalize_agent_config(agent_id, payload))
    normalized["source"] = "openclaw"
    return normalized


def list_agent_details() -> list[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    if AGENTS_DIR.exists():
        for agent_dir in sorted(AGENTS_DIR.iterdir(), key=lambda p: p.name.lower()):
            if not agent_dir.is_dir():
                continue
            payload = read_agent_config(agent_dir)
            payload["source"] = "manager"
            by_id[agent_dir.name] = payload

    for agent_id, hint in read_openclaw_agent_hints().items():
        external = build_agent_from_hint(agent_id, hint)
        current = by_id.get(agent_id)
        if not current:
            by_id[agent_id] = external
            continue

        merged = dict(current)
        for list_key in ("channel_ids", "skill_ids", "mcp_ids"):
            merged[list_key] = list(
                dict.fromkeys(normalize_id_list(merged.get(list_key)) + normalize_id_list(external.get(list_key)))
            )
        if not normalize_text(merged.get("model_profile_id")):
            merged["model_profile_id"] = normalize_text(external.get("model_profile_id"))
        if not normalize_text(merged.get("chat_entry")):
            merged["chat_entry"] = normalize_text(external.get("chat_entry"))
        merged["source"] = "manager+openclaw"
        by_id[agent_id] = sync_agent_bindings(normalize_agent_config(agent_id, merged))
        by_id[agent_id]["source"] = "manager+openclaw"

    return sorted(by_id.values(), key=lambda x: normalize_text(x.get("id")).lower())


def get_agent_dir_or_404(agent_id: str) -> Path:
    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="éžæ³• Agent ID")
    agent_dir = AGENTS_DIR / agent_id
    if not agent_dir.exists() or not agent_dir.is_dir():
        raise HTTPException(status_code=404, detail="Agent ä¸å­˜åœ¨")
    return agent_dir


def get_agent_or_404(agent_id: str) -> Dict[str, Any]:
    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="invalid agent id")
    agent_dir = AGENTS_DIR / agent_id
    if agent_dir.exists() and agent_dir.is_dir():
        payload = read_agent_config(agent_dir)
        payload["source"] = "manager"
        return payload
    hint = read_openclaw_agent_hints().get(agent_id)
    if hint:
        return build_agent_from_hint(agent_id, hint)
    raise HTTPException(status_code=404, detail="agent not found")


def ensure_agent_dir_for_update(agent_id: str) -> Path:
    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="invalid agent id")
    agent_dir = AGENTS_DIR / agent_id
    if agent_dir.exists() and agent_dir.is_dir():
        return agent_dir
    hint = read_openclaw_agent_hints().get(agent_id)
    if not hint:
        raise HTTPException(status_code=404, detail="agent not found")
    agent_dir.mkdir(parents=True, exist_ok=False)
    seeded = build_agent_from_hint(agent_id, hint)
    save_agent_config(agent_dir, seeded)
    return agent_dir


def top_level_changes(old: Dict[str, Any], new: Dict[str, Any]) -> list[str]:
    keys = sorted(set(old.keys()) | set(new.keys()))
    rows: list[str] = []
    for key in keys:
        if old.get(key) != new.get(key):
            rows.append(key)
    return rows


def _default_model_from_catalog() -> Dict[str, str]:
    catalog = load_model_catalog()
    if not catalog:
        return {"provider": "OpenAI", "model": "gpt-4o-mini", "label": "gpt-4o-mini"}
    return {
        "provider": normalize_text(catalog[0].get("provider"), "OpenAI"),
        "model": normalize_text(catalog[0].get("model"), "gpt-4o-mini"),
        "label": normalize_text(catalog[0].get("label"), normalize_text(catalog[0].get("model"), "gpt-4o-mini")),
    }


def build_workbench_agent_candidate(payload: Dict[str, Any], current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    agent_id = normalize_text(payload.get("id"), normalize_text((current or {}).get("id")))
    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="invalid agent id")

    profiles_map = {item["id"]: item for item in load_model_profiles()}
    channels_map = {item["id"]: item for item in load_channels()}

    fallback = _default_model_from_catalog()
    model_profile_id = normalize_text(payload.get("model_profile_id"), normalize_text((current or {}).get("model_profile_id")))

    explicit_model = normalize_text(payload.get("model"))
    explicit_provider = normalize_text(payload.get("model_provider"))
    explicit_name = normalize_text(payload.get("model_name"))
    if not explicit_model and current:
        default_model = next(
            (m for m in current.get("models", []) if normalize_text(m.get("id")) == normalize_text(current.get("default_model_id"))),
            None,
        )
        explicit_model = normalize_text((default_model or {}).get("model"), normalize_text(current.get("model")))
        explicit_provider = normalize_text((default_model or {}).get("provider"), explicit_provider)
        explicit_name = normalize_text((default_model or {}).get("name"), explicit_name)

    if model_profile_id and model_profile_id in profiles_map:
        profile = profiles_map[model_profile_id]
        model = normalize_text(profile.get("model"), explicit_model or fallback["model"])
        model_provider = normalize_text(profile.get("provider"), explicit_provider or fallback["provider"])
        model_name = normalize_text(profile.get("name"), explicit_name or model)
    else:
        model = normalize_text(explicit_model, fallback["model"])
        model_provider = normalize_text(explicit_provider, fallback["provider"])
        model_name = normalize_text(explicit_name, model)

    channels_raw = normalize_id_list(payload.get("channel_ids"))
    if not channels_raw and current:
        channels_raw = normalize_id_list(current.get("channel_ids"))
    channel_ids = [cid for cid in channels_raw if cid in channels_map]
    default_channel_id = normalize_text(payload.get("default_channel_id"), normalize_text((current or {}).get("default_channel_id")))
    if channel_ids and default_channel_id not in channel_ids:
        default_channel_id = channel_ids[0]
    if not channel_ids:
        default_channel_id = ""

    chat_entry = normalize_text(payload.get("chat_entry"), normalize_text((current or {}).get("chat_entry")))
    if not chat_entry and default_channel_id and default_channel_id in channels_map:
        chat_entry = normalize_text(channels_map[default_channel_id].get("entry"), "default")
    if not chat_entry:
        chat_entry = "default"

    auth_type = normalize_text(payload.get("auth_type"), normalize_text((current or {}).get("auth_type"), "token")).lower()
    if auth_type not in {"token", "password"}:
        raise HTTPException(status_code=400, detail="auth_type must be token/password")
    token_or_pass = normalize_text(payload.get("token_or_pass"), normalize_text((current or {}).get("token_or_pass")))

    skill_ids = normalize_id_list(payload.get("skill_ids"))
    if not skill_ids and current:
        skill_ids = normalize_id_list(current.get("skill_ids"))
    mcp_ids = normalize_id_list(payload.get("mcp_ids"))
    if not mcp_ids and current:
        mcp_ids = normalize_id_list(current.get("mcp_ids"))

    now = now_iso()
    model_id = normalize_text((current or {}).get("default_model_id"), make_object_id("model"))
    chat_id = normalize_text((current or {}).get("default_chat_id"), make_object_id("chat"))

    seed = {
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
                "name": normalize_text(payload.get("chat_name"), chat_entry),
                "entry": chat_entry,
                "model_id": model_id,
            }
        ],
        "default_model_id": model_id,
        "default_chat_id": chat_id,
        "model_profile_id": model_profile_id,
        "channel_ids": channel_ids,
        "default_channel_id": default_channel_id,
        "skill_ids": skill_ids,
        "mcp_ids": mcp_ids,
        "created_at": normalize_text((current or {}).get("created_at"), now),
        "updated_at": now,
        "created_by": normalize_text((current or {}).get("created_by"), "workbench"),
    }
    return sync_agent_bindings(normalize_agent_config(agent_id, seed))


def build_openclaw_binding_preview(agent: Dict[str, Any]) -> Dict[str, Any]:
    channel_ids = normalize_id_list(agent.get("channel_ids"))
    chats = [item for item in agent.get("chats", []) if isinstance(item, dict)]
    rows: Dict[str, Any] = {}
    for chat in chats:
        entry = normalize_text(chat.get("entry"))
        if not entry:
            continue
        rows[entry] = {
            "agent": normalize_text(agent.get("id")),
            "entry": entry,
            "model_profile_id": normalize_text(agent.get("model_profile_id")),
            "channel_ids": channel_ids,
        }
    return {"agents": {"list": [normalize_text(agent.get("id"))], "bindings": rows}}


def merge_agent_into_openclaw_config(agent: Dict[str, Any]) -> Dict[str, Any]:
    cfg = load_openclaw_config()
    if not isinstance(cfg, dict):
        cfg = {}
    agents_cfg = cfg.get("agents")
    if not isinstance(agents_cfg, dict):
        agents_cfg = {}

    raw_list = agents_cfg.get("list")
    agent_list: list[str] = []
    if isinstance(raw_list, list):
        agent_list = normalize_id_list(raw_list)
    elif isinstance(raw_list, dict):
        agent_list = normalize_id_list(list(raw_list.keys()))
    elif isinstance(raw_list, str):
        agent_list = [raw_list]

    agent_id = normalize_text(agent.get("id"))
    if agent_id and agent_id not in agent_list:
        agent_list.append(agent_id)

    raw_bindings = agents_cfg.get("bindings")
    bindings: Dict[str, Any] = raw_bindings if isinstance(raw_bindings, dict) else {}
    binding_preview = build_openclaw_binding_preview(agent)
    binding_rows = (
        binding_preview.get("agents", {}).get("bindings", {})
        if isinstance(binding_preview.get("agents"), dict)
        else {}
    )
    if isinstance(binding_rows, dict):
        for entry, value in binding_rows.items():
            bindings[normalize_text(entry)] = value

    agents_cfg["list"] = agent_list
    agents_cfg["bindings"] = bindings
    cfg["agents"] = agents_cfg
    return cfg


def route_test(entry: str) -> list[Dict[str, Any]]:
    wanted = normalize_text(entry).lower()
    if not wanted:
        return []
    rows: list[Dict[str, Any]] = []
    for agent in list_agent_details():
        for chat in agent.get("chats", []):
            chat_entry = normalize_text(chat.get("entry")).lower()
            if chat_entry != wanted:
                continue
            rows.append(
                {
                    "agent_id": normalize_text(agent.get("id")),
                    "chat_id": normalize_text(chat.get("id")),
                    "entry": normalize_text(chat.get("entry")),
                    "source": normalize_text(agent.get("source"), "manager"),
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
        return {"ok": False, "code": -1, "stderr": "æœªæ‰¾åˆ° openclaw å‘½ä»¤"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "code": -1, "stderr": "é‡å¯ Gateway è¶…æ—¶"}


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


# åˆå§‹åŒ–ç›®å½•å’Œé…ç½®
DATA_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)
AGENTS_DIR.mkdir(parents=True, exist_ok=True)

manager_config = load_manager_config()
ensure_model_catalog_file()
ensure_list_file(CHANNELS_FILE, "channels")
ensure_list_file(MODEL_PROFILES_FILE, "models")
ensure_list_file(SKILLS_FILE, "skills")
ensure_list_file(MCP_FILE, "mcps")
ensure_default_channels()
ensure_default_model_profile()
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
        raise HTTPException(status_code=401, detail="ç”¨æˆ·åæˆ–å¯†ç é”™è¯¯")

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
        "index_workbench.html",
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
    agent_details = list_agent_details()
    channel_usage = usage_counter(agent_details, "channel_ids")
    skill_usage = usage_counter(agent_details, "skill_ids")
    mcp_usage = usage_counter(agent_details, "mcp_ids")
    profile_usage = usage_counter(agent_details, "model_profile_id")

    channels = load_channels()
    for item in channels:
        item["usage"] = channel_usage.get(item["id"], 0)

    model_profiles = load_model_profiles()
    for item in model_profiles:
        item["usage"] = profile_usage.get(item["id"], 0)

    skills = load_skills()
    for item in skills:
        item["usage"] = skill_usage.get(item["id"], 0)

    mcps = load_mcp_servers()
    for item in mcps:
        item["usage"] = mcp_usage.get(item["id"], 0)

    return {
        "status": "success",
        "username": user["username"],
        "must_change_password": bool(user.get("must_change_password", False)),
        "agents": list_agents(),
        "backups": list_backups(),
        "model_catalog": load_model_catalog(),
        "suggested_version": suggest_next_version(),
        "channels": channels,
        "model_profiles": model_profiles,
        "skills": skills,
        "mcp_servers": mcps,
        "openclaw_guide": summarize_openclaw_basics(),
        "max_history": manager_config.get("max_history", -1),
        "openclaw_dir": str(OPENCLAW_DIR),
        "gui_available": is_gui_available(),
        "auth_flow_ttl": AUTH_FLOW_TTL_SECONDS,
    }


@app.get("/api/workbench/state")
async def api_workbench_state(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    agents = list_agent_details()
    channel_usage = usage_counter(agents, "channel_ids")
    channels = load_channels()
    for item in channels:
        item["usage"] = channel_usage.get(item["id"], 0)

    profile_usage = usage_counter(agents, "model_profile_id")
    profiles = load_model_profiles()
    for item in profiles:
        item["usage"] = profile_usage.get(item["id"], 0)

    skill_usage = usage_counter(agents, "skill_ids")
    skills = load_skills()
    for item in skills:
        item["usage"] = skill_usage.get(item["id"], 0)

    mcp_usage = usage_counter(agents, "mcp_ids")
    mcps = load_mcp_servers()
    for item in mcps:
        item["usage"] = mcp_usage.get(item["id"], 0)

    openclaw_cfg = load_openclaw_config()
    bindings = []
    agents_cfg = openclaw_cfg.get("agents") if isinstance(openclaw_cfg, dict) else {}
    if isinstance(agents_cfg, dict):
        raw_bindings = agents_cfg.get("bindings")
        if isinstance(raw_bindings, dict):
            for entry, row in raw_bindings.items():
                if isinstance(row, dict):
                    bindings.append(
                        {
                            "entry": normalize_text(entry),
                            "agent": normalize_text(row.get("agent")),
                        }
                    )

    return {
        "status": "success",
        "username": user["username"],
        "must_change_password": bool(user.get("must_change_password", False)),
        "agents": agents,
        "models": load_model_catalog(),
        "model_profiles": profiles,
        "channels": channels,
        "skills": skills,
        "mcp_servers": mcps,
        "bindings": bindings,
        "suggested_version": suggest_next_version(),
        "openclaw_dir": str(OPENCLAW_DIR),
        "gui_available": is_gui_available(),
        "guide": summarize_openclaw_basics(),
    }


@app.post("/api/workbench/preview")
async def api_workbench_preview(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    agent_payload = payload.get("agent", payload)
    if not isinstance(agent_payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    agent_id = normalize_text(agent_payload.get("id"))
    if not agent_id:
        raise HTTPException(status_code=400, detail="agent id cannot be empty")

    existing: Optional[Dict[str, Any]] = None
    try:
        existing = get_agent_or_404(agent_id)
    except HTTPException:
        existing = None

    candidate = build_workbench_agent_candidate(agent_payload, existing)
    binding_preview = build_openclaw_binding_preview(candidate)
    changed = top_level_changes(existing or {}, candidate)
    return {
        "status": "success",
        "existing": existing,
        "candidate": candidate,
        "binding_preview": binding_preview,
        "changed_fields": changed,
    }


@app.post("/api/workbench/apply")
async def api_workbench_apply(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_payload = payload.get("agent", payload)
    if not isinstance(agent_payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    agent_id = normalize_text(agent_payload.get("id"))
    if not agent_id:
        raise HTTPException(status_code=400, detail="agent id cannot be empty")

    existing: Optional[Dict[str, Any]] = None
    try:
        existing = get_agent_or_404(agent_id)
    except HTTPException:
        existing = None

    candidate = build_workbench_agent_candidate(agent_payload, existing)
    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=normalize_text(payload.get("note"), f"workbench apply {agent_id}"),
        created_by=user["username"],
    )

    agent_dir = AGENTS_DIR / agent_id
    if not agent_dir.exists():
        agent_dir.mkdir(parents=True, exist_ok=False)
    saved_agent = save_agent_config(agent_dir, candidate)

    cfg = merge_agent_into_openclaw_config(saved_agent)
    write_json_file(OPENCLAW_DIR / "openclaw.json", cfg)

    restart = bool(payload.get("restart_gateway", True))
    gateway = restart_gateway() if restart else {"ok": True, "code": 0, "stdout": "skip restart", "stderr": ""}
    return {
        "status": "success",
        "backup_version": backup_version,
        "agent": saved_agent,
        "gateway": gateway,
    }


@app.post("/api/workbench/route-test")
async def api_workbench_route_test(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    entry = normalize_text(payload.get("entry"))
    if not entry:
        raise HTTPException(status_code=400, detail="entry cannot be empty")
    matches = route_test(entry)
    return {"status": "success", "entry": entry, "matches": matches}


@app.post("/api/workbench/restart")
async def api_workbench_restart(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "gateway": restart_gateway()}


@app.get("/api/models")
async def api_models(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "models": load_model_catalog()}


@app.post("/api/models/sync")
async def api_models_sync(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    models = sync_model_catalog_snapshot()
    return {
        "status": "success",
        "message": f"synced {len(models)} models",
        "models": models,
    }


@app.get("/api/openclaw-guide")
async def api_openclaw_guide(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "guide": summarize_openclaw_basics()}


@app.post("/api/models/custom")
async def api_add_custom_model(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    model = normalize_text(payload.get("model"))
    if not model:
        raise HTTPException(status_code=400, detail="model cannot be empty")
    provider = normalize_text(payload.get("provider"), "Custom")
    label = normalize_text(payload.get("label"), model)

    models = load_manual_model_catalog_items()
    if any(normalize_text(item.get("model")).lower() == model.lower() for item in load_model_catalog()):
        raise HTTPException(status_code=409, detail="model already exists")
    models.append({"provider": provider, "model": model, "label": label, "source": "manager"})
    save_manual_model_catalog_items(models)
    return {"status": "success", "models": load_model_catalog()}


@app.patch("/api/models/{model_name:path}")
async def api_update_model(model_name: str, request: Request) -> Dict[str, Any]:
    require_api_user(request)
    payload = await parse_payload(request)
    target_model = normalize_text(model_name)
    if not target_model:
        raise HTTPException(status_code=400, detail="model cannot be empty")

    provider = normalize_text(payload.get("provider"))
    label = normalize_text(payload.get("label"))
    renamed_model = normalize_text(payload.get("model"), target_model)
    if renamed_model.lower() != target_model.lower():
        existing = any(
            normalize_text(item.get("model")).lower() == renamed_model.lower() for item in load_model_catalog()
        )
        if existing:
            raise HTTPException(status_code=409, detail="target model already exists")

    manual = load_manual_model_catalog_items()
    idx = next(
        (
            i
            for i, item in enumerate(manual)
            if isinstance(item, dict) and normalize_text(item.get("model")).lower() == target_model.lower()
        ),
        -1,
    )

    if idx == -1:
        merged = next((m for m in load_model_catalog() if normalize_text(m.get("model")).lower() == target_model.lower()), None)
        if not merged:
            raise HTTPException(status_code=404, detail="model not found")
        manual.append(
            {
                "provider": provider or normalize_text(merged.get("provider"), "Custom"),
                "model": renamed_model,
                "label": label or normalize_text(merged.get("label"), renamed_model),
                "source": "manager",
            }
        )
    else:
        item = manual[idx]
        item["provider"] = provider or normalize_text(item.get("provider"), "Custom")
        item["label"] = label or normalize_text(item.get("label"), renamed_model)
        item["model"] = renamed_model
        item["source"] = "manager"
        manual[idx] = item

    save_manual_model_catalog_items(manual)
    return {"status": "success", "models": load_model_catalog()}


@app.get("/api/channels")
async def api_channels(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "channels": load_channels()}


@app.post("/api/channels")
async def api_create_channel(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    channels = load_local_channels()
    existing = list_channels()
    item = normalize_channel_item(
        {
            "id": normalize_text(payload.get("id"), make_object_id("channel")),
            "name": payload.get("name"),
            "entry": payload.get("entry"),
            "provider": payload.get("provider"),
            "description": payload.get("description"),
            "auth_json": payload.get("auth_json"),
            "settings_json": payload.get("settings_json"),
            "telegram_bot_token": payload.get("telegram_bot_token"),
            "telegram_dm_policy": payload.get("telegram_dm_policy"),
            "telegram_allow_from": payload.get("telegram_allow_from"),
            "telegram_group_policy": payload.get("telegram_group_policy"),
            "telegram_group_allow_from": payload.get("telegram_group_allow_from"),
            "telegram_require_mention": payload.get("telegram_require_mention"),
            "enabled": payload.get("enabled", True),
        }
    )
    if any(normalize_text(x.get("id")) == item["id"] for x in existing):
        raise HTTPException(status_code=409, detail="channel id already exists")
    if any(normalize_text(x.get("entry")) == item["entry"] for x in existing):
        raise HTTPException(status_code=409, detail="channel entry already exists")
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"create channel {item['id']}",
        created_by=user["username"],
    )
    channels.append(item)
    save_channels(channels)
    return {"status": "success", "channel": item, "channels": load_channels()}


@app.patch("/api/channels/{channel_id}")
async def api_update_channel(channel_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    channels = load_local_channels()
    target = next((x for x in channels if normalize_text(x.get("id")) == channel_id), None)
    if not target:
        hinted = next((x for x in list_channels() if normalize_text(x.get("id")) == channel_id), None)
        if hinted:
            target = normalize_channel_item(hinted)
            target["source"] = "manager"
            channels.append(target)
    if not target:
        raise HTTPException(status_code=404, detail="channel not found")
    if "name" in payload:
        target["name"] = normalize_text(payload.get("name"), target.get("name", "channel"))
    if "entry" in payload:
        entry = normalize_text(payload.get("entry"))
        if not entry:
            raise HTTPException(status_code=400, detail="entry cannot be empty")
        if any(
            normalize_text(x.get("entry")) == entry and normalize_text(x.get("id")) != channel_id
            for x in list_channels()
        ):
            raise HTTPException(status_code=409, detail="channel entry already exists")
        target["entry"] = entry
    if "provider" in payload:
        target["provider"] = normalize_text(payload.get("provider"), target.get("provider", "custom"))
    if "description" in payload:
        target["description"] = normalize_text(payload.get("description"))
    if "auth_json" in payload:
        target["auth_json"] = normalize_text(payload.get("auth_json"))
    if "settings_json" in payload:
        target["settings_json"] = normalize_text(payload.get("settings_json"))
    if "telegram_bot_token" in payload:
        target["telegram_bot_token"] = normalize_text(payload.get("telegram_bot_token"))
    if "telegram_dm_policy" in payload:
        target["telegram_dm_policy"] = normalize_text(payload.get("telegram_dm_policy"), "all")
    if "telegram_allow_from" in payload:
        target["telegram_allow_from"] = ",".join(normalize_id_list(payload.get("telegram_allow_from")))
    if "telegram_group_policy" in payload:
        target["telegram_group_policy"] = normalize_text(payload.get("telegram_group_policy"), "off")
    if "telegram_group_allow_from" in payload:
        target["telegram_group_allow_from"] = ",".join(normalize_id_list(payload.get("telegram_group_allow_from")))
    if "telegram_require_mention" in payload:
        target["telegram_require_mention"] = bool(payload.get("telegram_require_mention"))
    if "enabled" in payload:
        target["enabled"] = bool(payload.get("enabled"))
    target["updated_at"] = now_iso()
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"update channel {channel_id}",
        created_by=user["username"],
    )
    save_channels(channels)
    return {"status": "success", "channel": target, "channels": load_channels()}


@app.get("/api/model-profiles")
async def api_model_profiles(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "model_profiles": load_model_profiles()}


@app.post("/api/model-profiles")
async def api_create_model_profile(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    profiles = load_model_profiles()
    item = normalize_model_profile_item(
        {
            "id": normalize_text(payload.get("id"), make_object_id("model_profile")),
            "name": payload.get("name"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "auth_mode": payload.get("auth_mode"),
            "auth_profile": payload.get("auth_profile"),
            "auth_value": payload.get("auth_value"),
            "base_url": payload.get("base_url"),
            "enabled": payload.get("enabled", True),
        }
    )
    if any(normalize_text(x.get("id")) == item["id"] for x in profiles):
        raise HTTPException(status_code=409, detail="model profile id already exists")
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"create model profile {item['id']}",
        created_by=user["username"],
    )
    profiles.append(item)
    save_model_profiles(profiles)
    return {"status": "success", "model_profile": item, "model_profiles": load_model_profiles()}


@app.patch("/api/model-profiles/{profile_id}")
async def api_update_model_profile(profile_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    profiles = load_model_profiles()
    target = next((x for x in profiles if normalize_text(x.get("id")) == profile_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="model profile not found")
    for key in ("name", "provider", "model", "auth_mode", "auth_profile", "auth_value", "base_url"):
        if key in payload:
            target[key] = normalize_text(payload.get(key), target.get(key, ""))
    if "enabled" in payload:
        target["enabled"] = bool(payload.get("enabled"))
    target["updated_at"] = now_iso()
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"update model profile {profile_id}",
        created_by=user["username"],
    )
    save_model_profiles(profiles)
    return {"status": "success", "model_profile": target, "model_profiles": load_model_profiles()}


@app.get("/api/skills")
async def api_skills(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "skills": load_skills()}


@app.post("/api/skills")
async def api_create_skill(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    skills = load_skills()
    item = normalize_skill_item(
        {
            "id": normalize_text(payload.get("id"), make_object_id("skill")),
            "name": payload.get("name"),
            "description": payload.get("description"),
            "entry": payload.get("entry"),
            "enabled": payload.get("enabled", True),
        }
    )
    if any(normalize_text(x.get("id")) == item["id"] for x in skills):
        raise HTTPException(status_code=409, detail="skill id already exists")
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"create skill {item['id']}",
        created_by=user["username"],
    )
    skills.append(item)
    save_skills(skills)
    return {"status": "success", "skill": item, "skills": load_skills()}


@app.patch("/api/skills/{skill_id}")
async def api_update_skill(skill_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    skills = load_skills()
    target = next((x for x in skills if normalize_text(x.get("id")) == skill_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="skill not found")
    for key in ("name", "description", "entry"):
        if key in payload:
            target[key] = normalize_text(payload.get(key), target.get(key, ""))
    if "enabled" in payload:
        target["enabled"] = bool(payload.get("enabled"))
    target["updated_at"] = now_iso()
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"update skill {skill_id}",
        created_by=user["username"],
    )
    save_skills(skills)
    return {"status": "success", "skill": target, "skills": load_skills()}


@app.get("/api/mcp-servers")
async def api_mcp_servers(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    return {"status": "success", "mcp_servers": load_mcp_servers()}


@app.post("/api/mcp-servers")
async def api_create_mcp_server(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    servers = load_mcp_servers()
    item = normalize_mcp_item(
        {
            "id": normalize_text(payload.get("id"), make_object_id("mcp")),
            "name": payload.get("name"),
            "transport": payload.get("transport"),
            "url": payload.get("url"),
            "command": payload.get("command"),
            "args": payload.get("args"),
            "env_json": payload.get("env_json"),
            "enabled": payload.get("enabled", True),
        }
    )
    if any(normalize_text(x.get("id")) == item["id"] for x in servers):
        raise HTTPException(status_code=409, detail="mcp id already exists")
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"create mcp {item['id']}",
        created_by=user["username"],
    )
    servers.append(item)
    save_mcp_servers(servers)
    return {"status": "success", "mcp_server": item, "mcp_servers": load_mcp_servers()}


@app.patch("/api/mcp-servers/{mcp_id}")
async def api_update_mcp_server(mcp_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    servers = load_mcp_servers()
    target = next((x for x in servers if normalize_text(x.get("id")) == mcp_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="mcp server not found")
    for key in ("name", "transport", "url", "command", "env_json"):
        if key in payload:
            target[key] = normalize_text(payload.get(key), target.get(key, ""))
    if "args" in payload:
        target["args"] = normalize_id_list(payload.get("args"))
    if "enabled" in payload:
        target["enabled"] = bool(payload.get("enabled"))
    target["updated_at"] = now_iso()
    backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"update mcp {mcp_id}",
        created_by=user["username"],
    )
    save_mcp_servers(servers)
    return {"status": "success", "mcp_server": target, "mcp_servers": load_mcp_servers()}


@app.post("/api/agents")
async def create_agent(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)

    agent_id = normalize_text(payload.get("id"))
    model_profile_id = normalize_text(payload.get("model_profile_id"))
    channel_ids = normalize_id_list(payload.get("channel_ids"))
    skill_ids = normalize_id_list(payload.get("skill_ids"))
    mcp_ids = normalize_id_list(payload.get("mcp_ids"))
    default_channel_id = normalize_text(payload.get("default_channel_id"))
    model = normalize_text(payload.get("model"))
    model_name = normalize_text(payload.get("model_name"), model)
    model_provider = normalize_text(payload.get("model_provider"), "Custom")
    chat_entry = normalize_text(payload.get("chat_entry"))
    chat_name = normalize_text(payload.get("chat_name"), chat_entry or "default")
    auth_type = normalize_text(payload.get("auth_type"), "token").lower()
    token_or_pass = normalize_text(payload.get("token_or_pass"))
    version_label = normalize_text(payload.get("version"))

    profiles = {item["id"]: item for item in load_model_profiles() if item.get("enabled", True)}
    channels = {item["id"]: item for item in load_channels() if item.get("enabled", True)}

    if model_profile_id and model_profile_id in profiles:
        profile = profiles[model_profile_id]
        model = normalize_text(profile.get("model"), model)
        model_name = normalize_text(profile.get("name"), model_name or model)
        model_provider = normalize_text(profile.get("provider"), model_provider)

    valid_channel_ids = [cid for cid in channel_ids if cid in channels]
    if valid_channel_ids:
        channel_ids = valid_channel_ids
    if not channel_ids and channels:
        channel_ids = [next(iter(channels.keys()))]
    if channel_ids:
        if default_channel_id not in channel_ids:
            default_channel_id = channel_ids[0]
        chat_entry = normalize_text(chat_entry, channels[default_channel_id].get("entry", "default"))
        chat_name = normalize_text(chat_name, channels[default_channel_id].get("name", chat_entry))
    else:
        chat_entry = normalize_text(chat_entry, "default")
        chat_name = normalize_text(chat_name, chat_entry)

    if not AGENT_ID_RE.fullmatch(agent_id):
        raise HTTPException(status_code=400, detail="Agent ID ä»…æ”¯æŒå­—æ¯æ•°å­—ã€_ã€-ï¼Œé•¿åº¦ 2-40")
    if not model:
        raise HTTPException(status_code=400, detail="æ¨¡åž‹ä¸èƒ½ä¸ºç©º")
    if auth_type not in {"token", "password"}:
        raise HTTPException(status_code=400, detail="auth_type ä»…æ”¯æŒ token æˆ– password")

    agent_dir = AGENTS_DIR / agent_id
    if agent_dir.exists() or any(item.get("id") == agent_id for item in list_agents()):
        raise HTTPException(status_code=409, detail="Agent å·²å­˜åœ¨")

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
            "model_profile_id": model_profile_id,
            "channel_ids": channel_ids,
            "default_channel_id": default_channel_id,
            "skill_ids": skill_ids,
            "mcp_ids": mcp_ids,
            "created_at": timestamp,
            "updated_at": timestamp,
            "created_by": user["username"],
        },
    )

    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"Agent {agent_id} åˆ›å»ºå®Œæˆ",
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
    return {"status": "success", "agent": get_agent_or_404(agent_id)}


@app.patch("/api/agents/{agent_id}")
async def api_update_agent(agent_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = ensure_agent_dir_for_update(agent_id)
    current = read_agent_config(agent_dir)
    profiles = {item["id"]: item for item in load_model_profiles()}
    channels = {item["id"]: item for item in load_channels()}
    skills = {item["id"]: item for item in load_skills()}
    mcps = {item["id"]: item for item in load_mcp_servers()}

    if "auth_type" in payload:
        auth_type = normalize_text(payload.get("auth_type"), current.get("auth_type", "token")).lower()
        if auth_type not in {"token", "password"}:
            raise HTTPException(status_code=400, detail="auth_type ä»…æ”¯æŒ token æˆ– password")
        current["auth_type"] = auth_type
    if "token_or_pass" in payload:
        current["token_or_pass"] = normalize_text(payload.get("token_or_pass"))

    if "default_model_id" in payload:
        wanted = normalize_text(payload.get("default_model_id"))
        model_ids = {item.get("id") for item in current.get("models", [])}
        if wanted not in model_ids:
            raise HTTPException(status_code=400, detail="default_model_id ä¸å­˜åœ¨")
        current["default_model_id"] = wanted

    if "default_chat_id" in payload:
        wanted = normalize_text(payload.get("default_chat_id"))
        chat_ids = {item.get("id") for item in current.get("chats", [])}
        if wanted not in chat_ids:
            raise HTTPException(status_code=400, detail="default_chat_id ä¸å­˜åœ¨")
        current["default_chat_id"] = wanted

    if "model_profile_id" in payload:
        model_profile_id = normalize_text(payload.get("model_profile_id"))
        if model_profile_id and model_profile_id not in profiles:
            raise HTTPException(status_code=400, detail="model_profile_id not found")
        current["model_profile_id"] = model_profile_id

    if "channel_ids" in payload:
        channel_ids = normalize_id_list(payload.get("channel_ids"))
        for channel_id in channel_ids:
            if channel_id not in channels:
                raise HTTPException(status_code=400, detail=f"channel_id not found: {channel_id}")
        current["channel_ids"] = channel_ids
        if channel_ids and normalize_text(current.get("default_channel_id")) not in channel_ids:
            current["default_channel_id"] = channel_ids[0]

    if "default_channel_id" in payload:
        default_channel_id = normalize_text(payload.get("default_channel_id"))
        channel_ids = normalize_id_list(current.get("channel_ids"))
        if default_channel_id and default_channel_id not in channel_ids:
            raise HTTPException(status_code=400, detail="default_channel_id not found in channel_ids")
        current["default_channel_id"] = default_channel_id

    if "skill_ids" in payload:
        skill_ids = normalize_id_list(payload.get("skill_ids"))
        for skill_id in skill_ids:
            if skill_id not in skills:
                raise HTTPException(status_code=400, detail=f"skill_id not found: {skill_id}")
        current["skill_ids"] = skill_ids

    if "mcp_ids" in payload:
        mcp_ids = normalize_id_list(payload.get("mcp_ids"))
        for mcp_id in mcp_ids:
            if mcp_id not in mcps:
                raise HTTPException(status_code=400, detail=f"mcp_id not found: {mcp_id}")
        current["mcp_ids"] = mcp_ids

    backup_version = backup_current_config(
        version_label=normalize_text(payload.get("version")) or None,
        note=f"update agent {agent_id}",
        created_by=user["username"],
    )
    updated = save_agent_config(agent_dir, current)
    gateway = restart_gateway()
    return {
        "status": "success",
        "message": f"Agent {agent_id} å·²æ›´æ–°",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.post("/api/agents/{agent_id}/models")
async def api_add_agent_model(agent_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = ensure_agent_dir_for_update(agent_id)
    current = read_agent_config(agent_dir)

    model = normalize_text(payload.get("model"))
    if not model:
        raise HTTPException(status_code=400, detail="model ä¸èƒ½ä¸ºç©º")
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
        "message": "æ¨¡åž‹é…ç½®å·²æ–°å¢ž",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.patch("/api/agents/{agent_id}/models/{model_id}")
async def api_patch_agent_model(agent_id: str, model_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = ensure_agent_dir_for_update(agent_id)
    current = read_agent_config(agent_dir)

    models = list(current.get("models", []))
    target = next((m for m in models if normalize_text(m.get("id")) == model_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="æ¨¡åž‹é…ç½®ä¸å­˜åœ¨")

    if "model" in payload:
        value = normalize_text(payload.get("model"))
        if not value:
            raise HTTPException(status_code=400, detail="model ä¸èƒ½ä¸ºç©º")
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
        "message": "æ¨¡åž‹é…ç½®å·²æ›´æ–°",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.post("/api/agents/{agent_id}/chats")
async def api_add_agent_chat(agent_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = ensure_agent_dir_for_update(agent_id)
    current = read_agent_config(agent_dir)

    entry = normalize_text(payload.get("entry") or payload.get("chat_entry"))
    if not entry:
        raise HTTPException(status_code=400, detail="chat entry ä¸èƒ½ä¸ºç©º")

    model_ids = {item.get("id") for item in current.get("models", [])}
    chosen_model_id = normalize_text(payload.get("model_id"), current.get("default_model_id"))
    if chosen_model_id not in model_ids:
        raise HTTPException(status_code=400, detail="model_id ä¸å­˜åœ¨")

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
        "message": "èŠå¤©é…ç½®å·²æ–°å¢ž",
        "backup_version": backup_version,
        "agent": updated,
        "gateway": gateway,
    }


@app.patch("/api/agents/{agent_id}/chats/{chat_id}")
async def api_patch_agent_chat(agent_id: str, chat_id: str, request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    agent_dir = ensure_agent_dir_for_update(agent_id)
    current = read_agent_config(agent_dir)

    chats = list(current.get("chats", []))
    target = next((c for c in chats if normalize_text(c.get("id")) == chat_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="èŠå¤©é…ç½®ä¸å­˜åœ¨")

    if "entry" in payload or "chat_entry" in payload:
        entry = normalize_text(payload.get("entry") or payload.get("chat_entry"))
        if not entry:
            raise HTTPException(status_code=400, detail="chat entry ä¸èƒ½ä¸ºç©º")
        target["entry"] = entry
    if "name" in payload:
        target["name"] = normalize_text(payload.get("name"), target.get("entry", "chat"))
    if "model_id" in payload:
        model_id = normalize_text(payload.get("model_id"))
        model_ids = {item.get("id") for item in current.get("models", [])}
        if model_id not in model_ids:
            raise HTTPException(status_code=400, detail="model_id ä¸å­˜åœ¨")
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
        "message": "èŠå¤©é…ç½®å·²æ›´æ–°",
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
        raise HTTPException(status_code=400, detail="æ¨¡åž‹ä¸èƒ½ä¸ºç©º")

    agent_dirs = [p for p in AGENTS_DIR.iterdir() if p.is_dir()]
    if not agent_dirs:
        raise HTTPException(status_code=400, detail="å½“å‰æ²¡æœ‰å¯æ›´æ–°çš„ Agent")

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
        "message": f"å·²æ›´æ–° {updated} ä¸ª Agent çš„æ¨¡åž‹",
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
        "message": f"å¤‡ä»½ {version} åˆ›å»ºæˆåŠŸ",
        "version": version,
        "backups": list_backups(),
    }


@app.post("/api/rollback")
async def rollback(request: Request) -> Dict[str, Any]:
    user = require_api_user(request)
    payload = await parse_payload(request)
    version = normalize_text(payload.get("version"))
    if not version:
        raise HTTPException(status_code=400, detail="version ä¸èƒ½ä¸ºç©º")

    target_backup = find_backup(version)
    if not target_backup:
        raise HTTPException(status_code=404, detail="æŒ‡å®šç‰ˆæœ¬ä¸å­˜åœ¨")
    backup_path = safe_backup_path(version)
    if not backup_path or not backup_path.exists() or not backup_path.is_dir():
        raise HTTPException(status_code=404, detail="æŒ‡å®šç‰ˆæœ¬ä¸å­˜åœ¨")

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
        "message": f"å·²å›žæ»šåˆ° {version}",
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
        raise HTTPException(status_code=400, detail="login_url ä¸èƒ½ä¸ºç©º")
    parsed = urlparse(login_url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="login_url å¿…é¡»æ˜¯ http/https åœ°å€")

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
        "message": "è¯·åœ¨å¤–éƒ¨æµè§ˆå™¨æ‰“å¼€é‰´æƒ URLï¼Œå®Œæˆç™»å½•åŽä¼šè‡ªåŠ¨å›žè°ƒå¹¶æ•èŽ·å‡­æ®",
    }


@app.get("/api/capture-auth/url-result")
async def capture_auth_url_result(request: Request) -> Dict[str, Any]:
    require_api_user(request)
    cleanup_auth_flows()
    state = str(request.query_params.get("state", "")).strip()
    if not state:
        raise HTTPException(status_code=400, detail="state ä¸èƒ½ä¸ºç©º")
    flow = AUTH_FLOWS.get(state)
    if not flow:
        raise HTTPException(status_code=404, detail="é‰´æƒä¼šè¯ä¸å­˜åœ¨æˆ–å·²è¿‡æœŸ")

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
        "message": "å°šæœªæ”¶åˆ°å›žè°ƒï¼Œè¯·å®Œæˆå¤–éƒ¨æµè§ˆå™¨é‰´æƒ",
        "expires_in": max(0, int(flow.get("expires_at", 0) - time.time())),
    }


@app.post("/api/capture-auth/url-callback-fragment")
async def capture_auth_url_callback_fragment(request: Request) -> Dict[str, Any]:
    payload = await parse_payload(request)
    state = str(payload.get("state", "")).strip()
    fragment = payload.get("fragment", {})
    if not state:
        raise HTTPException(status_code=400, detail="state ä¸èƒ½ä¸ºç©º")
    if not isinstance(fragment, dict):
        fragment = {}
    if state not in AUTH_FLOWS:
        raise HTTPException(status_code=404, detail="é‰´æƒä¼šè¯ä¸å­˜åœ¨æˆ–å·²è¿‡æœŸ")
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
    <h1>æŽˆæƒå›žè°ƒå·²æŽ¥æ”¶</h1>
    <p id="msg">ä½ å¯ä»¥è¿”å›ž OpenClaw Agent Manager é¡µé¢ï¼Œç‚¹å‡»â€œæ£€æŸ¥å›žè°ƒç»“æžœâ€ã€‚</p>
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
        msg.textContent = "å·²ä»Ž URL ç‰‡æ®µæ•èŽ·å‡­æ®ï¼Œä½ å¯ä»¥è¿”å›žç®¡ç†é¡µé¢å®Œæˆç»‘å®šã€‚";
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
        raise HTTPException(status_code=400, detail="login_url ä¸èƒ½ä¸ºç©º")
    parsed = urlparse(login_url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="login_url å¿…é¡»æ˜¯ http/https åœ°å€")

    if prefer_url or not is_gui_available() or not username or not password:
        manual = create_manual_auth_flow(request=request, login_url=login_url)
        return {
            "status": "pending_external_auth",
            "mode": "url",
            "manual": manual,
            "message": "å½“å‰çŽ¯å¢ƒæ— å›¾å½¢ç•Œé¢æˆ–æœªæä¾›è´¦å·å¯†ç ï¼Œè¯·å¤åˆ¶é‰´æƒ URL åˆ°å¯è®¿é—®æµè§ˆå™¨å®ŒæˆæŽˆæƒ",
        }

    script_path = BASE_DIR / "auth_capture.py"
    if not script_path.exists():
        manual = create_manual_auth_flow(request=request, login_url=login_url)
        return {
            "status": "pending_external_auth",
            "mode": "url",
            "manual": manual,
            "message": "æœªæ‰¾åˆ°è‡ªåŠ¨æ•èŽ·è„šæœ¬ï¼Œå·²åˆ‡æ¢åˆ° URL é‰´æƒæ¨¡å¼",
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
            "message": "è‡ªåŠ¨æ•èŽ·è¶…æ—¶ï¼Œå·²åˆ‡æ¢åˆ° URL é‰´æƒæ¨¡å¼",
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
        "message": "å¼¹çª—è‡ªåŠ¨æ•èŽ·å¤±è´¥ï¼Œå·²åˆ‡æ¢åˆ° URL é‰´æƒæ¨¡å¼",
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
        raise HTTPException(status_code=400, detail="æ–°å¯†ç é•¿åº¦è‡³å°‘ 6 ä½")
    if new_password != confirm_password:
        raise HTTPException(status_code=400, detail="ä¸¤æ¬¡æ–°å¯†ç è¾“å…¥ä¸ä¸€è‡´")

    users = load_users()
    current_user = users.get(user["username"])
    if not current_user:
        raise HTTPException(status_code=404, detail="ç”¨æˆ·ä¸å­˜åœ¨")
    if not verify_password(current_password, current_user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="å½“å‰å¯†ç é”™è¯¯")

    current_user["password_hash"] = hash_password(new_password)
    current_user["updated_at"] = now_iso()
    current_user["must_change_password"] = False
    users[user["username"]] = current_user
    save_users(users)

    return {"status": "success", "message": "å¯†ç ä¿®æ”¹æˆåŠŸ"}


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

