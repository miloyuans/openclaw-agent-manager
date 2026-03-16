import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict

from core.json5lite import loads_json5


def _as_text(value: Any, fallback: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else fallback


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parts = [x.strip() for x in value.split(",")]
        return [x for x in parts if x]
    return []


def _dedupe_text(values: list[Any]) -> list[str]:
    rows: list[str] = []
    for item in values:
        text = _as_text(item)
        if text and text not in rows:
            rows.append(text)
    return rows


def _stable_id(*parts: str) -> str:
    raw = "_".join(_as_text(p, "") for p in parts if _as_text(p, ""))
    lowered = raw.lower()
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", lowered).strip("_")
    if not cleaned:
        cleaned = "item"
    return cleaned[:60]


def read_openclaw_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}

    try:
        payload = loads_json5(text)
    except Exception:
        payload = None
    if isinstance(payload, dict):
        return payload

    try:
        script = (
            "const fs=require('fs');"
            "const vm=require('vm');"
            "const p=process.argv[1];"
            "let txt=fs.readFileSync(p,'utf8');"
            "if(txt.charCodeAt(0)===0xFEFF){txt=txt.slice(1);}"
            "const obj=vm.runInNewContext('('+txt+')',{});"
            "process.stdout.write(JSON.stringify(obj));"
        )
        result = subprocess.run(
            ["node", "-e", script, str(path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            node_payload = json.loads(result.stdout)
            if isinstance(node_payload, dict):
                return node_payload
    except Exception:
        return {}
    return {}


def discover_agent_hints(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    agents_section = config.get("agents")
    list_section: Any = None
    bindings_section: Any = None

    if isinstance(agents_section, dict):
        list_section = agents_section.get("list")
        bindings_section = agents_section.get("bindings")
    if bindings_section is None:
        bindings_section = config.get("bindings")

    def ensure_agent(agent_id: str) -> Dict[str, Any]:
        aid = _as_text(agent_id)
        if not aid:
            return {}
        if aid not in out:
            out[aid] = {
                "id": aid,
                "model": "",
                "provider": "",
                "chat_entry": "",
                "auth_type": "",
                "token_or_pass": "",
                "model_profile_id": "",
                "channel_ids": [],
                "channel_entries": [],
                "skill_ids": [],
                "mcp_ids": [],
                "source": "openclaw-config",
            }
        return out[aid]

    def merge_binding(agent_id: str, binding: Dict[str, Any], binding_key: str = "") -> None:
        target = ensure_agent(agent_id)
        if not target:
            return

        entry = _as_text(binding.get("entry"), _as_text(binding_key))
        if entry:
            target["chat_entry"] = target.get("chat_entry") or entry
            target["channel_entries"] = _dedupe_text(target.get("channel_entries", []) + [entry])

        model_obj = binding.get("model")
        if isinstance(model_obj, dict):
            model_value = _as_text(model_obj.get("model"), _as_text(model_obj.get("id")))
            provider_value = _as_text(model_obj.get("provider"))
            if model_value:
                target["model"] = model_value
            if provider_value:
                target["provider"] = provider_value
        elif isinstance(model_obj, str):
            target["model"] = _as_text(model_obj)

        model_inline = _as_text(binding.get("model_id"), _as_text(binding.get("model")))
        if model_inline and not target.get("model"):
            target["model"] = model_inline

        profile_id = _as_text(
            binding.get("model_profile_id"),
            _as_text(binding.get("modelProfile"), _as_text(binding.get("profile"))),
        )
        if profile_id:
            target["model_profile_id"] = profile_id

        channels = _as_list(binding.get("channel_ids")) + _as_list(binding.get("channels"))
        channel_single = _as_text(binding.get("channel"))
        if channel_single:
            channels.append(channel_single)
        if channels:
            target["channel_ids"] = _dedupe_text(target.get("channel_ids", []) + channels)

        skills = _as_list(binding.get("skill_ids")) + _as_list(binding.get("skills"))
        if skills:
            target["skill_ids"] = _dedupe_text(target.get("skill_ids", []) + skills)

        mcps = _as_list(binding.get("mcp_ids")) + _as_list(binding.get("mcp"))
        if mcps:
            target["mcp_ids"] = _dedupe_text(target.get("mcp_ids", []) + mcps)

        auth = binding.get("auth")
        if isinstance(auth, dict):
            mode = _as_text(auth.get("type"), _as_text(auth.get("mode")))
            if mode in {"token", "password"}:
                target["auth_type"] = mode
            token_value = _as_text(
                auth.get("token"),
                _as_text(auth.get("apiKey"), _as_text(auth.get("password"))),
            )
            if token_value:
                target["token_or_pass"] = token_value

        auth_type_inline = _as_text(binding.get("auth_type"))
        if auth_type_inline in {"token", "password"}:
            target["auth_type"] = auth_type_inline
        token_inline = _as_text(binding.get("token_or_pass"))
        if token_inline:
            target["token_or_pass"] = token_inline

    if isinstance(list_section, list):
        for item in list_section:
            if isinstance(item, str):
                ensure_agent(item)
            elif isinstance(item, dict):
                agent_id = _as_text(item.get("id"), _as_text(item.get("agent")))
                if not agent_id:
                    continue
                merge_binding(agent_id, item)
    elif isinstance(list_section, dict):
        for key, value in list_section.items():
            agent_id = _as_text(key)
            ensure_agent(agent_id)
            if isinstance(value, dict):
                merge_binding(agent_id, value)
    elif isinstance(list_section, str):
        ensure_agent(list_section)

    if isinstance(bindings_section, list):
        for binding in bindings_section:
            if not isinstance(binding, dict):
                continue
            agent_id = _as_text(
                binding.get("agent"),
                _as_text(binding.get("agentId"), _as_text(binding.get("id"), _as_text(binding.get("target")))),
            )
            if not agent_id:
                continue
            merge_binding(agent_id, binding)
    elif isinstance(bindings_section, dict):
        for key, value in bindings_section.items():
            if isinstance(value, str):
                ensure_agent(value)
                continue
            if not isinstance(value, dict):
                continue
            agent_id = _as_text(
                value.get("agent"),
                _as_text(value.get("agentId"), _as_text(value.get("id"), _as_text(value.get("target")))),
            )
            if not agent_id:
                agent_id = _as_text(key)
            if not agent_id:
                continue
            merge_binding(agent_id, value, binding_key=_as_text(key))

    for value in out.values():
        value["channel_ids"] = _dedupe_text(value.get("channel_ids", []))
        value["channel_entries"] = _dedupe_text(value.get("channel_entries", []))
        value["skill_ids"] = _dedupe_text(value.get("skill_ids", []))
        value["mcp_ids"] = _dedupe_text(value.get("mcp_ids", []))
    return out


def discover_model_hints(config: Dict[str, Any]) -> list[Dict[str, str]]:
    rows: list[Dict[str, str]] = []

    def add_model(provider: Any, model: Any, label: Any = "") -> None:
        model_text = _as_text(model)
        if not model_text:
            return
        provider_text = _as_text(provider, "OpenClaw")
        label_text = _as_text(label, model_text)
        rows.append(
            {
                "provider": provider_text,
                "model": model_text,
                "label": label_text,
                "source": "openclaw-config",
            }
        )

    for key in ("models", "model_providers", "providers"):
        section = config.get(key)
        if isinstance(section, list):
            for item in section:
                if isinstance(item, dict):
                    model_value = _as_text(item.get("model"), _as_text(item.get("id")))
                    add_model(item.get("provider"), model_value, item.get("name") or item.get("label"))
                elif isinstance(item, str):
                    add_model("OpenClaw", item, item)
        elif isinstance(section, dict):
            for provider, value in section.items():
                if isinstance(value, list):
                    for model in value:
                        if isinstance(model, dict):
                            model_value = _as_text(model.get("model"), _as_text(model.get("id")))
                            add_model(provider, model_value, model.get("name") or model.get("label"))
                        else:
                            add_model(provider, model, model)
                elif isinstance(value, dict):
                    for model_key, model_value in value.items():
                        if isinstance(model_value, dict):
                            add_model(
                                provider,
                                _as_text(model_value.get("model"), _as_text(model_value.get("id"), model_key)),
                                model_value.get("name") or model_value.get("label"),
                            )
                        else:
                            add_model(provider, model_key, model_key)

    default_model = _as_text(config.get("model"))
    if default_model:
        add_model("OpenClaw", default_model, default_model)

    for agent in discover_agent_hints(config).values():
        add_model(agent.get("provider"), agent.get("model"), agent.get("model"))

    seen: set[str] = set()
    uniq_rows: list[Dict[str, str]] = []
    for item in rows:
        key = _as_text(item.get("model")).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq_rows.append(
            {
                "provider": _as_text(item.get("provider"), "OpenClaw"),
                "model": _as_text(item.get("model")),
                "label": _as_text(item.get("label"), _as_text(item.get("model"))),
                "source": _as_text(item.get("source"), "openclaw-config"),
            }
        )
    return uniq_rows


def discover_channel_hints(config: Dict[str, Any]) -> list[Dict[str, Any]]:
    channels = config.get("channels")
    if not isinstance(channels, dict):
        return []

    rows: list[Dict[str, Any]] = []

    def add_row(provider: str, account_name: str, value: Dict[str, Any]) -> None:
        entry = _as_text(
            value.get("entry"),
            _as_text(value.get("chat_entry"), f"{provider}:{account_name}"),
        )
        name = _as_text(value.get("name"), _as_text(value.get("title"), f"{provider}-{account_name}"))
        channel_id = _as_text(value.get("id"), f"oc_{_stable_id(provider, account_name)}")
        dm_policy = _as_text(value.get("dmPolicy"), _as_text(value.get("dm_policy")))
        group_policy = _as_text(value.get("groupPolicy"), _as_text(value.get("group_policy")))
        allow_from = _dedupe_text(_as_list(value.get("allowFrom")) + _as_list(value.get("allow_from")))
        group_allow_from = _dedupe_text(
            _as_list(value.get("groupAllowFrom")) + _as_list(value.get("group_allow_from"))
        )

        groups_cfg = value.get("groups")
        require_mention = False
        if isinstance(groups_cfg, dict):
            require_mention = bool(groups_cfg.get("requireMention", groups_cfg.get("require_mention", False)))

        auth_obj = value.get("auth")
        auth_json = ""
        if isinstance(auth_obj, (dict, list)):
            auth_json = json.dumps(auth_obj, ensure_ascii=False, separators=(",", ":"))

        settings_json = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        bot_token = _as_text(value.get("botToken"), _as_text(value.get("bot_token"), _as_text(value.get("token"))))

        rows.append(
            {
                "id": channel_id,
                "name": name,
                "entry": entry,
                "provider": provider,
                "description": _as_text(value.get("description")),
                "enabled": bool(value.get("enabled", True)),
                "auth_json": auth_json,
                "settings_json": settings_json,
                "telegram_bot_token": bot_token if provider == "telegram" else "",
                "telegram_dm_policy": dm_policy if provider == "telegram" else "",
                "telegram_allow_from": ",".join(allow_from) if provider == "telegram" else "",
                "telegram_group_policy": group_policy if provider == "telegram" else "",
                "telegram_group_allow_from": ",".join(group_allow_from) if provider == "telegram" else "",
                "telegram_require_mention": require_mention if provider == "telegram" else False,
                "source": "openclaw-config",
            }
        )

    list_section = channels.get("list")
    if isinstance(list_section, list):
        for item in list_section:
            if not isinstance(item, dict):
                continue
            provider = _as_text(item.get("provider"), "custom")
            account_name = _as_text(item.get("id"), _as_text(item.get("name"), provider))
            add_row(provider, account_name, item)
    elif isinstance(list_section, dict):
        for key, value in list_section.items():
            if not isinstance(value, dict):
                continue
            provider = _as_text(value.get("provider"), "custom")
            add_row(provider, _as_text(key), value)

    for provider, section in channels.items():
        if provider in {"list", "defaultAccount", "default_account"}:
            continue
        if not isinstance(section, dict):
            continue
        accounts = section.get("accounts")
        if isinstance(accounts, dict):
            for account_name, account_cfg in accounts.items():
                if not isinstance(account_cfg, dict):
                    continue
                add_row(provider, _as_text(account_name), account_cfg)
            continue
        if isinstance(accounts, list):
            for idx, account_cfg in enumerate(accounts):
                if not isinstance(account_cfg, dict):
                    continue
                account_name = _as_text(account_cfg.get("id"), _as_text(account_cfg.get("name"), f"{provider}_{idx+1}"))
                add_row(provider, account_name, account_cfg)
            continue

        add_row(provider, _as_text(section.get("id"), provider), section)

    merged: Dict[str, Dict[str, Any]] = {}
    for item in rows:
        cid = _as_text(item.get("id"))
        if cid and cid not in merged:
            merged[cid] = item
    return list(merged.values())
