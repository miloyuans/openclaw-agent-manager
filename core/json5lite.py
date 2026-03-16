import json
import re
from typing import Any


def _strip_json5_comments(text: str) -> str:
    out: list[str] = []
    in_string = False
    quote_char = ""
    escape = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    length = len(text)

    while i < length:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < length else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if in_string:
            out.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote_char:
                in_string = False
            i += 1
            continue

        if ch in {"'", '"'}:
            in_string = True
            quote_char = ch
            out.append(ch)
            i += 1
            continue

        if ch == "/" and nxt == "/":
            in_line_comment = True
            i += 2
            continue

        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def _strip_trailing_commas(text: str) -> str:
    out: list[str] = []
    in_string = False
    quote_char = ""
    escape = False
    i = 0
    length = len(text)

    while i < length:
        ch = text[i]

        if in_string:
            out.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote_char:
                in_string = False
            i += 1
            continue

        if ch in {"'", '"'}:
            in_string = True
            quote_char = ch
            out.append(ch)
            i += 1
            continue

        if ch == ",":
            j = i + 1
            while j < length and text[j].isspace():
                j += 1
            if j < length and text[j] in {"}", "]"}:
                i += 1
                continue

        out.append(ch)
        i += 1

    return "".join(out)


def _quote_unquoted_keys(text: str) -> str:
    pattern = re.compile(r'([{\[,]\s*)([A-Za-z_][$\w-]*)(\s*:)')
    previous = None
    current = text
    while previous != current:
        previous = current
        current = pattern.sub(r'\1"\2"\3', current)
    return current


def _convert_single_quoted_strings(text: str) -> str:
    pattern = re.compile(r"'([^'\\]*(?:\\.[^'\\]*)*)'")

    def repl(match: re.Match[str]) -> str:
        inner = match.group(1)
        inner = inner.replace("\\'", "'")
        inner = inner.replace('"', '\\"')
        return f'"{inner}"'

    return pattern.sub(repl, text)


def loads_json5(text: str) -> Any:
    cleaned = _strip_json5_comments(text)
    cleaned = _strip_trailing_commas(cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    normalized = _convert_single_quoted_strings(cleaned)
    normalized = _quote_unquoted_keys(normalized)
    return json.loads(normalized)
