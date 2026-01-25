import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests


def mask(value: Optional[str]) -> str:
    if not value:
        return ""
    if len(value) <= 10:
        return "***"
    return f"{value[:6]}***{value[-4:]}"


def log_kv(label: str, data: Dict[str, Any]) -> None:
    safe = {}
    for key, val in data.items():
        if isinstance(val, str) and any(token in key.lower() for token in ("token", "secret", "code")):
            safe[key] = mask(val)
        else:
            safe[key] = val
    print(f"{label}: {safe}")


def request_json(method: str, url: str, *, params: Optional[Dict[str, Any]] = None,
                 json_body: Optional[Dict[str, Any]] = None,
                 raw_body: Optional[str] = None,
                 headers: Optional[Dict[str, str]] = None,
                 timeout: int = 40) -> Dict[str, Any]:
    print(f"> {method} {url}")
    if params:
        log_kv("  params", params)
    if json_body:
        log_kv("  json", json_body)
    if raw_body is not None:
        if headers is None:
            headers = {}
        headers.setdefault("Content-Type", "application/json")
        response = requests.request(method, url, params=params, data=raw_body, headers=headers, timeout=timeout)
    else:
        response = requests.request(method, url, params=params, json=json_body, headers=headers, timeout=timeout)
    print(f"< {response.status_code} {response.reason}")
    content_type = response.headers.get("Content-Type", "")
    print(f"< content-type: {content_type}")
    if response.status_code == 204:
        return {}
    if response.status_code >= 400:
        body = response.text.strip()
        if body:
            snippet = body[:800]
            print(f"< error body (first 800 chars): {snippet}")
        raise RuntimeError(f"HTTP {response.status_code} error")
    if not response.text:
        raise RuntimeError("Empty response body")
    try:
        return response.json()
    except json.JSONDecodeError:
        snippet = response.text[:600]
        print(f"< body (first 600 chars): {snippet}")
        raise RuntimeError("Invalid JSON response")


def load_env() -> Dict[str, str]:
    load_dotenv()
    return {
        "base_url": os.getenv("AMO_BASE_URL", "").rstrip("/"),
        "client_id": os.getenv("AMO_CLIENT_ID", ""),
        "client_secret": os.getenv("AMO_CLIENT_SECRET", ""),
        "redirect_uri": os.getenv("AMO_REDIRECT_URI", ""),
        "auth_code": os.getenv("AMO_AUTH_CODE", ""),
        "tokens_file": os.getenv("AMO_TOKENS_FILE", "tokens.json"),
        "force_code": os.getenv("AMO_FORCE_CODE", ""),
        "use_raw": os.getenv("AMO_USE_RAW_REQUEST", ""),
        "print_curl": os.getenv("AMO_PRINT_CURL", ""),
        "long_token": os.getenv("AMO_LONG_TOKEN", ""),
    }


def is_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_dotenv() -> None:
    env_path = Path(os.getenv("AMO_DOTENV_PATH", Path(__file__).resolve().parent / ".env"))
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def require_env(env: Dict[str, str], keys: list[str]) -> None:
    missing = [k for k in keys if not env.get(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")


def exchange_code(env: Dict[str, str], code: str) -> Dict[str, Any]:
    require_env(env, ["base_url", "client_id", "client_secret", "redirect_uri"])
    payload = {
        "client_id": env["client_id"],
        "client_secret": env["client_secret"],
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": env["redirect_uri"],
    }
    url = f"{env['base_url']}/oauth2/access_token"
    use_raw = is_truthy(env.get("use_raw"))
    print_curl = is_truthy(env.get("print_curl"))
    if print_curl:
        body = json.dumps(payload, ensure_ascii=False)
        masked = body.replace(env["client_secret"], mask(env["client_secret"]))
        masked = masked.replace(code, mask(code))
        print("curl command (masked):")
        print(f"curl -X POST '{url}' -H 'Content-Type: application/json' -d '{masked}'")
    if use_raw:
        body = json.dumps(payload, ensure_ascii=False)
        tokens = request_json("POST", url, raw_body=body, headers={"Content-Type": "application/json"})
    else:
        tokens = request_json("POST", url, json_body=payload, headers={"Content-Type": "application/json"})
    if not tokens.get("access_token"):
        raise RuntimeError("OAuth response missing access_token")
    tokens["updated_at"] = int(time.time())
    return tokens


def resolve_tokens_path(tokens_file: str) -> Path:
    path = Path(tokens_file)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return path


def save_tokens(tokens_file: str, tokens: Dict[str, Any]) -> None:
    tokens_path = resolve_tokens_path(tokens_file)
    with open(tokens_path, "w", encoding="utf-8") as handle:
        json.dump(tokens, handle, ensure_ascii=False, indent=2)
    print(f"Saved tokens -> {tokens_path}")


def load_tokens(tokens_file: str) -> Dict[str, Any]:
    tokens_path = resolve_tokens_path(tokens_file)
    with open(tokens_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def test_leads(env: Dict[str, str], tokens: Dict[str, Any]) -> None:
    access_token = tokens.get("access_token")
    if not access_token:
        raise RuntimeError("access_token is missing in tokens.json")
    url = f"{env['base_url']}/api/v4/leads"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/hal+json",
    }
    data = request_json("GET", url, params={"limit": 1}, headers=headers)
    leads = data.get("_embedded", {}).get("leads", [])
    print(f"Leads returned: {len(leads)}")
    if leads:
        lead_id = leads[0].get("id")
        print(f"Example lead id: {lead_id}")


def fetch_lead_by_id(env: Dict[str, str], tokens: Dict[str, Any], lead_id: str) -> None:
    access_token = tokens.get("access_token")
    if not access_token:
        raise RuntimeError("access_token is missing in tokens.json")
    url = f"{env['base_url']}/api/v4/leads/{lead_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/hal+json",
    }
    data = request_json("GET", url, headers=headers)
    print(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="amoCRM OAuth bootstrap + test")
    parser.add_argument("--code", help="Authorization code (20 min)")
    parser.add_argument("--force-code", action="store_true", help="Always use auth code even if tokens.json exists")
    parser.add_argument("--test", action="store_true", help="Test /api/v4/leads after tokens")
    parser.add_argument("--lead-id", help="Fetch lead by id and print full response")
    args = parser.parse_args()

    env = load_env()
    code = args.code or env.get("auth_code", "")
    force_code = args.force_code or is_truthy(env.get("force_code"))
    long_token = (env.get("long_token") or "").strip()
    tokens_path = resolve_tokens_path(env["tokens_file"])

    if args.code:
        print("Exchanging auth code for tokens...")
        tokens = exchange_code(env, code)
        save_tokens(env["tokens_file"], tokens)
    elif long_token and not force_code:
        print("Using AMO_LONG_TOKEN from env")
        tokens = {"access_token": long_token}
    elif tokens_path.exists() and not force_code:
        print(f"Using existing tokens.json -> {tokens_path}")
        tokens = load_tokens(env["tokens_file"])
    elif code:
        print("Exchanging auth code for tokens...")
        tokens = exchange_code(env, code)
        save_tokens(env["tokens_file"], tokens)
    else:
        raise RuntimeError(f"Auth code missing and tokens file not found: {tokens_path}")

    if args.test:
        print("Testing leads endpoint...")
        test_leads(env, tokens)
    if args.lead_id:
        print(f"Fetching lead {args.lead_id}...")
        fetch_lead_by_id(env, tokens, args.lead_id)


if __name__ == "__main__":
    main()
