import os
from pathlib import Path
from typing import Optional

from .constants import ENV_KEY_NAMES, PROXY_KEY_NAMES
from .utils import load_env_file


def resolve_api_key(cli_api_key: Optional[str]) -> Optional[str]:
    if cli_api_key:
        return cli_api_key

    for key_name in ENV_KEY_NAMES:
        value = os.environ.get(key_name)
        if value:
            return value

    cwd_env = Path.cwd() / ".env"
    project_root_env = Path(__file__).resolve().parents[2] / ".env"

    for env_path in (cwd_env, project_root_env):
        env_values = load_env_file(env_path)
        for key_name in ENV_KEY_NAMES:
            value = env_values.get(key_name)
            if value:
                return value

    return None


def apply_proxy_environment() -> None:
    cwd_env = Path.cwd() / ".env"
    project_root_env = Path(__file__).resolve().parents[2] / ".env"

    for env_path in (cwd_env, project_root_env):
        env_values = load_env_file(env_path)
        for key_name in PROXY_KEY_NAMES:
            value = env_values.get(key_name)
            if value:
                os.environ[key_name] = value
