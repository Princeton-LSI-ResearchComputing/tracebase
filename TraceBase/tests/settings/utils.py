import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

REPO_ROOT = Path(__file__).resolve().parents[3]


def get_setting_values(
    settings_module, *setting_names, env_overrides: Optional[Dict[str, str]] = None
):
    """Start a fresh Django process using the requested settings module and return the requested settings as a dict.

    Args:
        settings_module (str): E.g. 'TraceBase.settings.prod'
        setting_names (List[str]): The environment variables to return in the dict.
        env_overrides (Optional[Dict[str, str]]): E.g. {'DEBUG': True}
    Exceptions:
        RuntimeError - If the settings module fails to load.
    Returns:
        (dict): The environment variable names and their values.
    """
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = settings_module

    if env_overrides:
        env.update(env_overrides)

    # The '!r' in '{setting_names!r}' puts quotes around the values
    python_code = f"""
import json
from django.conf import settings

print(json.dumps({{
    name: getattr(settings, name)
    for name in {setting_names!r}
}}, default=str))
"""

    result = subprocess.run(
        [sys.executable, "-c", python_code],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    return json.loads(result.stdout)
