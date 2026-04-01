from __future__ import annotations

import getpass

import keyring

_SERVICE = "taskpull"
_CLAUDE_TOKEN_KEY = "claude-oauth-token"


def get_claude_token() -> str:
    """Return the stored Claude OAuth token, prompting if missing."""
    token = keyring.get_password(_SERVICE, _CLAUDE_TOKEN_KEY)
    if token:
        return token
    token = getpass.getpass(
        "Claude OAuth token not found in keychain.\n"
        "Run `claude setup-token` to generate one, then paste it here.\n"
        "Token: "
    )
    if not token:
        raise SystemExit("No token provided.")
    keyring.set_password(_SERVICE, _CLAUDE_TOKEN_KEY, token)
    return token
