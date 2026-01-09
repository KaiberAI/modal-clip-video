"""
Environment Variables Module for Video Scenes

This module re-exports environment variables from kaiber_utils.env_vars
and adds any project-specific environment variables.

Usage:
    from env_vars import R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL

Environment Setup:
    - Local Development: Run `npx dotenv-vault@latest pull` to download
      development variables to local .env file
    - Production (Modal): Set DOTENV_KEY in Modal secrets to load production
      variables from encrypted vault

Note: Environment loading happens in kaiber_utils.env_vars which handles
      dotenv-vault decryption based on DOTENV_KEY presence.
"""

import os
from typing import Optional

# Load environment variables - kaiber_utils.env_vars handles the vault loading
try:
    import kaiber_utils.env_vars  # type: ignore  # noqa: F401
except ImportError:
    # kaiber_utils not available yet (e.g., during Modal build)
    from dotenv import load_dotenv
    load_dotenv('.env', override=False)


# Import functions from kaiber_utils.env_vars
try:
    from kaiber_utils.env_vars import get_env, get_env_bool  # type: ignore
except ImportError:
    # Fallback implementations if kaiber_utils not available
    def get_env(key: str, default_value: Optional[str] = None) -> str:
        """Fallback get_env implementation"""
        value = os.environ.get(key)
        if value is not None:
            return value
        return default_value if default_value is not None else ''

    def get_env_bool(key: str, default_value: bool = False) -> bool:
        """Fallback get_env_bool implementation"""
        value = os.environ.get(key)
        if value is not None:
            return value.lower() in ('true', '1', 'yes', 'on')
        return default_value


# Project-specific environment variables

# R2/S3 Credentials
R2_ACCESS_KEY_ID = get_env('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = get_env('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT_URL = get_env('R2_ENDPOINT_URL', 'https://3a0e8493c00fa2487b71580016cad807.r2.cloudflarestorage.com')
R2_BUCKET_NAME = get_env('R2_BUCKET_NAME', 'secret-memories')

# GitHub Token (if needed for future features)
GITHUB_TOKEN = get_env('GITHUB_TOKEN', '')

# Webhook Secrets
SIEVE_WEBHOOK_SECRET = get_env('SIEVE_WEBHOOK_SECRET', '')

# Gemini API
GEMINI_API_KEY = get_env('GOOGLE_GEMINI_API_KEY', '')

# Platform Detection
SIEVE_ENABLED = get_env_bool('SIEVE_ENABLED', False)
MODAL_ENABLED = get_env_bool('MODAL_ENABLED', False)