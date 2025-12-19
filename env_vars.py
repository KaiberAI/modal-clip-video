"""
Environment Variables Module for Video Scenes

Simple environment variable loading for R2 credentials.
"""

import os
from typing import Optional


def get_env(key: str, default_value: Optional[str] = None) -> str:
    """Get environment variable with optional default."""
    value = os.environ.get(key)
    if value is not None:
        return value
    return default_value if default_value is not None else ''


def get_env_bool(key: str, default_value: bool = False) -> bool:
    """Get boolean environment variable."""
    value = os.environ.get(key)
    if value is not None:
        return value.lower() in ('true', '1', 'yes', 'on')
    return default_value


# R2/S3 Credentials
R2_ACCESS_KEY_ID = get_env('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = get_env('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT_URL = get_env('R2_ENDPOINT_URL', 'https://3a0e8493c00fa2487b71580016cad807.r2.cloudflarestorage.com')
R2_BUCKET_NAME = get_env('R2_BUCKET_NAME', 'secret-memories')

# Public CDN URL for serving files
R2_PUBLIC_CDN_URL = get_env('R2_PUBLIC_CDN_URL', 'https://media.kybercorp.org')
