import os
from typing import Optional

# Try to load, but don't fail if variables are missing yet
try:
    from kaiber_utils.env_vars import get_env, get_env_bool
except ImportError:
    def get_env(key: str, default: str = "") -> str:
        return os.environ.get(key, default)
    def get_env_bool(key: str, default: bool = False) -> bool:
        return os.environ.get(key, "").lower() in ("true", "1")

# Use a Class or a function to fetch values so it's only called INSIDE the function
class Config:
    @property
    def R2_ACCESS_KEY_ID(self): return get_env('R2_ACCESS_KEY_ID')
    
    @property
    def R2_SECRET_ACCESS_KEY(self): return get_env('R2_SECRET_ACCESS_KEY')
    
    @property
    def R2_ENDPOINT_URL(self): return get_env('R2_ENDPOINT_URL')
    
    @property
    def R2_BUCKET_NAME(self): return get_env('R2_BUCKET_NAME')
    
    @property
    def GOOGLE_GEMINI_API_KEY(self): return get_env('GOOGLE_GEMINI_API_KEY')

config = Config()