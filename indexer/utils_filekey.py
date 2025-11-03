"""
Utilities for generating stable file keys and normalizing paths.
"""

import hashlib
import os
from pathlib import Path

def compute_file_key(path: str, chunk_size: int = 1024 * 1024) -> str:
    """Deterministic key from file content (preferred over path).
    
    Args:
        path: Path to the file to hash
        chunk_size: Size of chunks to read (default 1MB)
        
    Returns:
        24-character prefix of SHA-256 hash of file contents
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b: break
            h.update(b)
    return h.hexdigest()[:24]  # short, stable

def normalize_path(p: str) -> str:
    """Convert path to normalized absolute form with forward slashes."""
    try:
        return Path(p).resolve().as_posix().lower()
    except Exception:
        return str(p).replace("\\", "/").lower()