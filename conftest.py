"""Repository-level pytest conftest.

This file ensures workspace packages' `src/` directories are added to
`sys.path` before test collection when the workspace packages are not
installed. If `orthant` is already importable (e.g., editable installs),
we do nothing to avoid module name collisions.
"""
from pathlib import Path
import sys

try:
    import orthant  # type: ignore
except Exception:
    repo_root = Path(__file__).resolve().parent
    packages_dir = repo_root / "packages"
    if packages_dir.exists():
        for child in packages_dir.iterdir():
            src = child / "src"
            if src.exists():
                # insert at front so workspace packages override installed packages
                sys.path.insert(0, str(src))
