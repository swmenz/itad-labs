"""Кроссплатформенная точка входа для проверки чтения raw-объекта из SeaweedFS через DuckDB."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    """Подключить каталог src и вызвать CLI-проверку чтения raw."""
    src_dir = Path(__file__).resolve().parents[1] / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    from analysis.read_raw import main as run_read_raw

    run_read_raw()


if __name__ == "__main__":
    main()
