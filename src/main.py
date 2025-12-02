from __future__ import annotations
import logging
import subprocess
import sys
from pathlib import Path


logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def _run_script(script_name: str, args: list[str]) -> int:
    here = Path(__file__).parent
    script = here / script_name
    if not script.exists():
        logging.error('Script not found: %s', script)
        return 2
    cmd = [sys.executable, str(script)] + args
    proc = subprocess.run(cmd)
    return proc.returncode


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    if not argv:
        print('Usage: main.py <find-new|fetch-items> [args...]')
        return 1

    cmd = argv[0]
    rest = argv[1:]
    if cmd in ('find-new', 'find'):
        return _run_script('find_new_smart_processes.py', rest)
    if cmd in ('fetch-items', 'fetch'):
        return _run_script('fetch_smart_process_items.py', rest)

    print('Unknown command:', cmd)
    print('Available commands: find-new, fetch-items')
    return 1


if __name__ == '__main__':
    raise SystemExit(main())