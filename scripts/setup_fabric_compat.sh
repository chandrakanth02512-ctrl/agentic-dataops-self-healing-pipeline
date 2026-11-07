#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Local compatibility environment only. Never execute this inside Fabric.
if [[ ! -x build/fabric-venv/bin/python ]]; then
    uv --cache-dir .cache/uv venv --seed --python python3.13 --no-python-downloads build/fabric-venv
fi
build/fabric-venv/bin/python -c 'import sys; assert sys.version_info[:2] == (3, 13), "Compatibility environment requires Python 3.13"'
build/fabric-venv/bin/python -m pip install -r requirements-fabric.lock
build/fabric-venv/bin/python -m pip install --no-deps --no-build-isolation -e .
build/fabric-venv/bin/python scripts/setup_local_delta.py
build/fabric-venv/bin/python -m pip check
