#!/usr/bin/env sh
# Shim only — the implementation is docker_e2e_test.py (DESIGN_PHASE1.md §9, T0.7).
# Kept because this path is in README.md and in muscle memory.
exec python3 "$(dirname "$0")/docker_e2e_test.py" "$@"
