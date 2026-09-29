#!/usr/bin/env sh
# Shim only — the implementation is k8s_e2e_test.py (DESIGN_PHASE1.md §9, §14).
exec python3 "$(dirname "$0")/k8s_e2e_test.py" "$@"
