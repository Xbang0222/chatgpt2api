"""Pin the auth key before any test module imports services/config.

Without this, the first test module to set CHATGPT2API_AUTH_KEY via setdefault
wins, and HTTP tests that hard-code Bearer "chatgpt2api" fail when sibling
tests pick a different value (e.g. test_account_image_capabilities sets
"test-auth"). conftest.py loads before every test module, so seeding here
keeps the auth key consistent across the suite.
"""
from __future__ import annotations

import os

os.environ.setdefault("CHATGPT2API_AUTH_KEY", "chatgpt2api")
