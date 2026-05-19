# Image Slot Leak Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the per-account image inflight slot from leaking when an image generation hits the poll-timeout path, by decoupling slot lifetime from per-account statistics and centralizing release into a single `try/finally`.

**Architecture:** Split the bundled `mark_image_result(token, success)` into two single-responsibility methods (`mark_image_success`, `mark_image_failure`) that **never** touch `_image_inflight`. Move `release_image_slot` into one `finally` block inside `stream_image_outputs_with_pool` so it runs on every exit path (success / poll-timeout / business-error / unexpected-exception).

**Tech Stack:** Python 3.13, `unittest.TestCase`, `unittest.mock.patch`, FastAPI/Starlette (not exercised by these tests).

**Spec:** `docs/superpowers/specs/2026-05-19-image-slot-leak-fix-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `services/account_service.py` | Modify (lines 343-376) | Replace `mark_image_result` with `mark_image_success` + `mark_image_failure`; keep `release_image_slot` unchanged |
| `services/protocol/conversation.py` | Modify (lines 642-686, inside `stream_image_outputs_with_pool`) | Wrap per-token work in `try/finally`; replace `mark_image_result(True/False)` call sites with `mark_image_success` / `mark_image_failure` |
| `test/test_account_image_capabilities.py` | Modify (line 29-47) | Rename + retarget the existing `mark_image_result` quota test to `mark_image_success` |
| `test/test_image_slot_lifecycle.py` | Create | 7 new tests covering slot release on all paths and the production-leak regression scenario |

---

## Task 1 — Introduce `mark_image_success` / `mark_image_failure` (additive, keep `mark_image_result` for now)

**Files:**
- Modify: `services/account_service.py:343-376`
- Modify: `test/test_account_image_capabilities.py:29-47` (add new tests; do NOT yet retarget the existing one — that happens in Task 3)

- [ ] **Step 1: Write the failing unit tests for the new methods**

Append to `test/test_account_image_capabilities.py` inside the existing `AccountCapabilityTests` class:

```python
    def test_mark_image_success_does_not_change_inflight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {"status": "正常", "quota": 5, "image_quota_unknown": False},
            )
            service._image_inflight["token-1"] = 3

            service.mark_image_success("token-1")

            self.assertEqual(service._image_inflight["token-1"], 3)
            service.release_image_slot("token-1")
            self.assertEqual(service._image_inflight["token-1"], 2)

    def test_mark_image_failure_does_not_change_inflight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {"status": "正常", "quota": 5, "image_quota_unknown": False},
            )
            service._image_inflight["token-1"] = 3

            service.mark_image_failure("token-1")

            self.assertEqual(service._image_inflight["token-1"], 3)
            service.release_image_slot("token-1")
            self.assertEqual(service._image_inflight["token-1"], 2)

    def test_mark_image_success_updates_quota_like_old_mark_image_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {"status": "正常", "quota": 3, "image_quota_unknown": False},
            )

            updated = service.mark_image_success("token-1")

            self.assertIsNotNone(updated)
            self.assertEqual(updated["quota"], 2)
            self.assertEqual(updated["success"], 1)
            self.assertEqual(updated["status"], "正常")

    def test_mark_image_failure_increments_fail_counter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {"status": "正常", "quota": 3, "image_quota_unknown": False},
            )

            updated = service.mark_image_failure("token-1")

            self.assertIsNotNone(updated)
            self.assertEqual(updated["fail"], 1)
            self.assertEqual(updated["quota"], 3)   # failure does not consume quota
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `python -m unittest test.test_account_image_capabilities -v`

Expected: 4 new tests FAIL with `AttributeError: 'AccountService' object has no attribute 'mark_image_success'` (and `mark_image_failure`).

- [ ] **Step 3: Implement `mark_image_success` and `mark_image_failure`**

In `services/account_service.py`, after the existing `mark_image_result` method (currently lines 343-376), add:

```python
    def mark_image_success(self, access_token: str) -> dict | None:
        """Update success counter, decrement quota, refresh status. Does NOT release slot."""
        if not access_token:
            return None
        with self._lock:
            current = self._accounts.get(access_token)
            if current is None:
                return None
            next_item = dict(current)
            next_item["last_used_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            image_quota_unknown = bool(next_item.get("image_quota_unknown"))
            next_item["success"] = int(next_item.get("success") or 0) + 1
            if not image_quota_unknown:
                next_item["quota"] = max(0, int(next_item.get("quota") or 0) - 1)
            if not image_quota_unknown and next_item["quota"] == 0:
                next_item["status"] = "限流"
                next_item["restore_at"] = next_item.get("restore_at") or None
            elif next_item.get("status") == "限流":
                next_item["status"] = "正常"
            account = self._normalize_account(next_item)
            if account is None:
                return None
            if account.get("status") == "限流" and config.auto_remove_rate_limited_accounts:
                self._accounts.pop(access_token, None)
                self._save_accounts()
                log_service.add(LOG_TYPE_ACCOUNT, "自动移除限流账号", {"token": anonymize_token(access_token)})
                return None
            self._accounts[access_token] = account
            self._save_accounts()
            return dict(account)

    def mark_image_failure(self, access_token: str) -> dict | None:
        """Update failure counter. Does NOT release slot, does NOT touch quota."""
        if not access_token:
            return None
        with self._lock:
            current = self._accounts.get(access_token)
            if current is None:
                return None
            next_item = dict(current)
            next_item["last_used_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            next_item["fail"] = int(next_item.get("fail") or 0) + 1
            account = self._normalize_account(next_item)
            if account is None:
                return None
            self._accounts[access_token] = account
            self._save_accounts()
            return dict(account)
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `python -m unittest test.test_account_image_capabilities -v`

Expected: All 4 new tests PASS. The pre-existing `test_mark_image_result_does_not_consume_unknown_quota` continues to PASS (we have not yet removed `mark_image_result`).

- [ ] **Step 5: Commit**

```bash
git add services/account_service.py test/test_account_image_capabilities.py
git commit -m "$(cat <<'EOF'
feat(account): add mark_image_success / mark_image_failure

Introduce two single-responsibility methods that update per-account
statistics without touching the image inflight slot counter. They will
replace mark_image_result in the next commit; for now mark_image_result
stays in place so conversation.py callers keep working.

Why split: mark_image_result bundles slot release with statistics
updates, which silently broke when commit 32d1ec8 skipped the call on
poll-timeout to avoid inflating failure counters.
EOF
)"
```

---

## Task 2 — Add the failing regression tests for `stream_image_outputs_with_pool`

These tests target the production bug. Before Task 3 lands, the timeout-related tests will FAIL on current code (proving the bug); the success / business-error / unexpected-exception tests will PASS (proving the other paths already release correctly).

**Files:**
- Create: `test/test_image_slot_lifecycle.py`

- [ ] **Step 1: Write the new test file**

Create `test/test_image_slot_lifecycle.py` with the following content:

```python
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("CHATGPT2API_AUTH_KEY", "test-auth")

import services.account_service as account_service_module
import services.protocol.conversation as conv_mod
from services.account_service import AccountService
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol.conversation import (
    ConversationRequest,
    ImageGenerationError,
    ImageOutput,
    stream_image_outputs_with_pool,
)
from services.storage.json_storage import JSONStorageBackend


def _make_service(tmp_dir: str, *, token: str = "token-1") -> AccountService:
    service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
    service.add_accounts([token])
    service.update_account(
        token,
        {
            "status": "正常",
            "quota": 0,
            "image_quota_unknown": True,
        },
    )
    return service


def _fake_remote_info(self, access_token, event="fetch_remote_info"):
    # Simulate a successful refresh: returns a healthy account so
    # get_available_access_token treats the token as usable.
    return self._accounts.get(access_token)


def _request() -> ConversationRequest:
    return ConversationRequest(
        prompt="hello",
        model="gpt-image-2",
        n=1,
        size=None,
        response_format="b64_json",
        base_url=None,
        message_as_error=False,
    )


def _stub_stream_timeout(backend, request, index, total):
    raise ImagePollTimeoutError("simulated poll timeout")
    yield  # noqa: makes this a generator function


def _stub_stream_success(backend, request, index, total):
    yield ImageOutput(
        kind="result",
        model=request.model,
        index=index,
        total=total,
        data=[{"b64_json": "ZmFrZQ=="}],
        created=1234567890,
    )


def _stub_stream_generation_error(backend, request, index, total):
    raise ImageGenerationError("simulated policy rejection", status_code=400)
    yield  # noqa


def _stub_stream_runtime_error(backend, request, index, total):
    raise RuntimeError("simulated upstream boom")
    yield  # noqa


class ImageSlotLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.service = _make_service(self._tmp.name)
        # Replace the module-level singleton used by stream_image_outputs_with_pool.
        self._patch_service = patch.object(conv_mod, "account_service", self.service)
        self._patch_service.start()
        self.addCleanup(self._patch_service.stop)
        # Replace the AccountService.fetch_remote_info so get_available_access_token
        # does not perform a real HTTP call.
        self._patch_remote = patch.object(
            account_service_module.AccountService, "fetch_remote_info", _fake_remote_info
        )
        self._patch_remote.start()
        self.addCleanup(self._patch_remote.stop)

    def _patch_stream(self, fake):
        p = patch.object(conv_mod, "stream_image_outputs", fake)
        p.start()
        self.addCleanup(p.stop)

    def test_slot_released_on_poll_timeout(self) -> None:
        self._patch_stream(_stub_stream_timeout)
        with self.assertRaises(ImagePollTimeoutError):
            list(stream_image_outputs_with_pool(_request()))
        self.assertEqual(self.service._image_inflight, {})

    def test_slot_released_on_success(self) -> None:
        self._patch_stream(_stub_stream_success)
        outputs = list(stream_image_outputs_with_pool(_request()))
        self.assertTrue(any(o.kind == "result" for o in outputs))
        self.assertEqual(self.service._image_inflight, {})

    def test_slot_released_on_generation_error(self) -> None:
        self._patch_stream(_stub_stream_generation_error)
        with self.assertRaises(ImageGenerationError):
            list(stream_image_outputs_with_pool(_request()))
        self.assertEqual(self.service._image_inflight, {})

    def test_slot_released_on_unexpected_exception(self) -> None:
        self._patch_stream(_stub_stream_runtime_error)
        with self.assertRaises(ImageGenerationError):
            # The pool wraps unexpected exceptions via _image_error_from_upstream.
            list(stream_image_outputs_with_pool(_request()))
        self.assertEqual(self.service._image_inflight, {})

    def test_no_leak_under_repeated_timeouts(self) -> None:
        self._patch_stream(_stub_stream_timeout)
        for _ in range(50):
            with self.assertRaises(ImagePollTimeoutError):
                list(stream_image_outputs_with_pool(_request()))
        self.assertEqual(self.service._image_inflight, {})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the file and observe expected partial failure**

Run: `python -m unittest test.test_image_slot_lifecycle -v`

Expected on current `main`:
- `test_slot_released_on_success` → **PASS** (success path already releases)
- `test_slot_released_on_generation_error` → **PASS** (already releases)
- `test_slot_released_on_unexpected_exception` → **PASS** (already releases)
- `test_slot_released_on_poll_timeout` → **FAIL** (bug: leaks)
- `test_no_leak_under_repeated_timeouts` → **FAIL** (bug accumulates)

This is the proof that the regression tests catch the production bug.

- [ ] **Step 3: Commit the failing tests**

```bash
git add test/test_image_slot_lifecycle.py
git commit -m "$(cat <<'EOF'
test(image): add regression tests for slot release on every exit path

Adds 5 tests covering stream_image_outputs_with_pool: slot must be
released on success / poll-timeout / generation-error / runtime-error,
and 50 consecutive poll-timeouts must not accumulate leaked inflight
slots.

On current main two tests intentionally FAIL — they reproduce the
production bug introduced by commit 32d1ec8. The next commit fixes
the code so all 5 pass.
EOF
)"
```

---

## Task 3 — Refactor `stream_image_outputs_with_pool`, retarget existing test, remove `mark_image_result`

**Files:**
- Modify: `services/protocol/conversation.py:642-686`
- Modify: `services/account_service.py:343-376` (remove `mark_image_result`)
- Modify: `test/test_account_image_capabilities.py:29-47` (retarget existing test)

- [ ] **Step 1: Refactor `stream_image_outputs_with_pool` to use try/finally + new methods**

In `services/protocol/conversation.py`, replace the body of `stream_image_outputs_with_pool` (currently lines 636-691) with:

```python
def stream_image_outputs_with_pool(request: ConversationRequest) -> Iterator[ImageOutput]:
    if str(request.model or "").strip() not in IMAGE_MODELS:
        raise ImageGenerationError("unsupported image model,supported models: " + ", ".join(IMAGE_MODELS))

    emitted = False
    last_error = ""
    for index in range(1, request.n + 1):
        while True:
            try:
                token = account_service.get_available_access_token()
            except RuntimeError as exc:
                if emitted:
                    return
                raise ImageGenerationError(str(exc) or "image generation failed") from exc

            emitted_for_token = False
            returned_message = False
            returned_result = False
            try:
                backend = OpenAIBackendAPI(access_token=token)
                for output in stream_image_outputs(backend, request, index, request.n):
                    if output.kind == "message" and request.message_as_error:
                        raise ImageGenerationError(
                            output.text or "Image generation was rejected by upstream policy.",
                            status_code=400,
                            error_type="invalid_request_error",
                            code="content_policy_violation",
                        )
                    emitted = True
                    emitted_for_token = True
                    returned_message = output.kind == "message"
                    returned_result = returned_result or output.kind == "result"
                    yield output
                if returned_message or not returned_result:
                    account_service.mark_image_failure(token)
                    return
                account_service.mark_image_success(token)
                break
            except ImagePollTimeoutError:
                # Timeout is not an account-level failure; do not update stats.
                # The slot is still released by the finally clause below.
                raise
            except ImageGenerationError:
                account_service.mark_image_failure(token)
                raise
            except Exception as exc:
                account_service.mark_image_failure(token)
                last_error = str(exc)
                logger.warning({"event": "image_stream_fail", "request_token": token, "error": last_error})
                if not emitted_for_token and is_token_invalid_error(last_error):
                    account_service.remove_invalid_token(token, "image_stream")
                    continue
                raise _image_error_from_upstream(exc, last_error) from exc
            finally:
                account_service.release_image_slot(token)

    if not emitted:
        if not last_error:
            last_error = "no account in the pool could generate images — check account quota and rate-limit status"
        raise ImageGenerationError(image_stream_error_message(last_error))
```

- [ ] **Step 2: Run the lifecycle tests to confirm they ALL pass now**

Run: `python -m unittest test.test_image_slot_lifecycle -v`

Expected: All 5 tests PASS.

- [ ] **Step 3: Retarget the existing `test_mark_image_result_does_not_consume_unknown_quota`**

In `test/test_account_image_capabilities.py:29-47`, replace the method with:

```python
    def test_mark_image_success_does_not_consume_unknown_quota(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = AccountService(JSONStorageBackend(Path(tmp_dir) / "accounts.json"))
            service.add_accounts(["token-1"])
            service.update_account(
                "token-1",
                {
                    "status": "正常",
                    "quota": 0,
                    "image_quota_unknown": True,
                },
            )

            updated = service.mark_image_success("token-1")

            self.assertIsNotNone(updated)
            self.assertEqual(updated["quota"], 0)
            self.assertEqual(updated["status"], "正常")
            self.assertTrue(updated["image_quota_unknown"])
```

- [ ] **Step 4: Remove `mark_image_result` from `services/account_service.py`**

Delete the entire `def mark_image_result(self, access_token: str, success: bool) -> dict | None:` method (lines 343-376 in the pre-Task-1 numbering; locate by `def mark_image_result`). Do not leave a stub or deprecated shim — repo-wide grep before this plan was written confirmed only `services/protocol/conversation.py` (now rewritten in Step 1) and `test/test_account_image_capabilities.py` (now rewritten in Step 3) called it.

- [ ] **Step 5: Run the full test suite**

Run: `python -m unittest discover -s test -v`

Expected: All tests PASS. Pay attention to: `test_account_image_capabilities`, `test_image_slot_lifecycle`, plus anything else under `test/`.

- [ ] **Step 6: Search the repo for any remaining `mark_image_result` reference**

Run: `grep -rn "mark_image_result" --include="*.py" . | grep -v ".venv"`

Expected: zero matches.

- [ ] **Step 7: Commit**

```bash
git add services/protocol/conversation.py services/account_service.py test/test_account_image_capabilities.py
git commit -m "$(cat <<'EOF'
fix(image): release inflight slot in finally to stop production leak

Wrap the per-token work in stream_image_outputs_with_pool in
try/finally so release_image_slot runs on every exit path
(success, poll-timeout, generation-error, runtime-error).

Replace call sites of mark_image_result with the new
single-responsibility mark_image_success / mark_image_failure
methods, then delete mark_image_result entirely so the bundled
side effect cannot reappear.

Fixes the production symptom where image requests started timing
out after the server had been running for a while: every
ImagePollTimeoutError silently leaked one slot per account, and once
the per-account counter reached image_account_concurrency (default
10) the account dropped out of the candidate pool. Restarting the
container cleared the in-memory dict, hence the "restart fixes it"
pattern.

Regression introduced by 32d1ec8 (2026-05-16).
EOF
)"
```

---

## Task 4 — Final verification + push

- [ ] **Step 1: Re-run the full test suite from a clean state**

Run: `python -m unittest discover -s test -v 2>&1 | tail -20`

Expected: `OK` with the new tests listed.

- [ ] **Step 2: Diff review against `main`**

Run: `git log --oneline main..HEAD` and `git diff main..HEAD -- services/ test/`

Expected: 3 commits (Task 1, 2, 3); diff localized to the four files in the File Structure table.

- [ ] **Step 3: Push the branch**

Run: `git push -u origin fix/image-slot-leak-on-timeout`

(Confirm with the user before this step — pushing publishes the branch.)

---

## Self-Review Checklist (executed by plan author before handing off)

- [x] Spec section "Problem" → covered by Task 2 regression tests + Task 3 fix.
- [x] Spec section "Goals" #1 (stop leak) → Task 3 finally block.
- [x] Spec section "Goals" #2 (prevent future regressions) → Task 1 split + Task 3 finally.
- [x] Spec section "Goals" #3 (timeouts do not inflate failure counters) → Task 3 `except ImagePollTimeoutError: raise` skips `mark_image_failure`.
- [x] Spec testing #1-7 → Task 1 tests (5-6), Task 2 tests (1-4 + 7).
- [x] Spec "Existing test updates" → Task 3 Step 3.
- [x] No placeholders, no "TBD", no "similar to Task N" — every step has full code.
- [x] Method names consistent across tasks (`mark_image_success`, `mark_image_failure`, `release_image_slot`).
- [x] File paths exact.
- [x] Each step is 2-5 minutes of work.

---

## Execution Notes

- **Branch:** `fix/image-slot-leak-on-timeout` (already checked out, based on `origin/main`)
- **No worktree:** user opted to work directly in the current directory.
- **After this plan:** code review (×2), then version-tag, then `docker buildx` multi-arch build + push to DockerHub. Those steps are tracked as separate tasks outside this plan document.
