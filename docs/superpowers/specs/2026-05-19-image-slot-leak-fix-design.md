# Image Slot Leak on Poll Timeout — Fix Design

**Date:** 2026-05-19
**Author:** xbang (with Claude assistance)
**Branch:** `fix/image-slot-leak-on-timeout`
**Base:** `main`

## Problem

`services/protocol/conversation.py:674-675` catches `ImagePollTimeoutError` and re-raises without releasing the per-account image inflight slot. Every image generation that hits the 180s timeout permanently bumps `account_service._image_inflight[token]` by one. Once an account's counter reaches `image_account_concurrency` (default 10), it is removed from the candidate pool. Repeated timeouts drain the pool; the server appears to hang on every image request until restart, at which point the in-memory dict resets.

### Evidence

Production log analysis on `38.255.16.127`:

| Date | Total calls | Timeouts | Timeout rate |
|------|-------------|----------|--------------|
| 04-30 ~ 05-15 | ~2,400 | 0 | 0% |
| 05-16 | 795 | 1 | 0.1% (commit `32d1ec8` landed) |
| 05-17 | 539 | 0 | 0% |
| 05-18 | 610 | 23 | 3.8% |
| 05-19 | 447 | 60 | 13.4% |

All timeouts hit `model=gpt-image-2` with error `等待图片生成超时（180s），后台任务仍在执行`, then required a container restart to recover.

### Root cause

`account_service.mark_image_result(token, success)` was the only call site that released the slot — it bundled two unrelated effects:

1. Decrement `_image_inflight[token]` (the slot release).
2. Update success/failure counters, quota, status.

Commit `32d1ec8` (2026-05-16, "fix: raise clear error message on image poll timeout") added an `except ImagePollTimeoutError: raise` branch that deliberately skipped `mark_image_result` because **the author wanted to avoid counting timeouts as account failures**. The author did not notice the bundled slot-release side effect, so the change silently broke the release path for one exception class.

The bug is a **misnamed function with hidden side effects** — `mark_image_result` reads as "update statistics" but also owns concurrency-control state.

## Goals

1. Stop leaking image slots on `ImagePollTimeoutError`.
2. Restructure so future exception branches cannot reintroduce this class of bug.
3. Preserve the original intent of commit `32d1ec8`: timeouts must not inflate per-account failure counters.

## Non-Goals

- Persisting `_image_inflight` across restarts (out of scope; restart is acceptable as a last-resort recovery).
- Reworking the worker-queue isolation in `pr/image-worker-queue` (separate PR, separate concern).
- Changing the 180s timeout value or the polling cadence.
- Adding a config hot-reload mechanism (separate discussion).

## Design

### Architectural change

Decouple "slot lifetime" from "statistics" by:

1. Splitting `mark_image_result(token, success)` into two single-responsibility methods that **do not** touch `_image_inflight`.
2. Moving slot release into a single `finally` block on the caller side. The `release_image_slot` call site becomes textually obvious and structurally unmissable.

### Changes to `services/account_service.py`

**Remove:**

```python
def mark_image_result(self, access_token: str, success: bool) -> dict | None:
    ...
    self.release_image_slot(access_token)   # ← bundled side effect
    ...
```

**Add (replacements):**

```python
def mark_image_success(self, access_token: str) -> dict | None:
    """Update success counter, decrement quota, refresh status. Does NOT release slot."""

def mark_image_failure(self, access_token: str) -> dict | None:
    """Update failure counter. Does NOT release slot."""
```

Both new methods contain only the `with self._lock:` body of the current `mark_image_result`, split by the `if success:` branch. The leading `self.release_image_slot(access_token)` line is dropped from both.

`release_image_slot` itself is unchanged. It remains the sole public API for releasing slots.

### Changes to `services/protocol/conversation.py`

Replace the four call sites inside `stream_image_outputs_with_pool` and wrap them in `try/finally`:

```python
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
```

### Invariants

After this change:

- `_image_inflight[token]` is incremented exactly once per successful `get_available_access_token()` call.
- It is decremented exactly once per matching `release_image_slot()` call.
- For every successful `get_available_access_token()`, the `finally` clause runs exactly once (Python guarantees this for try/finally around any normal/break/continue/return/raise exit).
- `mark_image_success` and `mark_image_failure` only mutate per-account counters and status fields. They never touch `_image_inflight`.

### Note on `remove_invalid_token`

The `except Exception` branch may call `account_service.remove_invalid_token(token, "image_stream")` followed by `continue`. The `continue` re-enters the `while True` loop and acquires a new token. `remove_invalid_token` internally calls `self._image_inflight.pop(token, None)` (`account_service.py:310`), which is correct for the case where the account is being deleted entirely. The `finally` clause still runs and calls `release_image_slot(token)`, which is a no-op for the already-popped token (the function early-returns on missing keys). No double-release possible.

## Testing

New test file: `test/test_image_slot_lifecycle.py` (using `unittest.TestCase` for consistency with the rest of `test/`)

Tests (using `AccountService` with `JSONStorageBackend` in a `tempfile.TemporaryDirectory`, plus monkeypatching `services.protocol.conversation.OpenAIBackendAPI` and `services.account_service.AccountService.fetch_remote_info` to inject controlled behavior):

1. **`test_slot_released_on_poll_timeout`** — Stub backend so `stream_image_outputs` raises `ImagePollTimeoutError`. Run `list(stream_image_outputs_with_pool(request))` inside `self.assertRaises(ImagePollTimeoutError)`. Assert `service._image_inflight == {}` afterwards. **This is the core regression test.**
2. **`test_slot_released_on_success`** — Stub backend to yield a valid `ImageOutput(kind="result", ...)`. Exhaust the generator via `list(...)`, then assert `_image_inflight == {}`.
3. **`test_slot_released_on_generation_error`** — Stub backend to raise `ImageGenerationError`. Wrap in `assertRaises`. Assert `_image_inflight == {}`.
4. **`test_slot_released_on_unexpected_exception`** — Stub backend to raise `RuntimeError("boom")` (which the pool converts via `_image_error_from_upstream`). Wrap in `assertRaises(ImageGenerationError)`. Assert `_image_inflight == {}`.
5. **`test_mark_image_success_does_not_change_inflight`** — Manually set `service._image_inflight["token-1"] = 3`, call `mark_image_success("token-1")`, assert `_image_inflight["token-1"] == 3`. Then call `release_image_slot("token-1")`, assert it drops to 2. This locks in the responsibility split.
6. **`test_mark_image_failure_does_not_change_inflight`** — Symmetric to #5 with `mark_image_failure`.
7. **`test_no_leak_under_repeated_timeouts`** — Fire 50 consecutive `list(stream_image_outputs_with_pool(request))` calls with the timeout stub (each wrapped in `assertRaises(ImagePollTimeoutError)`). Assert `_image_inflight == {}` at the end. This directly reproduces the user-visible production bug.

Existing test updates:

- `test/test_account_image_capabilities.py:42` calls `service.mark_image_result("token-1", success=True)`. Update to `service.mark_image_success("token-1")`. The test name (`test_mark_image_result_does_not_consume_unknown_quota`) should be renamed to `test_mark_image_success_does_not_consume_unknown_quota`.

## Migration / Rollout

- No data-format changes. No config changes. No mounted-volume changes.
- Single rebuild + container restart.
- Worker queue PR (`pr/image-worker-queue`) is unrelated and can land independently in either order.

## Risks

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| External callers of `mark_image_result` outside the repo (forks/plugins) | Low — repo-wide grep shows only 4 call sites, all in `conversation.py` | Document the rename in commit message; affected forks see a clear `AttributeError` rather than silent leaks |
| `try/finally` placement misses a code path | Low — only one acquire site (`get_available_access_token`); one matching finally | Regression test #7 directly exercises the original failure mode |
| `mark_image_failure` called twice on the same task (e.g., from `_image_error_from_upstream` path) | Already possible in current code; not regressing | Out of scope |

## Out of Scope (Follow-up Candidates)

- Persisting `_image_inflight` so restarts do not silently dump in-flight bookkeeping.
- Refactoring `_image_inflight` into a proper context manager (`with account_service.image_slot() as token:`) for stronger compile-time guarantees. The try/finally approach in this fix is the minimal step toward that future refactor.
- Adding a Prometheus / structured-log gauge for current inflight counts so a runaway pool drain becomes observable before user impact.
