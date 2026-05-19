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
from services.config import config
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
        self._patch_service = patch.object(conv_mod, "account_service", self.service)
        self._patch_service.start()
        self.addCleanup(self._patch_service.stop)
        self._patch_remote = patch.object(
            account_service_module.AccountService, "fetch_remote_info", _fake_remote_info
        )
        self._patch_remote.start()
        self.addCleanup(self._patch_remote.stop)
        # Default image_account_concurrency is 3; on the buggy code path
        # repeated-leak tests would deadlock waiting for a slot once the
        # leaked count reaches the cap. Raise it for the duration of these
        # tests so the assertions can fire instead of hanging.
        self._patch_concurrency = patch.dict(
            config.data, {"image_account_concurrency": 100}
        )
        self._patch_concurrency.start()
        self.addCleanup(self._patch_concurrency.stop)

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
            list(stream_image_outputs_with_pool(_request()))
        self.assertEqual(self.service._image_inflight, {})

    def test_no_leak_under_repeated_timeouts(self) -> None:
        # 50 iterations directly reproduces the production accumulation pattern.
        # On the buggy code path every iteration leaks one slot; the test bumps
        # image_account_concurrency in setUp to keep get_available_access_token
        # from blocking on the leaked-slot deadlock so the assertion can fire.
        self._patch_stream(_stub_stream_timeout)
        for _ in range(50):
            with self.assertRaises(ImagePollTimeoutError):
                list(stream_image_outputs_with_pool(_request()))
        self.assertEqual(self.service._image_inflight, {})


if __name__ == "__main__":
    unittest.main()
