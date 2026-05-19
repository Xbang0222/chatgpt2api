"""Tests for upstream HTTP status code propagation.

Pinned contract:
- 4xx from UpstreamHTTPError, or any exception with status_code, is propagated.
- 5xx or missing status_code falls back to 502, the gateway-failure default.
- Image streaming and chat-text logged calls honor the same contract.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
root_path = str(ROOT_DIR)
if root_path in sys.path:
    sys.path.remove(root_path)
sys.path.insert(0, root_path)
loaded_utils = sys.modules.get("utils")
if loaded_utils is not None and not hasattr(loaded_utils, "__path__"):
    del sys.modules["utils"]

from services.log_service import LoggedCall
from services.protocol.conversation import (
    ConversationRequest,
    ImageGenerationError,
    stream_image_outputs_with_pool,
)
from services.protocol.error_response import upstream_status_or_default
from utils.helper import UpstreamHTTPError


class UpstreamStatusOrDefaultTests(unittest.TestCase):
    """Unit tests for the shared helper itself."""

    def test_4xx_propagates(self):
        for status in (400, 403, 413, 422, 429):
            with self.subTest(status=status):
                exc = UpstreamHTTPError("/x", status, "")
                self.assertEqual(upstream_status_or_default(exc), status)

    def test_5xx_returns_default(self):
        for status in (500, 502, 503, 504):
            with self.subTest(status=status):
                exc = UpstreamHTTPError("/x", status, "")
                self.assertEqual(upstream_status_or_default(exc), 502)

    def test_no_status_attribute_returns_default(self):
        self.assertEqual(upstream_status_or_default(RuntimeError("boom")), 502)

    def test_custom_default(self):
        self.assertEqual(
            upstream_status_or_default(RuntimeError("boom"), default=500),
            500,
        )

    def test_3xx_returns_default(self):
        exc = UpstreamHTTPError("/x", 301, "")
        self.assertEqual(upstream_status_or_default(exc), 502)


def _raise_upstream(status_code: int, body: str = ""):
    """Build a stream function that raises UpstreamHTTPError immediately."""

    def _stream(*_args, **_kwargs):
        raise UpstreamHTTPError("/backend-api/f/conversation", status_code, body)
        yield

    return _stream


class ImageStreamingPropagationTests(unittest.TestCase):
    """Integration test: stream_image_outputs_with_pool propagates upstream 4xx."""

    def _drive(self, status_code: int) -> ImageGenerationError:
        request = ConversationRequest(prompt="test", model="gpt-image-2", n=1)
        with (
            patch("services.protocol.conversation.account_service") as mock_acc,
            patch("services.protocol.conversation.OpenAIBackendAPI") as MockBackend,
        ):
            mock_acc.get_available_access_token.return_value = "fake-token"
            MockBackend.return_value.stream_conversation = _raise_upstream(status_code)
            with self.assertRaises(ImageGenerationError) as ctx:
                list(stream_image_outputs_with_pool(request))
        return ctx.exception

    def test_413_propagates_with_invalid_request_error_type(self):
        exc = self._drive(413)
        self.assertEqual(exc.status_code, 413)
        self.assertEqual(exc.error_type, "invalid_request_error")

    def test_429_propagates_with_rate_limit_error_type(self):
        exc = self._drive(429)
        self.assertEqual(exc.status_code, 429)
        self.assertEqual(exc.error_type, "rate_limit_error")

    def test_403_propagates_with_permission_error_type(self):
        exc = self._drive(403)
        self.assertEqual(exc.status_code, 403)
        self.assertEqual(exc.error_type, "permission_error")

    def test_5xx_stays_502_with_server_error_type(self):
        exc = self._drive(503)
        self.assertEqual(exc.status_code, 502)
        self.assertEqual(exc.error_type, "server_error")


class LoggedCallPropagationTests(unittest.IsolatedAsyncioTestCase):
    """Direct tests for chat-text error handling through LoggedCall."""

    def _call(self) -> LoggedCall:
        return LoggedCall(identity={}, endpoint="/v1/chat/completions", model="gpt", summary="test")

    async def test_handler_4xx_exception_propagates(self):
        def handler():
            raise UpstreamHTTPError("/backend-api/conversation", 413, "")

        response = await self._call().run(handler)
        payload = json.loads(response.body.decode("utf-8"))

        self.assertEqual(response.status_code, 413)
        self.assertEqual(payload["error"]["type"], "invalid_request_error")

    async def test_stream_first_item_4xx_exception_propagates(self):
        def handler():
            return _raise_upstream(429)()

        response = await self._call().run(handler)
        payload = json.loads(response.body.decode("utf-8"))

        self.assertEqual(response.status_code, 429)
        self.assertEqual(payload["error"]["type"], "rate_limit_error")

    async def test_handler_5xx_exception_stays_502(self):
        def handler():
            raise UpstreamHTTPError("/backend-api/conversation", 503, "")

        response = await self._call().run(handler)
        payload = json.loads(response.body.decode("utf-8"))

        self.assertEqual(response.status_code, 502)
        self.assertEqual(payload["error"]["type"], "server_error")


if __name__ == "__main__":
    unittest.main()
