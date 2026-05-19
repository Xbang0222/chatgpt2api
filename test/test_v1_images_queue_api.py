from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
from api.errors import install_exception_handlers
from services.image_task_service import ImageQueueFullError


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class FakeImageTaskService:
    def __init__(self, result=None, error: Exception | None = None):
        self.result = result or {
            "status": "success",
            "created": 123,
            "data": [{"url": "http://testserver/images/fake.png"}],
        }
        self.error = error
        self.calls = []

    async def submit_and_wait_async(self, identity, *, mode, payload, timeout):
        self.calls.append({
            "identity": identity,
            "mode": mode,
            "payload": payload,
            "timeout": timeout,
        })
        if self.error is not None:
            raise self.error
        return self.result


class OpenAIImageQueueApiTests(unittest.TestCase):
    def make_client(self, service: FakeImageTaskService) -> TestClient:
        self.service_patcher = mock.patch.object(ai_module, "image_task_service", service)
        self.filter_patcher = mock.patch.object(ai_module, "check_request", lambda _text: None)
        self.service_patcher.start()
        self.filter_patcher.start()
        self.addCleanup(self.service_patcher.stop)
        self.addCleanup(self.filter_patcher.stop)

        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())
        return TestClient(app)

    def test_generation_uses_image_worker_queue(self):
        service = FakeImageTaskService()
        client = self.make_client(service)

        response = client.post(
            "/v1/images/generations",
            headers=AUTH_HEADERS,
            json={"prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["created"], 123)
        self.assertEqual(service.calls[0]["mode"], "generate")
        self.assertEqual(service.calls[0]["payload"]["base_url"], "http://testserver")

    def test_worker_error_status_propagates_to_openai_response(self):
        service = FakeImageTaskService({
            "status": "error",
            "error": "rate limited",
            "status_code": 429,
            "error_type": "rate_limit_error",
            "code": "rate_limit_exceeded",
            "data": [],
        })
        client = self.make_client(service)

        response = client.post(
            "/v1/images/generations",
            headers=AUTH_HEADERS,
            json={"prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 429, response.text)
        payload = response.json()
        self.assertEqual(payload["error"]["message"], "rate limited")
        self.assertEqual(payload["error"]["type"], "rate_limit_error")
        self.assertEqual(payload["error"]["code"], "rate_limit_exceeded")

    def test_queue_full_returns_retry_after(self):
        service = FakeImageTaskService(error=ImageQueueFullError(retry_after=7))
        client = self.make_client(service)

        response = client.post(
            "/v1/images/generations",
            headers=AUTH_HEADERS,
            json={"prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(response.headers["retry-after"], "7")
        self.assertIn("image worker pool is full", response.json()["error"]["message"])


if __name__ == "__main__":
    unittest.main()
