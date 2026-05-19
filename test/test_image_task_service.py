from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from services.image_task_service import ImageQueueFullError, ImageTaskService
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol.conversation import ImageGenerationError


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}
OTHER_OWNER = {"id": "owner-2", "name": "Other", "role": "user"}


def wait_for_task(service: ImageTaskService, identity: dict[str, object], task_id: str, status: str, timeout: float = 2.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        result = service.list_tasks(identity, [task_id])
        last = (result.get("items") or [None])[0]
        if last and last.get("status") == status:
            return last
        time.sleep(0.02)
    raise AssertionError(f"task {task_id} did not reach {status}, last={last}")


def wait_for_inflight(service: ImageTaskService, value: int, timeout: float = 2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if service.current_inflight == value:
            return
        time.sleep(0.02)
    raise AssertionError(f"inflight did not reach {value}, current={service.current_inflight}")


class ImageTaskServiceTests(unittest.TestCase):
    def make_service(self, path: Path, handler=None) -> ImageTaskService:
        return ImageTaskService(
            path,
            generation_handler=handler or (lambda _payload: {"data": [{"url": "http://example.test/image.png"}]}),
            edit_handler=handler or (lambda _payload: {"data": [{"url": "http://example.test/edit.png"}]}),
            retention_days_getter=lambda: 30,
        )

    def test_duplicate_submit_uses_existing_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            calls = 0

            def handler(_payload):
                nonlocal calls
                calls += 1
                time.sleep(0.05)
                return {"data": [{"url": "http://example.test/image.png"}]}

            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            first = service.submit_generation(
                OWNER,
                client_task_id="task-1",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            second = service.submit_generation(
                OWNER,
                client_task_id="task-1",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            self.assertEqual(first["id"], "task-1")
            self.assertEqual(second["id"], "task-1")
            task = wait_for_task(service, OWNER, "task-1", "success")
            self.assertEqual(task["data"][0]["url"], "http://example.test/image.png")
            self.assertEqual(calls, 1)

    def test_different_owner_cannot_query_task(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            service.submit_generation(
                OWNER,
                client_task_id="private-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )

            wait_for_task(service, OWNER, "private-task", "success")
            result = service.list_tasks(OTHER_OWNER, ["private-task"])

            self.assertEqual(result["items"], [])
            self.assertEqual(result["missing_ids"], ["private-task"])

    def test_success_task_persists_to_new_service_instance(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            service = self.make_service(path)
            service.submit_generation(
                OWNER,
                client_task_id="persisted-task",
                prompt="cat",
                model="gpt-image-2",
                size=None,
                base_url="http://local.test",
            )
            wait_for_task(service, OWNER, "persisted-task", "success")

            reloaded = self.make_service(path)
            result = reloaded.list_tasks(OWNER, ["persisted-task"])

            self.assertEqual(result["missing_ids"], [])
            self.assertEqual(result["items"][0]["status"], "success")
            self.assertEqual(result["items"][0]["data"][0]["url"], "http://example.test/image.png")

    def test_startup_marks_unfinished_tasks_as_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            path.write_text(
                json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "queued-task",
                                "owner_id": "owner-1",
                                "status": "queued",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": "2099-01-01 00:00:00",
                                "updated_at": "2099-01-01 00:00:00",
                            },
                            {
                                "id": "running-task",
                                "owner_id": "owner-1",
                                "status": "running",
                                "mode": "generate",
                                "model": "gpt-image-2",
                                "created_at": "2099-01-01 00:00:00",
                                "updated_at": "2099-01-01 00:00:00",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            service = self.make_service(path)
            result = service.list_tasks(OWNER, ["queued-task", "running-task"])

            self.assertEqual([item["status"] for item in result["items"]], ["error", "error"])
            self.assertTrue(all("已中断" in item.get("error", "") for item in result["items"]))

    def test_worker_slots_reject_when_full(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            service.configure_max_workers(1)
            service.acquire_slot()
            try:
                with self.assertRaises(ImageQueueFullError) as ctx:
                    service.acquire_slot()

                self.assertEqual(ctx.exception.retry_after, 5)
                self.assertEqual(service.current_inflight, 1)
                self.assertEqual(service.rejection_count_24h, 1)
            finally:
                service.release_slot()

            self.assertEqual(service.current_inflight, 0)


class ImageTaskServiceOpenAIWorkerTests(unittest.IsolatedAsyncioTestCase):
    def make_service(self, path: Path, handler=None) -> ImageTaskService:
        return ImageTaskService(
            path,
            generation_handler=handler or (lambda _payload: {"created": 123, "data": [{"url": "http://example.test/image.png"}]}),
            edit_handler=handler or (lambda _payload: {"created": 123, "data": [{"url": "http://example.test/edit.png"}]}),
            retention_days_getter=lambda: 30,
        )

    async def test_submit_and_wait_async_success_releases_slot(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json")
            service.configure_max_workers(1)

            task = await service.submit_and_wait_async(
                OWNER,
                mode="generate",
                payload={"prompt": "cat", "model": "gpt-image-2"},
                timeout=1.0,
            )

            self.assertEqual(task["status"], "success")
            self.assertEqual(task["created"], 123)
            self.assertEqual(service.current_inflight, 0)

    async def test_submit_and_wait_async_error_preserves_status_and_releases_slot(self):
        def handler(_payload):
            raise ImageGenerationError(
                "rate limited",
                status_code=429,
                error_type="rate_limit_error",
                code="rate_limit_exceeded",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            service.configure_max_workers(1)

            task = await service.submit_and_wait_async(
                OWNER,
                mode="generate",
                payload={"prompt": "cat", "model": "gpt-image-2"},
                timeout=1.0,
            )

            self.assertEqual(task["status"], "error")
            self.assertEqual(task["status_code"], 429)
            self.assertEqual(task["error_type"], "rate_limit_error")
            self.assertEqual(task["code"], "rate_limit_exceeded")
            self.assertEqual(service.current_inflight, 0)

    async def test_submit_and_wait_async_timeout_keeps_slot_until_worker_finishes(self):
        gate = threading.Event()

        def handler(_payload):
            gate.wait(1.0)
            return {"created": 123, "data": [{"url": "http://example.test/image.png"}]}

        with tempfile.TemporaryDirectory() as tmp_dir:
            service = self.make_service(Path(tmp_dir) / "image_tasks.json", handler)
            service.configure_max_workers(1)

            with self.assertRaises(ImagePollTimeoutError):
                await service.submit_and_wait_async(
                    OWNER,
                    mode="generate",
                    payload={"prompt": "cat", "model": "gpt-image-2"},
                    timeout=0.02,
                )

            self.assertEqual(service.current_inflight, 1)
            gate.set()
            wait_for_inflight(service, 0)


if __name__ == "__main__":
    unittest.main()
