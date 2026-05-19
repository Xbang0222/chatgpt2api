from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from services.config import DATA_DIR, config
from services.content_filter import request_text
from services.log_service import LOG_TYPE_CALL, _request_excerpt, log_service
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol import openai_v1_image_edit, openai_v1_image_generations
from services.protocol.conversation import ImageGenerationError
from services.protocol.error_response import error_message_from_detail
from utils.helper import UpstreamHTTPError
from utils.log import logger

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_ERROR = "error"
TERMINAL_STATUSES = {TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}
UNFINISHED_STATUSES = {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _timestamp(value: object) -> float:
    if not isinstance(value, str) or not value.strip():
        return 0.0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:26], fmt).timestamp()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _clean(value: object, default: str = "") -> str:
    return str(value or default).strip()


def _owner_id(identity: dict[str, object]) -> str:
    return _clean(identity.get("id")) or "anonymous"


def _task_key(owner_id: str, task_id: str) -> str:
    return f"{owner_id}:{task_id}"


def _collect_image_urls(data: list[Any]) -> list[str]:
    urls: list[str] = []
    for item in data:
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str) and url:
                urls.append(url)
    return urls


class ImageQueueFullError(Exception):
    DEFAULT_RETRY_AFTER_SECONDS = 5

    def __init__(self, retry_after: int = DEFAULT_RETRY_AFTER_SECONDS) -> None:
        self.retry_after = retry_after
        super().__init__("image worker pool is full")


def _safe_error_message(exc: Exception) -> str:
    """Extract a client-safe message from an upstream exception.

    UpstreamHTTPError.__str__ embeds the upstream URL path in the prefix,
    which is internal routing detail we don't want leaking through task["error"].
    """
    if isinstance(exc, UpstreamHTTPError):
        body = getattr(exc, "body", "") or ""
        if body:
            try:
                parsed = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                parsed = None
            if parsed is not None:
                message = error_message_from_detail(parsed)
                if message:
                    return message
        status = getattr(exc, "status_code", 0) or 0
        return f"upstream returned HTTP {status}" if status else "upstream request failed"
    text = str(exc).strip()
    return text or "image task failed"


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    item = {
        "id": task.get("id"),
        "status": task.get("status"),
        "mode": task.get("mode"),
        "model": task.get("model"),
        "size": task.get("size"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
    }
    if task.get("data") is not None:
        item["data"] = task.get("data")
    if task.get("error"):
        item["error"] = task.get("error")
    return item


class ImageTaskService:
    def __init__(
        self,
        path: Path,
        *,
        generation_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_generations.handle,
        edit_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_edit.handle,
        retention_days_getter: Callable[[], int] | None = None,
    ):
        self.path = path
        self.generation_handler = generation_handler
        self.edit_handler = edit_handler
        self.retention_days_getter = retention_days_getter or (lambda: config.image_retention_days)
        self._lock = threading.RLock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self._semaphore: threading.BoundedSemaphore | None = None
        self._max_workers_effective = 0
        self._inflight_count = 0
        self._rejection_timestamps: list[float] = []
        self._oai_callbacks: dict[str, Callable[[dict[str, Any]], None]] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._tasks = self._load_locked()
            changed = self._recover_unfinished_locked()
            changed = self._cleanup_locked() or changed
            if changed:
                self._save_locked()

    def configure_max_workers(self, max_workers: int) -> None:
        value = max(1, int(max_workers))
        with self._lock:
            if self._semaphore is not None:
                raise RuntimeError("image worker pool is already configured")
            self._semaphore = threading.BoundedSemaphore(value)
            self._max_workers_effective = value

    def acquire_slot(self) -> None:
        semaphore = self._semaphore
        if semaphore is None:
            raise RuntimeError("image worker pool is not configured")
        if not semaphore.acquire(blocking=False):
            with self._lock:
                self._rejection_timestamps.append(time.time())
                self._trim_old_rejections_locked()
                current_inflight = self._inflight_count
            logger.warning({"event": "image_queue_full_reject", "current_inflight": current_inflight})
            raise ImageQueueFullError()
        with self._lock:
            self._inflight_count += 1

    def release_slot(self) -> None:
        semaphore = self._semaphore
        if semaphore is not None:
            semaphore.release()
        with self._lock:
            self._inflight_count -= 1

    def wait_for_inflight(self, timeout: float) -> None:
        """Best-effort wait for in-flight OAI workers before shutdown."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and self.current_inflight > 0:
            time.sleep(0.05)

    async def submit_and_wait_async(
        self,
        identity: dict[str, object],
        *,
        mode: str,
        payload: dict[str, Any],
        timeout: float,
    ) -> dict[str, Any]:
        self.acquire_slot()

        task_id = f"oai-{uuid.uuid4().hex[:16]}"
        callback_registered = False
        try:
            loop = asyncio.get_running_loop()
            event = asyncio.Event()
            result_container: dict[str, dict[str, Any]] = {}

            def on_complete(task: dict[str, Any]) -> None:
                result_container["task"] = task
                try:
                    loop.call_soon_threadsafe(event.set)
                except RuntimeError:
                    pass

            with self._lock:
                self._oai_callbacks[task_id] = on_complete
                callback_registered = True

            thread = threading.Thread(
                target=self._run_oai_task,
                args=(task_id, mode, dict(payload)),
                daemon=True,
                name=f"image-oai-{task_id[:12]}",
            )
            logger.info({"event": "image_task_submitted", "task_id": task_id, "mode": mode})
            thread.start()
        except Exception:
            if callback_registered:
                with self._lock:
                    self._oai_callbacks.pop(task_id, None)
            self.release_slot()
            raise

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise ImagePollTimeoutError(f"等待图片生成超时（{timeout}s），后台任务仍在执行") from exc

        task = result_container.get("task")
        if task is None:
            raise RuntimeError("image task completed without result")
        return task

    def submit_generation(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        base_url: str,
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "model": model,
            "n": 1,
            "size": size,
            "response_format": "url",
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="generate", payload=payload)

    def submit_edit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        base_url: str,
        images: list[tuple[bytes, str, str]],
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "images": images,
            "model": model,
            "n": 1,
            "size": size,
            "response_format": "url",
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="edit", payload=payload)

    def list_tasks(self, identity: dict[str, object], task_ids: list[str]) -> dict[str, Any]:
        owner = _owner_id(identity)
        requested_ids = [_clean(task_id) for task_id in task_ids if _clean(task_id)]
        with self._lock:
            if self._cleanup_locked():
                self._save_locked()
            items = []
            missing_ids = []
            for task_id in requested_ids:
                task = self._tasks.get(_task_key(owner, task_id))
                if task is None:
                    missing_ids.append(task_id)
                else:
                    items.append(_public_task(task))
            if not requested_ids:
                items = [
                    _public_task(task)
                    for task in self._tasks.values()
                    if task.get("owner_id") == owner
                ]
                items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
                missing_ids = []
            return {"items": items, "missing_ids": missing_ids}

    def _submit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        mode: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        task_id = _clean(client_task_id)
        if not task_id:
            raise ValueError("client_task_id is required")
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        now = _now_iso()
        should_start = False
        with self._lock:
            cleaned = self._cleanup_locked()
            task = self._tasks.get(key)
            if task is not None:
                if cleaned:
                    self._save_locked()
                return _public_task(task)
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": TASK_STATUS_QUEUED,
                "mode": mode,
                "model": _clean(payload.get("model"), "gpt-image-2"),
                "size": _clean(payload.get("size")),
                "created_at": now,
                "updated_at": now,
            }
            self._tasks[key] = task
            self._save_locked()
            should_start = True

        if should_start:
            thread = threading.Thread(
                target=self._run_task,
                args=(key, mode, payload, dict(identity), _clean(payload.get("model"), "gpt-image-2")),
                name=f"image-task-{task_id[:16]}",
                daemon=True,
            )
            thread.start()
        return _public_task(task)

    def _run_task(
        self,
        key: str,
        mode: str,
        payload: dict[str, Any],
        identity: dict[str, object],
        model: str,
    ) -> None:
        started = time.time()
        self._update_task(key, status=TASK_STATUS_RUNNING, error="")
        try:
            handler = self.edit_handler if mode == "edit" else self.generation_handler
            result = handler(payload)
            if not isinstance(result, dict):
                raise RuntimeError("image task returned streaming result unexpectedly")
            data = result.get("data")
            if not isinstance(data, list) or not data:
                upstream = _clean(result.get("message"))
                if upstream:
                    message = upstream
                else:
                    message = "号池中没有可用账号或所有账号均被限流，请检查号池状态（账号额度、是否被封禁、是否到达生图上限）"
                raise RuntimeError(message)
            self._update_task(key, status=TASK_STATUS_SUCCESS, data=data, error="")
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成",
                request_preview=request_text(payload.get("prompt")),
                urls=_collect_image_urls(data),
            )
        except Exception as exc:
            error_message = str(exc) or "image task failed"
            self._update_task(key, status=TASK_STATUS_ERROR, error=error_message, data=[])
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败",
                request_preview=request_text(payload.get("prompt")),
                status="failed",
                error=error_message,
            )

    def _run_oai_task(
        self,
        task_id: str,
        mode: str,
        payload: dict[str, Any],
    ) -> None:
        started = time.time()
        task_dict: dict[str, Any]
        try:
            handler = self.edit_handler if mode == "edit" else self.generation_handler
            result = handler(payload)
            if not isinstance(result, dict):
                raise RuntimeError("internal error: handler returned a streaming iterator")

            data = result.get("data")
            message = _clean(result.get("message"))
            if not isinstance(data, list) or not data:
                if message:
                    raise RuntimeError(message)
                raise RuntimeError("号池中没有可用账号或所有账号均被限流，请检查号池状态（账号额度、是否被封禁、是否到达生图上限）")

            task_dict = {
                "status": TASK_STATUS_SUCCESS,
                "data": data,
                "created": int(result.get("created") or time.time()),
            }
        except ImagePollTimeoutError as exc:
            task_dict = self._make_error_task_dict(str(exc), status_code=504)
        except ImageGenerationError as exc:
            status_code = int(getattr(exc, "status_code", 502) or 502)
            error_type = getattr(exc, "error_type", None)
            code = getattr(exc, "code", None)
            if "no available image quota" in str(exc).lower():
                status_code = 429
                error_type = "insufficient_quota"
                code = "insufficient_quota"
            task_dict = self._make_error_task_dict(
                str(exc),
                status_code=status_code,
                error_type=error_type,
                code=code,
                param=getattr(exc, "param", None),
            )
        except HTTPException as exc:
            task_dict = self._make_error_task_dict(
                error_message_from_detail(exc.detail),
                status_code=exc.status_code,
            )
        except Exception as exc:
            task_dict = self._make_error_task_dict(_safe_error_message(exc), status_code=502)
        finally:
            self.release_slot()

        logger.info({
            "event": "image_task_completed",
            "task_id": task_id,
            "duration_ms": int((time.time() - started) * 1000),
            "status": task_dict.get("status"),
        })
        with self._lock:
            callback = self._oai_callbacks.pop(task_id, None)
        if callback is not None:
            try:
                callback(task_dict)
            except Exception as exc:
                logger.warning({"event": "oai_callback_error", "error": str(exc)})

    def _make_error_task_dict(
        self,
        error: str,
        *,
        status_code: int,
        error_type: str | None = None,
        code: str | None = None,
        param: str | None = None,
    ) -> dict[str, Any]:
        task: dict[str, Any] = {
            "status": TASK_STATUS_ERROR,
            "error": error or "image task failed",
            "status_code": status_code,
            "data": [],
            "created": int(time.time()),
        }
        if error_type:
            task["error_type"] = error_type
        if code:
            task["code"] = code
        if param:
            task["param"] = param
        return task

    def _log_call(
        self,
        identity: dict[str, object],
        mode: str,
        model: str,
        started: float,
        suffix: str,
        *,
        request_preview: str = "",
        status: str = "success",
        error: str = "",
        urls: list[str] | None = None,
    ) -> None:
        endpoint = "/v1/images/edits" if mode == "edit" else "/v1/images/generations"
        summary_prefix = "图生图" if mode == "edit" else "文生图"
        detail = {
            "key_id": identity.get("id"),
            "key_name": identity.get("name"),
            "role": identity.get("role"),
            "endpoint": endpoint,
            "model": model,
            "started_at": datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": _now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "status": status,
        }
        excerpt = _request_excerpt(request_preview)
        if excerpt:
            detail["request_text"] = excerpt
        if error:
            detail["error"] = error
        if urls:
            detail["urls"] = list(dict.fromkeys(urls))
        try:
            log_service.add(LOG_TYPE_CALL, f"{summary_prefix}{suffix}", detail)
        except Exception:
            pass

    def _update_task(self, key: str, **updates: Any) -> None:
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return
            task.update(updates)
            task["updated_at"] = _now_iso()
            self._save_locked()

    def _load_locked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        raw_items = raw.get("tasks") if isinstance(raw, dict) else raw
        if not isinstance(raw_items, list):
            return {}
        tasks: dict[str, dict[str, Any]] = {}
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            task_id = _clean(item.get("id"))
            owner = _clean(item.get("owner_id"))
            if not task_id or not owner:
                continue
            status = _clean(item.get("status"))
            if status not in {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING, TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}:
                status = TASK_STATUS_ERROR
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": status,
                "mode": "edit" if item.get("mode") == "edit" else "generate",
                "model": _clean(item.get("model"), "gpt-image-2"),
                "size": _clean(item.get("size")),
                "created_at": _clean(item.get("created_at"), _now_iso()),
                "updated_at": _clean(item.get("updated_at"), _clean(item.get("created_at"), _now_iso())),
            }
            data = item.get("data")
            if isinstance(data, list):
                task["data"] = data
            error = _clean(item.get("error"))
            if error:
                task["error"] = error
            tasks[_task_key(owner, task_id)] = task
        return tasks

    def _save_locked(self) -> None:
        items = sorted(self._tasks.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps({"tasks": items}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.path)

    def _recover_unfinished_locked(self) -> bool:
        changed = False
        for task in self._tasks.values():
            if task.get("status") in UNFINISHED_STATUSES:
                task["status"] = TASK_STATUS_ERROR
                task["error"] = "服务已重启，未完成的图片任务已中断"
                task["updated_at"] = _now_iso()
                changed = True
        return changed

    def _cleanup_locked(self) -> bool:
        try:
            retention_days = max(1, int(self.retention_days_getter()))
        except Exception:
            retention_days = 30
        cutoff = time.time() - retention_days * 86400
        removed_keys = [
            key
            for key, task in self._tasks.items()
            if task.get("status") in TERMINAL_STATUSES and _timestamp(task.get("updated_at")) < cutoff
        ]
        for key in removed_keys:
            self._tasks.pop(key, None)
        return bool(removed_keys)

    def _trim_old_rejections_locked(self) -> None:
        cutoff = time.time() - 86400
        self._rejection_timestamps = [item for item in self._rejection_timestamps if item >= cutoff]

    @property
    def current_inflight(self) -> int:
        with self._lock:
            return self._inflight_count

    @property
    def rejection_count_24h(self) -> int:
        with self._lock:
            self._trim_old_rejections_locked()
            return len(self._rejection_timestamps)

    @property
    def max_workers_effective(self) -> int:
        with self._lock:
            return self._max_workers_effective


image_task_service = ImageTaskService(DATA_DIR / "image_tasks.json")
