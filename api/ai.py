from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from api.image_inputs import parse_image_edit_request, read_image_sources
from api.support import require_identity, resolve_image_base_url
from services.config import config
from services.content_filter import check_request, request_text
from services.image_task_service import ImageQueueFullError, image_task_service
from services.log_service import LoggedCall, _collect_urls
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol import (
    anthropic_v1_messages,
    openai_v1_chat_complete,
    openai_v1_image_edit,
    openai_v1_image_generations,
    openai_v1_models,
    openai_v1_response,
)
from services.protocol.error_response import openai_error_payload
from utils.log import logger

STREAM_QUEUE_MAXSIZE = 64


class ImageGenerationRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    model: str = "gpt-image-2"
    n: int = Field(default=1, ge=1, le=4)
    size: str | None = None
    response_format: str = "b64_json"
    history_disabled: bool = True
    stream: bool | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str | None = None
    prompt: str | None = None
    n: int | None = None
    stream: bool | None = None
    modalities: list[str] | None = None
    messages: list[dict[str, object]] | None = None


class ResponseCreateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str | None = None
    input: object | None = None
    tools: list[dict[str, object]] | None = None
    tool_choice: object | None = None
    stream: bool | None = None


class AnthropicMessageRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    model: str | None = None
    messages: list[dict[str, object]] | None = None
    system: object | None = None
    stream: bool | None = None


async def filter_or_log(call: LoggedCall, text: str) -> None:
    try:
        await run_in_threadpool(check_request, text)
    except HTTPException as exc:
        call.log("调用失败", status="failed", error=str(exc.detail))
        raise


def _task_to_openai_image_response(task: dict[str, Any]) -> dict[str, Any]:
    data = task.get("data") or []
    if not data:
        raise HTTPException(status_code=502, detail={"error": "image generation returned no data"})
    return {
        "created": int(task.get("created") or 0),
        "data": data,
    }


def _raise_image_task_error(task: dict[str, Any], fallback: str) -> None:
    error = str(task.get("error") or fallback)
    status_code = int(task.get("status_code") or 502)
    error_type = task.get("error_type")
    code = task.get("code")
    param = task.get("param")
    detail = openai_error_payload(
        {"error": {"message": error}},
        status_code,
        error_type=error_type,
        code=code,
        param=param,
    )
    raise HTTPException(status_code=status_code, detail=detail)


async def _async_sse_json_stream(items: AsyncIterator[dict[str, Any]], call: LoggedCall) -> AsyncIterator[str]:
    urls: list[str] = []
    failed = False
    error_message = ""
    yield ": stream-open\n\n"
    try:
        async for item in items:
            urls.extend(_collect_urls(item))
            if isinstance(item, dict) and item.get("error"):
                failed = True
                error_message = json.dumps(item.get("error"), ensure_ascii=False)
            yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
    except Exception as exc:
        failed = True
        logger.warning({
            "event": "image_stream_bridge_error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        })
        error_message = str(exc) or "image stream interrupted"
        error = {"error": {"message": error_message, "type": "server_error"}}
        yield f"data: {json.dumps(error, ensure_ascii=False)}\n\n"
    finally:
        if failed:
            call.log("流式调用失败", status="failed", error=error_message or "image stream failed", urls=urls)
        else:
            call.log("流式调用结束", urls=urls)
    yield "data: [DONE]\n\n"


async def _stream_image_via_worker_thread(
    handler: Callable[[dict[str, Any]], dict[str, Any] | Iterator[dict[str, Any]]],
    payload: dict[str, Any],
    call: LoggedCall,
) -> StreamingResponse:
    """Run the upstream image handler on a dedicated thread bounded by the
    same worker semaphore as the sync path. A fixed-size shared executor
    could starve other requests; one thread per stream keeps concurrency
    capped only by the semaphore."""
    image_task_service.acquire_slot()
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[dict[str, Any] | object] = asyncio.Queue(maxsize=STREAM_QUEUE_MAXSIZE)
    done = object()

    def _safe_put(item: dict[str, Any] | object) -> None:
        # The done sentinel must always land so the consumer can finish; for
        # data chunks we silently drop on backpressure rather than ballooning
        # memory for slow/disconnected clients.
        if item is done:
            while True:
                try:
                    queue.put_nowait(item)
                    return
                except asyncio.QueueFull:
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
        else:
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                pass

    def enqueue(item: dict[str, Any] | object) -> None:
        try:
            loop.call_soon_threadsafe(_safe_put, item)
        except RuntimeError:
            pass

    def producer() -> None:
        try:
            result = handler(payload)
            if isinstance(result, dict):
                enqueue(result)
            else:
                for chunk in result:
                    enqueue(chunk)
        except Exception as exc:
            if hasattr(exc, "to_openai_error"):
                error = exc.to_openai_error()
            else:
                error = {
                    "error": {
                        "message": str(exc) or "image generation failed",
                        "type": "server_error",
                    }
                }
            enqueue(error)
        finally:
            image_task_service.release_slot()
            enqueue(done)

    try:
        thread = threading.Thread(target=producer, daemon=True, name="image-stream")
        thread.start()
    except Exception:
        image_task_service.release_slot()
        raise

    async def consumer() -> AsyncIterator[dict[str, Any]]:
        while True:
            item = await queue.get()
            if item is done:
                return
            yield item

    return StreamingResponse(_async_sse_json_stream(consumer(), call), media_type="text/event-stream")


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/v1/models")
    async def list_models(authorization: str | None = Header(default=None)):
        require_identity(authorization)
        try:
            return await run_in_threadpool(openai_v1_models.list_models)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": str(exc)}) from exc

    @router.post("/v1/images/generations")
    async def generate_images(
            body: ImageGenerationRequest,
            request: Request,
            authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        payload = body.model_dump(mode="python")
        payload["base_url"] = resolve_image_base_url(request)
        call = LoggedCall(identity, "/v1/images/generations", body.model, "文生图", request_text=body.prompt)
        await filter_or_log(call, body.prompt)
        try:
            if body.stream:
                return await _stream_image_via_worker_thread(openai_v1_image_generations.handle, payload, call)
            task = await image_task_service.submit_and_wait_async(
                identity,
                mode="generate",
                payload=payload,
                timeout=config.image_poll_timeout_secs + 60,
            )
        except ImageQueueFullError as exc:
            call.log("调用失败", status="failed", error="image queue full")
            raise HTTPException(
                status_code=503,
                detail={"error": "image worker pool is full, retry later"},
                headers={"Retry-After": str(exc.retry_after)},
            ) from exc
        except ImagePollTimeoutError as exc:
            call.log("调用失败", status="failed", error=str(exc))
            raise HTTPException(status_code=504, detail={"error": str(exc)}) from exc

        if task.get("status") == "error":
            call.log("调用失败", status="failed", error=str(task.get("error") or ""))
            _raise_image_task_error(task, "image generation failed")
        response = _task_to_openai_image_response(task)
        call.log("调用完成", response)
        return response

    @router.post("/v1/images/edits")
    async def edit_images(
            request: Request,
            authorization: str | None = Header(default=None),
    ):
        identity = require_identity(authorization)
        payload, image_sources = await parse_image_edit_request(request)
        prompt = str(payload["prompt"])
        model = str(payload["model"])
        call = LoggedCall(identity, "/v1/images/edits", model, "图生图", request_text=prompt)
        await filter_or_log(call, prompt)
        payload["images"] = await read_image_sources(image_sources)
        payload["base_url"] = resolve_image_base_url(request)
        try:
            if payload.get("stream"):
                return await _stream_image_via_worker_thread(openai_v1_image_edit.handle, payload, call)
            task = await image_task_service.submit_and_wait_async(
                identity,
                mode="edit",
                payload=payload,
                timeout=config.image_poll_timeout_secs + 60,
            )
        except ImageQueueFullError as exc:
            call.log("调用失败", status="failed", error="image queue full")
            raise HTTPException(
                status_code=503,
                detail={"error": "image worker pool is full, retry later"},
                headers={"Retry-After": str(exc.retry_after)},
            ) from exc
        except ImagePollTimeoutError as exc:
            call.log("调用失败", status="failed", error=str(exc))
            raise HTTPException(status_code=504, detail={"error": str(exc)}) from exc

        if task.get("status") == "error":
            call.log("调用失败", status="failed", error=str(task.get("error") or ""))
            _raise_image_task_error(task, "image edit failed")
        response = _task_to_openai_image_response(task)
        call.log("调用完成", response)
        return response

    @router.post("/v1/chat/completions")
    async def create_chat_completion(body: ChatCompletionRequest, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("prompt"), payload.get("messages"))
        call = LoggedCall(identity, "/v1/chat/completions", model, "文本生成", request_text=request_preview)
        await filter_or_log(call, request_preview)
        return await call.run(openai_v1_chat_complete.handle, payload)

    @router.post("/v1/responses")
    async def create_response(body: ResponseCreateRequest, authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("input"), payload.get("instructions"))
        call = LoggedCall(identity, "/v1/responses", model, "Responses", request_text=request_preview)
        await filter_or_log(call, request_preview)
        return await call.run(openai_v1_response.handle, payload)

    @router.post("/v1/messages")
    async def create_message(
            body: AnthropicMessageRequest,
            authorization: str | None = Header(default=None),
            x_api_key: str | None = Header(default=None, alias="x-api-key"),
            anthropic_version: str | None = Header(default=None, alias="anthropic-version"),
    ):
        identity = require_identity(authorization or (f"Bearer {x_api_key}" if x_api_key else None))
        payload = body.model_dump(mode="python")
        model = str(payload.get("model") or "auto")
        request_preview = request_text(payload.get("system"), payload.get("messages"), payload.get("tools"))
        call = LoggedCall(identity, "/v1/messages", model, "Messages", request_text=request_preview)
        await filter_or_log(call, request_preview)
        return await call.run(anthropic_v1_messages.handle, payload, sse="anthropic")

    return router
