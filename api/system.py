from __future__ import annotations

from urllib.parse import quote

import anyio.to_thread
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict

from api.support import require_admin, require_identity, resolve_image_base_url
from services.account_service import account_service
from services.backup_service import BackupError, backup_service
from services.config import config
from services.image_service import delete_images, download_images_zip, get_image_download_response, get_image_response, get_thumbnail_response, list_images
from services.image_storage_service import ImageStorageError, image_storage_service
from services.image_tags_service import delete_tag, get_all_tags, set_tags
from services.image_task_service import image_task_service
from services.log_service import log_service
from services.proxy_service import test_proxy


class SettingsUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")


class ProxyTestRequest(BaseModel):
    url: str = ""


class ImageDeleteRequest(BaseModel):
    paths: list[str] = []
    start_date: str = ""
    end_date: str = ""
    all_matching: bool = False

class ImageDownloadRequest(BaseModel):
    paths: list[str]

class ImageTagsRequest(BaseModel):
    path: str
    tags: list[str]

class LogDeleteRequest(BaseModel):
    ids: list[str] = []
class BackupDeleteRequest(BaseModel):
    key: str = ""


def create_router(app_version: str) -> APIRouter:
    router = APIRouter()

    @router.post("/auth/login")
    async def login(authorization: str | None = Header(default=None)):
        identity = require_identity(authorization)
        return {
            "ok": True,
            "version": app_version,
            "role": identity.get("role"),
            "subject_id": identity.get("id"),
            "name": identity.get("name"),
        }

    @router.get("/version")
    async def get_version():
        return {"version": app_version}

    @router.get("/api/settings")
    async def get_settings(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"config": config.get()}

    @router.get("/api/system/info")
    async def get_system_info(request: Request, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        starlette_pool_effective = int(getattr(request.app.state, "starlette_pool_size_effective", 0) or 0)

        def snapshot():
            return {
                "max_workers_effective": image_task_service.max_workers_effective,
                "current_inflight": image_task_service.current_inflight,
                "rejection_count_24h": image_task_service.rejection_count_24h,
                "total_tokens": len(account_service.list_tokens()),
            }

        snap = await run_in_threadpool(snapshot)
        return {
            "image_workers": {
                "configured": config.max_image_workers,
                "effective": snap["max_workers_effective"],
                "current_inflight": snap["current_inflight"],
                "queue_full_rejections_24h": snap["rejection_count_24h"],
            },
            "starlette_pool": {
                "configured": config.starlette_pool_size,
                "effective": starlette_pool_effective,
                "current_total_tokens": anyio.to_thread.current_default_thread_limiter().total_tokens,
            },
            "accounts": {
                "total": snap["total_tokens"],
                "per_account_concurrency": config.image_account_concurrency,
            },
        }

    @router.post("/api/settings")
    async def save_settings(body: SettingsUpdateRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            return {"config": config.update(body.model_dump(mode="python"))}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.get("/api/images")
    async def get_images(request: Request, start_date: str = "", end_date: str = "", authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return list_images(resolve_image_base_url(request), start_date=start_date.strip(), end_date=end_date.strip())

    @router.get("/images/{image_path:path}", include_in_schema=False)
    async def get_image(image_path: str):
        return get_image_response(image_path)

    @router.get("/image-thumbnails/{image_path:path}", include_in_schema=False)
    async def get_image_thumbnail(image_path: str):
        return get_thumbnail_response(image_path)

    @router.post("/api/images/delete")
    async def delete_images_endpoint(body: ImageDeleteRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return delete_images(body.paths, start_date=body.start_date.strip(), end_date=body.end_date.strip(), all_matching=body.all_matching)

    @router.post("/api/images/download")
    async def download_images_endpoint(body: ImageDownloadRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        buf = download_images_zip(body.paths)
        return StreamingResponse(
            buf,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="images.zip"'},
        )

    @router.get("/api/images/download/{image_path:path}")
    async def download_single_image_endpoint(image_path: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return get_image_download_response(image_path)

    @router.get("/api/logs")
    async def get_logs(type: str = "", start_date: str = "", end_date: str = "", authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"items": log_service.list(type=type.strip(), start_date=start_date.strip(), end_date=end_date.strip())}

    @router.post("/api/logs/delete")
    async def delete_logs(body: LogDeleteRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return log_service.delete(body.ids)

    @router.post("/api/proxy/test")
    async def test_proxy_endpoint(body: ProxyTestRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        candidate = (body.url or "").strip() or config.get_proxy_settings()
        if not candidate:
            raise HTTPException(status_code=400, detail={"error": "proxy url is required"})
        return {"result": await run_in_threadpool(test_proxy, candidate)}

    @router.get("/api/storage/info")
    async def get_storage_info(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        storage = config.get_storage_backend()
        return {
            "backend": storage.get_backend_info(),
            "health": storage.health_check(),
        }

    @router.post("/api/backup/test")
    async def test_backup_connection(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            return {"result": await run_in_threadpool(backup_service.test_connection)}
        except BackupError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.post("/api/image-storage/test")
    async def test_image_storage_endpoint(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"result": await run_in_threadpool(image_storage_service.test_webdav)}

    @router.post("/api/image-storage/sync")
    async def sync_image_storage_endpoint(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            return {"result": await run_in_threadpool(image_storage_service.sync_all)}
        except ImageStorageError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.get("/api/backups")
    async def get_backups(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            return {
                "items": await run_in_threadpool(backup_service.list_backups),
                "state": backup_service.get_status(),
                "settings": backup_service.get_settings(),
            }
        except BackupError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.post("/api/backups/run")
    async def run_backup_endpoint(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            return {"result": await run_in_threadpool(backup_service.run_backup)}
        except BackupError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.post("/api/backups/delete")
    async def delete_backup_endpoint(body: BackupDeleteRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            await run_in_threadpool(backup_service.delete_backup, body.key)
            return {"ok": True}
        except BackupError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.get("/api/backups/detail")
    async def get_backup_detail(key: str = "", authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            return {"item": await run_in_threadpool(backup_service.get_backup_detail, key)}
        except BackupError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.get("/api/backups/download")
    async def download_backup_endpoint(key: str = "", authorization: str | None = Header(default=None)):
        require_admin(authorization)
        try:
            item = await run_in_threadpool(backup_service.download_backup, key)
        except BackupError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
        filename = str(item.get("name") or "backup.bin")
        quoted = quote(filename)
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{quoted}",
            "Content-Length": str(int(item.get("size") or 0)),
        }
        return Response(
            content=bytes(item.get("payload") or b""),
            media_type=str(item.get("content_type") or "application/octet-stream"),
            headers=headers,
        )


    @router.get("/api/images/tags")
    async def list_image_tags(authorization: str | None = Header(default=None)):
        require_admin(authorization)
        return {"tags": get_all_tags()}

    @router.post("/api/images/tags")
    async def update_image_tags(body: ImageTagsRequest, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        rel = body.path.strip().lstrip("/")
        if not rel:
            raise HTTPException(status_code=400, detail={"error": "path is required"})
        tags = set_tags(rel, body.tags)
        return {"ok": True, "tags": tags}

    @router.delete("/api/images/tags/{tag}")
    async def delete_image_tag(tag: str, authorization: str | None = Header(default=None)):
        require_admin(authorization)
        count = delete_tag(tag)
        return {"ok": True, "removed_from": count}

    return router
