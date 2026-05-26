from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event

import anyio.to_thread
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from api import accounts, ai, image_tasks, register, system
from api.errors import install_exception_handlers
from api.support import resolve_web_asset, start_limited_account_watcher
from services.account_service import account_service
from services.backup_service import backup_service
from services.config import config
from services.image_service import start_image_cleanup_scheduler
from services.image_task_service import image_task_service
from utils.log import logger

IMAGE_WORKER_HARD_CAP = 32
IMAGE_WORKER_HARD_MIN = 4
STARLETTE_POOL_DEFAULT = 100
STARLETTE_POOL_MIN = 20
SHUTDOWN_INFLIGHT_GRACE_SECONDS = 5.0


def _read_memory_limit_file(path: str) -> int | None:
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except Exception:
        return None
    if not value or value == "max":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _detect_memory_budget_mb() -> int:
    value = _read_memory_limit_file("/sys/fs/cgroup/memory.max")
    if value is None:
        value = _read_memory_limit_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if value is not None:
        return max(256, value // (1024 * 1024))

    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return max(256, int(line.split()[1]) // 1024)
    except Exception:
        pass
    return 1024


def compute_max_image_workers() -> int:
    explicit = config.max_image_workers
    if explicit is not None:
        return max(1, min(int(explicit), IMAGE_WORKER_HARD_CAP))

    account_count = max(1, len(account_service.list_tokens()))
    per_account = config.image_account_concurrency
    by_accounts = account_count * per_account
    by_memory = max(IMAGE_WORKER_HARD_MIN, _detect_memory_budget_mb() // 100)
    final = max(IMAGE_WORKER_HARD_MIN, min(by_accounts, by_memory, IMAGE_WORKER_HARD_CAP))
    logger.info({
        "event": "image_workers_auto_config",
        "account_count": account_count,
        "per_account": per_account,
        "by_accounts": by_accounts,
        "by_memory": by_memory,
        "hard_cap": IMAGE_WORKER_HARD_CAP,
        "hard_min": IMAGE_WORKER_HARD_MIN,
        "final": final,
    })
    return final


def _determine_starlette_pool_size() -> int:
    explicit = config.starlette_pool_size
    if explicit is not None:
        return max(STARLETTE_POOL_MIN, int(explicit))
    return STARLETTE_POOL_DEFAULT


def create_app() -> FastAPI:
    app_version = config.app_version

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool_size = _determine_starlette_pool_size()
        anyio.to_thread.current_default_thread_limiter().total_tokens = pool_size
        app.state.starlette_pool_size_effective = pool_size
        max_image_workers = compute_max_image_workers()
        image_task_service.configure_max_workers(max_image_workers)
        config.set_runtime_effective(
            max_image_workers=max_image_workers,
            starlette_pool_size=pool_size,
        )
        logger.info({
            "event": "starlette_pool_configured",
            "size": pool_size,
            "source": "manual" if config.starlette_pool_size is not None else "auto",
        })
        stop_event = Event()
        thread = start_limited_account_watcher(stop_event)
        cleanup_thread = start_image_cleanup_scheduler(stop_event)
        backup_service.start()
        config.cleanup_old_images()
        try:
            yield
        finally:
            stop_event.set()
            thread.join(timeout=1)
            cleanup_thread.join(timeout=1)
            backup_service.stop()
            image_task_service.wait_for_inflight(SHUTDOWN_INFLIGHT_GRACE_SECONDS)

    app = FastAPI(title="chatgpt2api", version=app_version, lifespan=lifespan)
    install_exception_handlers(app)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(ai.create_router())
    app.include_router(accounts.create_router())
    app.include_router(image_tasks.create_router())
    app.include_router(register.create_router())
    app.include_router(system.create_router(app_version))

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_web(full_path: str):
        asset = resolve_web_asset(full_path)
        if asset is not None:
            return FileResponse(asset)
        if full_path.strip("/").startswith("_next/"):
            raise HTTPException(status_code=404, detail="Not Found")
        fallback = resolve_web_asset("")
        if fallback is None:
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(fallback)

    return app
