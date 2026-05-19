# OpenAI 兼容图片接口走任务队列改造

**日期**: 2026-05-19
**状态**: Draft (Reviewed v2)
**作者**: xbang

**修订记录**:
- v1 (2026-05-19): 初版
- v2 (2026-05-19): 16 个 review 问题修复 —— 修正 `_run_oai_task` 完整实现、streaming bridge 死锁、初始化时序、错误处理路径、命名一致性、配置生效说明等

---

## 1. 背景

### 1.1 现状

`chatgpt2api` 基于 FastAPI + Uvicorn 构建。所有 HTTP 路由都是 `async def`，但绝大多数路由（包括 `/v1/chat/completions`、`/v1/images/*`、`/api/logs` 等）通过 `await run_in_threadpool(...)` 调用同步业务逻辑。

`run_in_threadpool` 使用 Starlette/AnyIO 提供的默认线程池，容量为 **40**（AnyIO 库的默认值）。**所有路由共享这一个池子**。

### 1.2 问题

`/v1/images/generations` 和 `/v1/images/edits` 在内部调用 `_poll_image_results`（`services/openai_backend_api.py:646`），后者使用同步 `time.sleep` + 同步 `requests` 反复轮询 ChatGPT 上游，单次请求会占用一个线程**长达 60-180 秒**。

当客户端并发图请求 >= 池子余量时：

1. 40 个工作线程全部进入 `_poll_image_results`，被 `time.sleep` 钉死
2. 新到达的任何 HTTP 请求（包括短任务如 `/version`、`/api/logs`）无法被分派到线程
3. 整个服务对外表现为"挂死"，必须重启 docker 容器才能恢复
4. 客户端超时后重试，进一步放大占用，加速死锁

### 1.3 已有的正确模式

代码库中 `services/image_task_service.py` 已实现"提交-后台执行-轮询"的异步任务模式：

- `submit_generation` / `submit_edit` 立刻返回任务 ID
- 业务逻辑在独立 `threading.Thread(daemon=True)` 中执行
- 客户端通过 `GET /api/image-tasks?ids=...` 轮询结果
- 任务状态持久化到 `data/image_tasks.json`

Web UI（`web/src/lib/api.ts:404,437`）已使用此模式。

**问题在于：兼容 OpenAI 协议的 `/v1/images/*` 仍走旧的同步路径。**

---

## 2. 目标

### 2.1 必须达成

- `/v1/images/generations` 和 `/v1/images/edits` 不再占用 Starlette 主线程池
- 客户端 API 契约不变（请求格式、响应格式、状态码、错误结构）
- 图请求积压时返回 503（带 `Retry-After`），不阻塞其他路由
- 跑腿小哥（图任务工作线程）数量上限可配置 + 智能默认
- 不同部署规模（1 个账号 / 50+ 账号）均可零配置开箱可用

### 2.2 不在本期范围

- 不重写 `curl-cffi` 为异步（保留同步 HTTP）
- 不重构 `/v1/chat/completions` 的执行路径
- 不引入新的外部依赖（Redis、Celery 等）
- 不改变图片存储、账号管理、日志系统的现有行为

---

## 3. 总体架构

### 3.1 双池模型

```
┌─────────────────────────────────────────────────────┐
│ 大堂: Starlette 默认线程池 (默认 100, 可配)          │
│  - 处理短任务: chat / version / logs / settings 等   │
│  - 接图任务请求 + 转任务队列 + 等结果 (await) + 返回 │
│  - 任何路由都不被图请求阻塞                          │
└─────────────────────────────────────────────────────┘
                 │ submit_and_wait_async
                 ▼
┌─────────────────────────────────────────────────────┐
│ 任务服务: ImageTaskService (扩展现有实现)            │
│  - submit_and_wait_async (新增, 给 OpenAI 协议用)    │
│  - submit_generation / submit_edit (现有, 给 web 用) │
│  - Semaphore(max_image_workers) 限制并发上限         │
│  - 满了立即抛 ImageQueueFullError → 路由层 503       │
└─────────────────────────────────────────────────────┘
                 │ thread.start (per task)
                 ▼
┌─────────────────────────────────────────────────────┐
│ 后厨: 图任务工作线程 (按需创建, 完成销毁)             │
│  - 数量上限 = min(账号数 × image_account_concurrency,│
│                内存预算, 32)                         │
│  - 执行 _poll_image_results (内部 time.sleep)        │
│  - 完成时回写任务表 + 通知所有等待者                  │
└─────────────────────────────────────────────────────┘
                 │ requests + curl_cffi
                 ▼
            [ChatGPT 上游]
```

### 3.2 关键性质

| 性质 | 实现机制 |
|------|---------|
| 大堂不被阻塞 | 路由用 `await event.wait()`，事件循环挂起协程不占线程 |
| 后厨容量可控 | `threading.Semaphore(max_image_workers)` |
| 满载快速失败 | `semaphore.acquire(blocking=False)` + 抛异常 → 路由层 503 |
| 跨线程唤醒 | `loop.call_soon_threadsafe(event.set)` |
| 按需创建小哥 | 沿用现有 `threading.Thread(daemon=True).start()` 模式 |
| 用完销毁 | daemon 线程，函数返回后自动回收 |

### 3.3 客户端视角

```
之前:                                之后:
POST /v1/images/generations          POST /v1/images/generations
  ↓                                    ↓
(等 60-180s, 服务可能死锁)            (等 60-180s, 服务永远响应)
  ↓                                    ↓
200 { data: [...] } 或超时            200 { data: [...] }
                                     或 503 (Retry-After: 5) 若过载
```

OpenAI SDK 默认会处理 503 + Retry-After，**客户端代码无需改动**。

---

## 4. 详细设计

### 4.1 ImageTaskService 扩展

新增方法 `submit_and_wait_async`：在已有的 daemon-thread worker 模式基础上桥接 asyncio。

#### 4.1.1 新增异常和数据结构

```python
class ImageQueueFullError(Exception):
    """跑腿小哥池满，应立即返回 503"""
    def __init__(self, retry_after: int = 5):
        self.retry_after = retry_after


# OAI 协议路径用的 task_dict 结构（与 Web UI 任务表 schema 解耦）
# {
#   "status": "success" | "error",
#   "data": [...],         # 成功时存在；元素格式由 handler 决定 (url 或 b64_json)
#   "error": "...",        # 失败时存在
#   "created": <int>,      # handler 返回的 unix 时间戳
# }
```

#### 4.1.2 新增字段（在 `__init__` 中）

```python
class ImageTaskService:
    def __init__(self, path, *, ...):
        # 既有初始化 ...
        self._semaphore: threading.Semaphore | None = None  # lifespan 启动时设置
        self._inflight_count = 0                            # 当前在跑的 OAI 任务数
        self._rejection_timestamps: list[float] = []        # 503 拒绝的时间戳列表
        self._oai_callbacks: dict[str, list[Callable[[dict], None]]] = {}

    def configure_max_workers(self, max_workers: int) -> None:
        """由 lifespan startup 调用，设定 semaphore 容量。
        注：threading.Semaphore 不支持运行时 resize，运行中改 config 需重启服务。
        """
        with self._lock:
            self._semaphore = threading.Semaphore(max(1, int(max_workers)))
            self._max_workers_effective = int(max_workers)
```

**重要**：semaphore 不在 `__init__` 创建，而是延迟到 lifespan startup（见 4.2 节）。原因：实例化 ImageTaskService 时，`account_service` 可能还没初始化，无法正确计算 max_workers。

#### 4.1.3 `submit_and_wait_async` 完整实现

```python
async def submit_and_wait_async(
    self,
    identity: dict,
    *,
    mode: str,
    payload: dict,
    timeout: float,
) -> dict[str, Any]:
    """
    OpenAI 兼容协议路径：提交任务并 async 等结果。
    - 池满时立刻抛 ImageQueueFullError
    - 完成时返回 task_dict
    - 客户端超时时不取消后台任务（worker 跑完会自动释放 semaphore）
    """
    assert self._semaphore is not None, "configure_max_workers() 必须先调用"

    if not self._semaphore.acquire(blocking=False):
        with self._lock:
            self._rejection_timestamps.append(time.time())
            self._trim_old_rejections_locked()
        raise ImageQueueFullError(retry_after=5)

    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    result_container: dict[str, Any] = {}

    task_id = f"oai-{uuid.uuid4().hex[:16]}"

    def on_complete(task: dict):
        result_container["task"] = task
        loop.call_soon_threadsafe(event.set)

    with self._lock:
        self._oai_callbacks.setdefault(task_id, []).append(on_complete)
        self._inflight_count += 1

    thread = threading.Thread(
        target=self._run_oai_task,
        args=(task_id, identity, mode, payload),
        daemon=True,
        name=f"image-oai-{task_id[:12]}",
    )
    thread.start()

    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        raise ImagePollTimeoutError(
            f"等待图片生成超时（{timeout}s），后台任务仍在执行"
        )

    return result_container["task"]


def _run_oai_task(self, task_id, identity, mode, payload):
    """OAI 协议路径的 worker 线程入口。完全独立于 Web UI 的 _run_task。"""
    started = time.time()
    model = str(payload.get("model") or "gpt-image-2")
    task_dict: dict[str, Any]

    try:
        handler = self.edit_handler if mode == "edit" else self.generation_handler
        result = handler(payload)

        # OAI 协议路径不应该走流式（流式走另一条路径，见 4.4）
        if not isinstance(result, dict):
            raise RuntimeError("internal error: handler returned a streaming iterator")

        data = result.get("data")
        message = _clean(result.get("message"))
        if not isinstance(data, list) or not data:
            # 复刻既有 _run_task 的错误消息选择
            if message:
                raise RuntimeError(message)
            raise RuntimeError(
                "号池中没有可用账号或所有账号均被限流，"
                "请检查号池状态（账号额度、是否被封禁、是否到达生图上限）"
            )

        task_dict = {
            "status": "success",
            "data": data,
            "created": int(result.get("created") or time.time()),
        }
    except ImagePollTimeoutError as exc:
        # 上游轮询超时 → 504
        task_dict = self._make_error_task_dict(str(exc), status_code=504)
    except ImageGenerationError as exc:
        # 既有异常携带 status_code / error_type / code
        task_dict = self._make_error_task_dict(
            str(exc),
            status_code=getattr(exc, "status_code", 502),
            error_type=getattr(exc, "error_type", None),
            code=getattr(exc, "code", None),
        )
    except Exception as exc:
        task_dict = self._make_error_task_dict(
            str(exc) or "image task failed",
            status_code=502,
        )
    finally:
        # 必须释放 semaphore + inflight count，无论成功失败
        self._semaphore.release()
        with self._lock:
            self._inflight_count = max(0, self._inflight_count - 1)

    # 触发回调（在锁外执行，避免回调里再 acquire 锁导致死锁）
    with self._lock:
        callbacks = self._oai_callbacks.pop(task_id, [])
    for cb in callbacks:
        try:
            cb(task_dict)
        except Exception as exc:
            logger.warning({"event": "oai_callback_error", "error": str(exc)})


def _make_error_task_dict(
    self, error: str, *, status_code: int,
    error_type: str | None = None, code: str | None = None,
) -> dict:
    """构造错误 task_dict，保留 HTTP 状态和 OpenAI 错误结构字段。"""
    out: dict[str, Any] = {
        "status": "error",
        "error": error,
        "status_code": status_code,  # 给路由层决定返回什么 HTTP code
        "data": [],
        "created": int(time.time()),
    }
    if error_type:
        out["error_type"] = error_type
    if code:
        out["code"] = code
    return out


def _trim_old_rejections_locked(self) -> None:
    """24 小时之前的拒绝记录删掉，避免列表无限增长。调用前必须持有 self._lock。"""
    cutoff = time.time() - 86400
    self._rejection_timestamps = [t for t in self._rejection_timestamps if t >= cutoff]


# 给 /api/system/info 用的只读属性
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
    return getattr(self, "_max_workers_effective", 0)
```

#### 4.1.4 设计要点

1. **`acquire(blocking=False)`**：快速失败，不让请求在 acquire 上排队。
2. **`loop.call_soon_threadsafe`**：asyncio 跨线程唤醒的官方方式。`event.set` 是同步操作，但要调度到事件循环所在线程执行。
3. **`finally: semaphore.release()`**：保证池子永不泄露，即使 handler 抛任何异常。
4. **客户端超时不取消 worker**：worker 跑完会自动释放 semaphore；新请求最多等 worker 完成。
5. **OAI 路径和 Web UI 路径完全独立**：`_oai_callbacks` 和 `_tasks` 两个表互不影响，task_id 用 `oai-` 前缀避免冲突。
6. **回调在锁外执行**：避免回调中再尝试获取锁导致死锁。

### 4.2 容量自动计算

#### 4.2.1 调用时机

`max_image_workers` **只在 lifespan startup 时计算一次**，由 `api/app.py` 调用 `image_task_service.configure_max_workers(...)` 传入。

**为什么不在 `__init__` 中计算？**

`image_task_service` 在模块导入时被实例化（`services/image_task_service.py` 末尾的 `image_task_service = ImageTaskService(...)`）。此时 `account_service` 可能还未导入或账号尚未从存储加载，`list_tokens()` 会返回空列表，导致 max_workers 被低估到 HARD_MIN。

将计算延迟到 lifespan startup 保证：
- `account_service` 已完成初始化和数据加载
- 配置（含可能的手动覆盖）已就绪

**运行时账号变化的处理**：本期 max_workers 启动时计算一次，**用户加减账号后需要重启服务**才能让 max_workers 重新计算。这是有意为之的简化（避免 Semaphore 动态 resize 的复杂性）。

#### 4.2.2 计算函数

放在 `api/app.py`（或一个新的 `api/support.py` 辅助模块）：

```python
def compute_max_image_workers() -> int:
    """根据配置 + 账号数 + 内存预算计算 max_image_workers。"""
    explicit = config.max_image_workers  # None 表示自动
    if explicit is not None:
        return max(1, int(explicit))

    account_count = max(1, len(account_service.list_tokens()))
    per_account = config.image_account_concurrency
    by_accounts = account_count * per_account

    by_memory = max(4, _detect_memory_budget_mb() // 100)

    HARD_CAP = 32
    HARD_MIN = 4

    final = max(HARD_MIN, min(by_accounts, by_memory, HARD_CAP))
    logger.info({
        "event": "image_workers_auto_config",
        "account_count": account_count,
        "per_account": per_account,
        "by_accounts": by_accounts,
        "by_memory": by_memory,
        "hard_cap": HARD_CAP,
        "hard_min": HARD_MIN,
        "final": final,
    })
    return final


def _detect_memory_budget_mb() -> int:
    """读取容器 cgroup 限制；失败则返回 1024（保守值）"""
    try:
        # cgroup v2
        with open("/sys/fs/cgroup/memory.max") as f:
            value = f.read().strip()
            if value != "max":
                return max(256, int(value) // (1024 * 1024))
    except FileNotFoundError:
        pass

    try:
        # cgroup v1
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            return max(256, int(f.read().strip()) // (1024 * 1024))
    except FileNotFoundError:
        pass

    try:
        # 兜底：/proc/meminfo
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    kb = int(line.split()[1])
                    return max(256, kb // 1024)
    except Exception:
        pass

    return 1024  # 1GB 保守默认
```

### 4.3 路由改造

`api/ai.py` 中 `/v1/images/generations` 改造：

```python
@router.post("/v1/images/generations")
async def generate_images(
        body: ImageGenerationRequest,
        request: Request,
        authorization: str | None = Header(default=None),
):
    identity = require_identity(authorization)
    payload = body.model_dump(mode="python")
    payload["base_url"] = resolve_image_base_url(request)

    call = LoggedCall(identity, "/v1/images/generations", body.model,
                      "文生图", request_text=body.prompt)
    await filter_or_log(call, body.prompt)

    # 流式请求走独立池 (4.4 节)
    if body.stream:
        return await _handle_stream_image_via_isolated_executor(
            openai_v1_image_generations.handle, payload, call
        )

    try:
        task = await image_task_service.submit_and_wait_async(
            identity,
            mode="generate",
            payload=payload,
            timeout=config.image_poll_timeout_secs + 30,  # +30s buffer for download+log
        )
    except ImageQueueFullError as exc:
        call.log("调用失败", status="failed", error="image queue full")
        raise HTTPException(
            status_code=503,
            detail={"error": "image worker pool is full, retry later"},
            headers={"Retry-After": str(exc.retry_after)},
        )
    except ImagePollTimeoutError as exc:
        # async 等待超时（worker 仍在跑）
        call.log("调用失败", status="failed", error=str(exc))
        raise HTTPException(
            status_code=504,
            detail={"error": str(exc)},
        )

    if task.get("status") == "error":
        # worker 内部失败，status_code 由 worker 决定（504 上游超时 / 400 内容策略 / 502 其他）
        call.log("调用失败", status="failed", error=task.get("error") or "")
        detail: dict[str, Any] = {"error": task.get("error") or "image generation failed"}
        if task.get("error_type"):
            detail["type"] = task["error_type"]
        if task.get("code"):
            detail["code"] = task["code"]
        raise HTTPException(
            status_code=task.get("status_code") or 502,
            detail=detail,
        )

    call.log("调用完成")
    return _task_to_openai_image_response(task)
```

`/v1/images/edits` 同构改造。**注意**：edits 路由的 stream 判断从 payload dict 取（而非 pydantic 字段）：

```python
@router.post("/v1/images/edits")
async def edit_images(request, authorization):
    identity = require_identity(authorization)
    payload, image_sources = await parse_image_edit_request(request)
    # ... 既有 prompt / model / filter / images 处理 ...

    if payload.get("stream"):
        return await _handle_stream_image_via_isolated_executor(
            openai_v1_image_edit.handle, payload, call
        )

    try:
        task = await image_task_service.submit_and_wait_async(
            identity, mode="edit", payload=payload,
            timeout=config.image_poll_timeout_secs + 30,
        )
    except ImageQueueFullError as exc:
        # 同上
    except ImagePollTimeoutError as exc:
        # 同上
    # ... 错误转 502 + 转 OAI 响应
```

### 4.4 流式响应处理

`stream=True` 时 handler 返回 `Iterator[ImageOutput]`，需要持续推 chunk 给客户端。

**策略**：流式请求使用一个**独立的、小容量的 ThreadPoolExecutor**（默认 4），与主池隔离。流式图请求实际使用极少，给它独立小池就够。

#### 4.4.1 Executor 生命周期

在 `api/app.py` lifespan 中创建并 shutdown：

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ... 既有 startup 逻辑 ...
    app.state.image_stream_executor = ThreadPoolExecutor(
        max_workers=4,
        thread_name_prefix="image-stream",
    )
    try:
        yield
    finally:
        app.state.image_stream_executor.shutdown(wait=False)
        # ... 既有 shutdown 逻辑 ...
```

#### 4.4.2 桥接代码（**无 `.result()` 阻塞**）

```python
async def _handle_stream_image_via_isolated_executor(handler, payload, call):
    """流式图请求专用通道，不污染主池。
    桥接策略：worker 线程 → call_soon_threadsafe 推到 asyncio.Queue → 异步消费。
    """
    loop = asyncio.get_running_loop()
    executor = _get_app_state().image_stream_executor

    # 无界 queue：图请求的 chunk 数量很少（一般 < 20 个）
    queue: asyncio.Queue = asyncio.Queue()
    DONE = object()

    def producer():
        """在 executor 的线程里跑同步 generator。"""
        try:
            for chunk in handler(payload):
                # 用 call_soon_threadsafe 把 put_nowait 调度到 loop 上执行
                # 不阻塞 producer 线程，避免客户端断开时死锁
                loop.call_soon_threadsafe(queue.put_nowait, chunk)
        except Exception as exc:
            # 生成 OpenAI SSE 错误事件格式（不是裸 dict）
            error_chunk = {
                "error": {
                    "message": str(exc),
                    "type": "image_generation_error",
                }
            }
            loop.call_soon_threadsafe(queue.put_nowait, error_chunk)
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, DONE)

    executor.submit(producer)

    async def consumer():
        try:
            while True:
                chunk = await queue.get()
                if chunk is DONE:
                    return
                yield chunk
        finally:
            # 客户端断开时，producer 会继续填 queue（无害，loop 会 GC）
            # 不需要主动取消 producer，因为它一定会跑到 DONE
            pass

    return StreamingResponse(
        stream_image_chunks(consumer()),  # 复用既有 SSE 序列化
        media_type="text/event-stream",
    )
```

#### 4.4.3 设计要点

1. **不用 `.result()`**：producer 用 `call_soon_threadsafe(put_nowait, ...)` 单向推，永不阻塞 producer 线程。即使客户端断开，producer 也能跑完。
2. **无界 queue**：图请求的 chunk 数量本来就少（progress + result），无界队列不会占太多内存。
3. **错误也是有效 chunk**：错误用 OpenAI SSE 错误事件格式包装，客户端能正常解析。
4. **不取消 producer**：客户端断开后让 producer 自然跑完（占一个 executor 槽 60-180s），最多 4 个流式请求并发占用。极端场景下流式 executor 可能短期排队，但这是设计意图——故意把流式做成"二等公民"，主路径走非流式。

### 4.5 大堂线程池调整 + max_workers 启动初始化

`api/app.py` 的 lifespan 启动时同时配置两件事：Starlette 池容量 + ImageTaskService 的 max_workers。

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 配置大堂池
    pool_size = _determine_starlette_pool_size()
    anyio.to_thread.current_default_thread_limiter().total_tokens = pool_size
    app.state.starlette_pool_size_effective = pool_size
    logger.info({
        "event": "starlette_pool_configured",
        "size": pool_size,
        "source": "manual" if config.starlette_pool_size is not None else "auto",
    })

    # 2. 配置图任务 worker 上限（此时 account_service 已就绪）
    max_workers = compute_max_image_workers()
    image_task_service.configure_max_workers(max_workers)

    # 3. 流式 executor
    app.state.image_stream_executor = ThreadPoolExecutor(
        max_workers=4, thread_name_prefix="image-stream",
    )

    # ... 既有 startup（backup_service.start 等）...
    try:
        yield
    finally:
        app.state.image_stream_executor.shutdown(wait=False)
        # ... 既有 shutdown ...


def _determine_starlette_pool_size() -> int:
    explicit = config.starlette_pool_size
    if explicit is not None:
        return max(20, int(explicit))
    return 100  # 自动默认
```

**Uvicorn 多 worker 模式提示**：
如果用户用 `uvicorn --workers N` 启动多进程，每个进程独立的事件循环和 Semaphore，**实际系统并发上限 = N × max_image_workers**。文档需要说明。本项目 Dockerfile 默认单 worker，常规部署无需担心。

### 4.6 任务到响应的转换

```python
def _task_to_openai_image_response(task: dict) -> dict:
    """把内部 task_dict 转成 OpenAI /v1/images/generations 响应格式。
    错误情况由调用方在路由里处理，本函数只负责成功路径的格式转换。
    """
    data = task.get("data") or []
    if not data:
        # 理论上不该到这里：上游已校验并写入 error。但兜底防御。
        raise HTTPException(
            status_code=502,
            detail={"error": "image generation returned no data"},
        )

    return {
        "created": int(task.get("created") or time.time()),  # 优先用 handler 给的时间
        "data": data,
    }
```

**调用方**（在 4.3 节路由代码中）已经处理了 `task["status"] == "error"` 的情况，会先把它转 502。所以本函数只处理成功路径。`data` 元素的格式（`{"url": ...}` 或 `{"b64_json": ...}`）由 handler 内部根据 `response_format` 决定，本函数不动。

---

## 5. 配置项

### 5.1 新增字段（`config.json`）

```json
{
  "max_image_workers": null,
  "starlette_pool_size": null
}
```

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `max_image_workers` | int 或 null | null | 跑腿小哥池上限。null 表示自动 |
| `starlette_pool_size` | int 或 null | null | 大堂线程池上限。null 表示自动 (100) |

### 5.2 与既有字段的关系

- `image_account_concurrency`（既有，默认 3）：单账号并发上限。**自动计算 `max_image_workers` 时会乘上此值。**
- `image_poll_timeout_secs`（既有，默认 120）：单次图任务的等待超时。**`submit_and_wait_async` 的 timeout = 此值 + 30s**（多 30s 给 worker 处理图片下载、base64、日志、callback 等收尾步骤；过紧会导致 worker 实际成功但 async 调用方已超时的尴尬情况）。

### 5.3 运行时变更

- `max_image_workers` 和 `starlette_pool_size` 的变更**需要重启服务才能生效**。原因：`threading.Semaphore` 和 AnyIO `CapacityLimiter` 都不支持运行时安全地 resize 已等待的协程。
- Web UI 的设置页改动配置后，向用户明确提示"重启容器后生效"。
- `image_account_concurrency` 的变更也需要重启（因为它影响 max_workers 自动计算）。
- `image_poll_timeout_secs` 的变更立刻生效（每个新请求读最新值）。

### 5.4 自动计算暴露

`services/config.py` 添加 property：

```python
@property
def max_image_workers(self) -> int | None:
    value = self.data.get("max_image_workers")
    if value is None or value == "":
        return None
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return None

@property
def starlette_pool_size(self) -> int | None:
    value = self.data.get("starlette_pool_size")
    if value is None or value == "":
        return None
    try:
        return max(20, int(value))
    except (TypeError, ValueError):
        return None
```

#### 5.4.1 `config.update` 校验

`services/config.py` 的 `update` 方法（既有）需要接受新字段：
- 接受 `None` / `""` / 正整数
- 其他类型 → 抛 `ValueError`（被 API 层转 400）

#### 5.4.2 `config.get()` 返回值扩展

`get()` 方法返回的 config 中暴露：

- `max_image_workers`: 用户设的值（可能是 null）
- `max_image_workers_effective`: 实际生效值（启动时从 `image_task_service.max_workers_effective` 读取）
- `starlette_pool_size` / `starlette_pool_size_effective` 同理（从 `app.state.starlette_pool_size_effective` 读取）

具体的 effective 值通过 lifespan 启动时保存到 `app.state` 或服务实例属性中，`config.get()` 调用辅助函数读取。

---

## 6. 错误处理

### 6.1 错误分类

| 场景 | HTTP 状态 | 来源 | Retry-After |
|------|---------|------|-------------|
| 跑腿小哥池满 | 503 | `submit_and_wait_async` 抛 `ImageQueueFullError` → 路由 | 5 秒 |
| async 等待超时（worker 仍跑） | 504 | `submit_and_wait_async` 抛 `ImagePollTimeoutError` → 路由 | - |
| worker 内部上游轮询超时 | 504 | worker 捕获 `ImagePollTimeoutError`，task_dict.status_code=504 | - |
| worker 上游 ChatGPT 失败 | 502 | worker 捕获 `ImageGenerationError` 默认 502 | - |
| 内容过滤未通过 | 400 | `ImageGenerationError(status_code=400, code="content_policy_violation")` | - |
| 未授权 | 401 | `require_identity` | - |

**关键**：worker 通过 task_dict 中的 `status_code` / `error_type` / `code` 字段把上游错误的 HTTP 语义传给路由，路由按 task_dict 决定返回什么状态码。

### 6.2 既有错误兼容

`ImageGenerationError`、`ImagePollTimeoutError`、`UpstreamHTTPError` 等既有异常继续在 worker 内部抛出，由 `_run_oai_task` 的 `except Exception` 块捕获并写入 task_dict 的 `error` 字段。路由层再将 task 转换为对应 HTTP 状态。

**注意**：`submit_and_wait_async` 自己抛的 `ImagePollTimeoutError`（async 等待超时）和 worker 内部抛的 `ImagePollTimeoutError`（上游轮询超时）都最终落到路由的 `except ImagePollTimeoutError`，统一返回 504。

### 6.3 异常安全

- Worker 内任何异常：semaphore 一定释放（`finally`）
- 回调函数挂掉：被 `try/except pass` 隔离，不影响其他回调
- 客户端断开连接：worker 继续跑，结果丢弃（无副作用）

---

## 7. 可观察性

### 7.1 新增 `/api/system/info` 端点

```python
@router.get("/api/system/info")
async def get_system_info(authorization: str | None = Header(default=None)):
    require_admin(authorization)
    return {
        "image_workers": {
            "configured": config.max_image_workers,  # null 或数字 (用户设的)
            "effective": image_task_service.max_workers_effective,  # 实际生效
            "current_inflight": image_task_service.current_inflight,  # 见 4.1.3
            "queue_full_rejections_24h": image_task_service.rejection_count_24h,
        },
        "starlette_pool": {
            "configured": config.starlette_pool_size,
            "effective": _get_starlette_pool_effective(),  # 从 app.state 读
            "current_total_tokens": anyio.to_thread
                .current_default_thread_limiter().total_tokens,
        },
        "accounts": {
            "total": len(account_service.list_tokens()),
            "per_account_concurrency": config.image_account_concurrency,
        },
    }
```

**实现细节**：`current_inflight` 和 `rejection_count_24h` 的具体维护逻辑见 4.1 节（在 `submit_and_wait_async` 和 `_run_oai_task` 中增减）。`max_workers_effective` 由 `configure_max_workers` 设置后只读。

### 7.2 关键日志事件

| 事件名 | 字段 | 触发时机 |
|--------|------|---------|
| `starlette_pool_configured` | size, source(auto/manual) | 启动时 |
| `image_workers_auto_config` | account_count, by_accounts, by_memory, final | 启动时 |
| `image_queue_full_reject` | request_id, current_inflight | 拒绝 503 时 |
| `image_task_submitted` | task_id, mode | 提交时 |
| `image_task_completed` | task_id, duration_ms, status | 完成时 |

### 7.3 web UI 系统设置页

在 `web/src/app/settings/` 下新增 `performance-settings-card.tsx`，复用现有 settings 卡片的视觉风格：

```
性能 / Performance
├─ 跑腿小哥池 (max_image_workers)
│   当前: 自动 (20)  [手动设置 ▼]
│   提示: 自动 = 账号数 × image_account_concurrency
│
├─ 大堂线程池 (starlette_pool_size)
│   当前: 自动 (100)  [手动设置 ▼]
│
└─ 实时状态
    跑腿小哥使用: 8 / 20
    24h 内 503 拒绝数: 3
    总账号数: 10
```

---

## 8. 测试策略

### 8.1 单元测试（`test/test_image_task_service.py` 扩展）

- `configure_max_workers(N)` 后，semaphore 容量 = N
- `submit_and_wait_async` 正常完成 → 返回 `status="success"` 的 task_dict，含 `data` 和 `created`
- `submit_and_wait_async` 池满 → 抛 `ImageQueueFullError`，且 `rejection_count_24h` 自增
- `submit_and_wait_async` worker 异常 → 返回 `status="error"` 的 task_dict（不外抛），semaphore 已释放
- `submit_and_wait_async` async 等待超时 → 抛 `ImagePollTimeoutError`，但 worker 继续运行且最终释放 semaphore
- 多个并发 `submit_and_wait_async` 共享 semaphore：N+1 个并发时第 N+1 个收到 `ImageQueueFullError`
- `current_inflight` 在任务运行中=1，完成后=0
- `compute_max_image_workers` mock 账号数/内存后能产出预期值（覆盖附录 B 表中的几行）

### 8.2 集成测试

- `POST /v1/images/generations` 正常 → 200 + 图片
- 同时提交 (max + 1) 个图请求 → 最后一个收到 503 + `Retry-After` 头
- 池满状态下 `GET /version` 和 `/v1/chat/completions` 仍正常响应（**核心回归保护**）
- `POST /v1/images/generations` with `stream=True` → 走 stream executor，返回 SSE 流
- 流式 handler 抛异常 → 客户端收到合法 SSE 错误 chunk
- `GET /api/system/info` 返回正确结构和当前值

### 8.3 负载验证（手动）

```bash
# 终端 1: 并发打 100 个图请求
for i in $(seq 1 100); do
  curl -X POST localhost:3000/v1/images/generations \
       -H "Authorization: Bearer xxx" \
       -d '{"prompt":"test","model":"gpt-image-2"}' &
done

# 终端 2: 同时检查短接口是否仍响应
while true; do
  time curl -s localhost:3000/version
  sleep 1
done
```

**验收标准**：
- 终端 1 中前 N 个请求成功（N = max_image_workers），其余收到 503
- 终端 2 的每次 `/version` 响应 < 100ms（**关键**）

---

## 9. 改动文件清单

| 文件 | 改动 | 工作量 |
|------|------|--------|
| `services/image_task_service.py` | 新增 `submit_and_wait_async` / `_run_oai_task` / `configure_max_workers` / 三个只读 property / `ImageQueueFullError` | ~150 行 |
| `services/config.py` | 新增 `max_image_workers` / `starlette_pool_size` property + `update` 校验 + `get` 返回 effective | ~50 行 |
| `api/ai.py` | 改造 `/v1/images/generations` 和 `/v1/images/edits`；新增 `_handle_stream_image_via_isolated_executor` 和 `_task_to_openai_image_response` | ~100 行 |
| `api/app.py` | lifespan 中：Starlette pool 配置 + image worker max 计算 + stream executor 生命周期 + `compute_max_image_workers` 辅助 | ~60 行 |
| `api/system.py` | 新增 `/api/system/info` 端点 | ~30 行 |
| `web/src/app/settings/components/` (新增 performance-settings-card.tsx) | 新增"性能"卡片 + 状态显示 | ~100 行 |
| `test/test_image_task_service.py` | 扩展测试（见 8.1） | ~150 行 |
| `test/test_v1_images_generations.py` 等 | 集成测试（见 8.2） | ~100 行 |
| `README.md` | "性能调优"章节 | ~50 行 |

**预估总工作量**: ~800 行新增/修改。

---

## 10. 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| `asyncio.Event` 跨线程唤醒写错 | 路由永远 await 不返回 | 用 `loop.call_soon_threadsafe(event.set)`；单元测试覆盖（4.1.3 节伪代码） |
| Semaphore 泄漏（worker 异常未释放） | 池子越用越小 | `try/finally` 保证释放 + 单元测试模拟异常 |
| `image_task_service` 持久化文件并发写竞争 | json 文件损坏 | OAI 路径不写文件（task_dict 在内存中），仅 Web UI 路径写。既有 `_lock` 保护够用 |
| 流式 producer 线程阻塞 | 客户端永远等不到 chunk | 用 `call_soon_threadsafe(put_nowait)` 单向推（4.4.2），无 `.result()` 阻塞点 |
| 客户端断开时流式 producer 仍占 executor 槽 | 流式池短期排队 | executor 限 4 槽，最多 4 个"幽灵"流式请求；做"二等公民"是有意为之 |
| 用户手动设置过大 `max_image_workers` 致 OOM | 容器被 kill | log 警告 + README 文档强调；启动时配 HARD_CAP=32 兜底 |
| `image_task_service` 同时被 web UI 和 OpenAI 协议用 | 两种调用方式冲突 | OAI 路径用 `_oai_callbacks` + 独立 `_inflight_count`；Web UI 路径用 `_tasks` 表；task_id 用 `oai-` 前缀避免冲突 |
| 启动顺序：`image_task_service` 实例化时 `account_service` 未就绪 | max_workers 被低估 | max_workers 计算延迟到 lifespan startup（4.2.1） |
| Semaphore 不支持 resize | 改配置必须重启 | 文档明确告知；UI 提示"重启生效"（5.3） |
| Uvicorn 多 worker 模式下并发上限 ≠ 单 worker 值 | 用户预期与实际不符 | 文档说明（4.5 末）；默认部署单 worker |

---

## 11. 部署与回滚

### 11.1 部署

- 单次部署，无数据迁移
- API 兼容，无客户端改动
- 默认配置（`max_image_workers: null`）自动算，无需手动调

### 11.2 验证

部署后立即验证：

1. 启动日志中能看到 `image_workers_auto_config` 和 `starlette_pool_configured` 事件
2. `GET /api/system/info` 返回当前配置 + `effective` 值 + `current_inflight=0`
3. 触发 1 个图请求 → 成功返回；期间 `current_inflight` 应该 = 1，完成后回 0
4. **先调 `/api/system/info` 读取 `image_workers.effective` 的值 N**，然后并发触发 N+1 个图请求 → 第 N+1 个收到 503（带 `Retry-After: 5` 头）
5. 池满状态下 `GET /version` 响应时间 < 100ms（核心验证）

### 11.3 回滚

- 改动局限在路由层和 image_task_service
- 无 schema 变更，无数据迁移
- git revert 单个 PR 即可完整回滚
- 配置文件向前兼容（旧版本无视新字段）

### 11.4 监控指标（部署后一周）

- `image_queue_full_reject` 出现频率（应 < 5% 图请求）
- `/version` 平均响应时间（应保持 < 50ms）
- `/v1/chat/completions` 在图高峰时段的 p99（应不受影响）
- 容器内存峰值（应 < 容器限制的 80%）

---

## 12. 后续工作（不在本期）

- 把 `/v1/chat/completions` 也接入类似机制（如果 chat 也开始出现排队）
- 把 `image_task_service` 持久化从 json 文件迁移到 SQLite（高并发支持）
- 引入 metrics 库（如 prometheus_client）暴露标准指标
- 真正异步化 `_poll_image_results`（需要先验证 `curl-cffi` async 模式过 CF）

---

## 附录 A: 关键代码对比

**改造前** `api/ai.py:78-88`:

```python
@router.post("/v1/images/generations")
async def generate_images(body, request, authorization):
    identity = require_identity(authorization)
    payload = body.model_dump(mode="python")
    payload["base_url"] = resolve_image_base_url(request)
    call = LoggedCall(identity, "/v1/images/generations", body.model,
                      "文生图", request_text=body.prompt)
    await filter_or_log(call, body.prompt)
    # ↓ 同步 handler 在 Starlette 池里跑 60-180s，占住一个线程
    return await call.run(openai_v1_image_generations.handle, payload)
```

**改造后**:

```python
@router.post("/v1/images/generations")
async def generate_images(body, request, authorization):
    identity = require_identity(authorization)
    payload = body.model_dump(mode="python")
    payload["base_url"] = resolve_image_base_url(request)
    call = LoggedCall(identity, "/v1/images/generations", body.model,
                      "文生图", request_text=body.prompt)
    await filter_or_log(call, body.prompt)

    if body.stream:
        return await _handle_stream_image_via_isolated_executor(
            openai_v1_image_generations.handle, payload, call
        )

    try:
        # ↓ 协程级别 await，不占线程
        task = await image_task_service.submit_and_wait_async(
            identity, mode="generate", payload=payload,
            timeout=config.image_poll_timeout_secs + 30,
        )
    except ImageQueueFullError as exc:
        call.log("调用失败", status="failed", error="queue full")
        raise HTTPException(503,
            detail={"error": "image worker pool is full, retry later"},
            headers={"Retry-After": str(exc.retry_after)})
    except ImagePollTimeoutError as exc:
        call.log("调用失败", status="failed", error=str(exc))
        raise HTTPException(504, detail={"error": str(exc)})

    if task.get("status") == "error":
        call.log("调用失败", status="failed", error=task.get("error") or "")
        detail = {"error": task.get("error") or "image generation failed"}
        if task.get("error_type"):
            detail["type"] = task["error_type"]
        if task.get("code"):
            detail["code"] = task["code"]
        raise HTTPException(
            status_code=task.get("status_code") or 502,
            detail=detail,
        )

    call.log("调用完成")
    return _task_to_openai_image_response(task)
```

---

## 附录 B: 自动计算示例

| 账号数 | per_account | mem (MB) | by_accounts | by_memory | final |
|--------|-------------|----------|-------------|-----------|-------|
| 1 | 3 | 512 | 3 | 5 | **4** (min 兜底) |
| 5 | 3 | 1024 | 15 | 10 | **10** |
| 10 | 3 | 2048 | 30 | 20 | **20** |
| 20 | 3 | 4096 | 60 | 32 | **32** (硬上限) |
| 50 | 3 | 8192 | 150 | 81 | **32** (硬上限) |
| 5 | 5 | 2048 | 25 | 20 | **20** |
