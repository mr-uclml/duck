from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, List, Optional, Union
from uuid import uuid4

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from duckai import (
    DEFAULT_MODEL,
    DuckAIBan,
    DuckAIError,
    FALLBACK_MODELS,
    ModelPool,
    fetch_model_catalog,
    resolve_model,
)

logging.basicConfig(
    level=os.getenv("DUCKAI_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("duckai2api")

API_KEY = os.getenv("DUCKAI_API_KEY", "").strip()
PROXIES = [p.strip() for p in os.getenv("DUCKAI_PROXIES", "").split(",") if p.strip()]
DEFAULT = os.getenv("DUCKAI_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
PORT = int(os.getenv("PORT", "8080"))
HOST = os.getenv("HOST", "0.0.0.0")
CATALOG_TTL = max(60.0, float(os.getenv("DUCKAI_CATALOG_TTL", "3600")))

POOLS: dict[str, ModelPool] = {}


def _pool(model: str) -> ModelPool:
    if model not in POOLS:
        POOLS[model] = ModelPool(model=model, proxies=PROXIES)
    return POOLS[model]


def _id() -> str:
    return f"chatcmpl-{uuid4().hex[:24]}"


def _created() -> int:
    return int(time.time())


def require_key(authorization: str = Header(default="")) -> None:
    if not API_KEY:
        return
    if not authorization.startswith("Bearer ") or authorization.split(" ", 1)[1] != API_KEY:
        raise HTTPException(status_code=401, detail="invalid API key")


def _text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                block_type = block.get("type")
                if block_type == "text":
                    parts.append(str(block.get("text", "")))
                elif block_type == "tool_result":
                    inner = block.get("content", "")
                    if isinstance(inner, list):
                        inner = " ".join(
                            str(item.get("text", "")) if isinstance(item, dict) else str(item)
                            for item in inner
                        )
                    parts.append(f"[tool_result {block.get('tool_use_id', '')}]: {inner}")
                elif block_type == "tool_use":
                    parts.append(
                        f'[tool_call name="{block.get("name", "")}"] '
                        f"{json.dumps(block.get('input', {}), ensure_ascii=False)}"
                    )
        return "\n".join(part for part in parts if part)
    return str(content)


def flatten(system: Any, messages: List[dict]) -> str:
    parts: List[str] = []
    system_text = _text(system)
    if system_text.strip():
        parts.append(f"System: {system_text}")
    for message in messages:
        role = message.get("role", "user")
        label = {"user": "Human", "assistant": "Assistant"}.get(role, "Human")
        if role == "tool":
            label = f"[tool_result {message.get('tool_call_id', '')}]"
        message_text = _text(message.get("content", ""))
        if message_text.strip():
            parts.append(f"{label}: {message_text}")
    return "\n\n".join(parts).strip()


CATALOG: list = list(FALLBACK_MODELS)
CATALOG_OK = False
_BOOT = time.time()
_REQS: collections.deque = collections.deque(maxlen=60)
_TOTALS = {"requests": 0, "in": 0, "out": 0, "ok": 0, "err": 0, "banned": 0}
_PER_MODEL: dict[str, dict] = {}


def _rec(model: str, tin: int, tout: int, elapsed_ms: float, ok: bool, banned: bool) -> None:
    _TOTALS["requests"] += 1
    _TOTALS["in"] += tin
    _TOTALS["out"] += tout
    _TOTALS["ok" if ok else "err"] += 1
    _TOTALS["banned"] += int(banned)
    stats = _PER_MODEL.setdefault(
        model,
        {"requests": 0, "in": 0, "out": 0, "ok": 0, "err": 0, "ms_sum": 0.0},
    )
    stats["requests"] += 1
    stats["in"] += tin
    stats["out"] += tout
    stats["ok" if ok else "err"] += 1
    stats["ms_sum"] += elapsed_ms
    _REQS.appendleft({
        "model": model,
        "in": tin,
        "out": tout,
        "ms": round(elapsed_ms),
        "ok": ok,
        "banned": banned,
        "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
    })


async def _catalog_refresher() -> None:
    global CATALOG, CATALOG_OK
    while True:
        try:
            models = await asyncio.to_thread(fetch_model_catalog)
            if models:
                CATALOG = models
                CATALOG_OK = True
                logger.info("Model catalog refreshed: %d models", len(models))
        except Exception as error:
            logger.warning("Catalog refresh failed; retaining previous catalog: %s", error)
        await asyncio.sleep(CATALOG_TTL)


async def _prewarm() -> None:
    try:
        await _pool(DEFAULT).prewarm()
        logger.info("Prewarmed browser pool for model %s", DEFAULT)
    except Exception as error:
        logger.warning("Prewarm skipped: %s", error)


app = FastAPI(
    title="DuckAI Web-to-API",
    version="2.0.0",
    description="OpenAI-style chat completions relay backed by the Duck.ai web interface.",
)


@app.on_event("startup")
async def _startup() -> None:
    if not API_KEY:
        logger.warning("DUCKAI_API_KEY is empty: API endpoints are not authenticated")
    asyncio.create_task(_catalog_refresher())
    if os.getenv("DUCKAI_PREWARM", "1").lower() not in ("0", "false", "no"):
        asyncio.create_task(_prewarm())


@app.on_event("shutdown")
async def _shutdown() -> None:
    for model, pool in list(POOLS.items()):
        try:
            await pool.close()
        except Exception as error:
            logger.warning("Pool shutdown failed for %s: %s", model, error)


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    role: str
    content: Union[str, List[Any], None] = ""
    tool_call_id: Optional[str] = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    model: str = DEFAULT
    messages: List[ChatMessage] = Field(min_length=1)
    stream: Optional[bool] = False
    temperature: Optional[float] = None
    top_p: Optional[float] = None


def _err_sse(message: str) -> str:
    payload = {"error": {"message": message, "type": "server_error"}}
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def _openai_stream(pool: ModelPool, prompt: str, model: str, chat_id: str):
    def chunk(delta: dict, finish_reason):
        return {
            "id": chat_id,
            "object": "chat.completion.chunk",
            "created": _created(),
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }

    started = time.time()
    output_chars = 0
    succeeded = False
    banned = False
    try:
        yield f"data: {json.dumps(chunk({'role': 'assistant', 'content': ''}, None))}\n\n"
        async for token in pool.stream(prompt):
            output_chars += len(token)
            yield f"data: {json.dumps(chunk({'content': token}, None), ensure_ascii=False)}\n\n"
        yield f"data: {json.dumps(chunk({}, 'stop'))}\n\n"
        yield "data: [DONE]\n\n"
        succeeded = True
    except DuckAIBan as error:
        banned = True
        yield _err_sse(f"{error} (HTTP 429; wait for cooldown or configure DUCKAI_PROXIES)")
    except DuckAIError as error:
        yield _err_sse(str(error))
    except Exception as error:
        logger.exception("Unexpected streaming error")
        yield _err_sse(f"stream failed: {error}")
    finally:
        _rec(model, len(prompt), output_chars, (time.time() - started) * 1000, succeeded, banned)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "name": "DuckAI Web-to-API",
        "version": "2.0.0",
        "status": "running",
        "health": "/health",
        "models": "/v1/models",
        "chat_completions": "/v1/chat/completions",
        "docs": "/docs",
    }


@app.post("/v1/chat/completions", dependencies=[Depends(require_key)])
async def chat_completions(request: ChatCompletionRequest):
    system = None
    turns = []
    for message in request.messages:
        if message.role == "system":
            system = message.content
        else:
            turns.append({
                "role": message.role,
                "content": message.content,
                "tool_call_id": message.tool_call_id,
            })
    prompt = flatten(system, turns)
    if not prompt.strip():
        raise HTTPException(status_code=400, detail="empty prompt")

    model = resolve_model(request.model)
    logger.info(
        "Chat request model=%s resolved=%s stream=%s messages=%d prompt_chars=%d",
        request.model, model, bool(request.stream), len(turns), len(prompt),
    )
    pool = _pool(model)
    if request.stream:
        return StreamingResponse(
            _openai_stream(pool, prompt, model, _id()),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    started = time.time()
    try:
        result = "".join([token async for token in pool.stream(prompt)])
    except DuckAIBan as error:
        _rec(model, len(prompt), 0, (time.time() - started) * 1000, False, True)
        raise HTTPException(
            status_code=429,
            detail=f"{error}. Wait for cooldown or configure DUCKAI_PROXIES.",
            headers={"Retry-After": "60"},
        ) from error
    except DuckAIError as error:
        _rec(model, len(prompt), 0, (time.time() - started) * 1000, False, False)
        raise HTTPException(status_code=502, detail=str(error)) from error
    except Exception as error:
        logger.exception("Unexpected completion error")
        _rec(model, len(prompt), 0, (time.time() - started) * 1000, False, False)
        raise HTTPException(status_code=502, detail="Browser relay failed") from error

    _rec(model, len(prompt), len(result), (time.time() - started) * 1000, True, False)
    return {
        "id": _id(),
        "object": "chat.completion",
        "created": _created(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": result.strip()},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


@app.post("/v1/images/generations", dependencies=[Depends(require_key)])
async def images_generations():
    raise HTTPException(
        status_code=501,
        detail=(
            "Image delivery is not implemented in this build. The previous stub only returned "
            "text from the chat stream, not an image URL or base64 image."
        ),
    )


@app.get("/v1/models", dependencies=[Depends(require_key)])
async def list_models():
    result = []
    for model in CATALOG:
        model_id = model.get("id")
        if not model_id:
            continue
        tiers = model.get("accessTier") or []
        result.append({
            "id": model_id,
            "object": "model",
            "created": _created(),
            "owned_by": "duck.ai",
            "display_name": model.get("name") or model_id,
            "access": "free" if "free" in tiers else "paid",
        })
    return {"object": "list", "data": result}


@app.get("/stats", dependencies=[Depends(require_key)])
async def stats():
    now = time.time()
    per_model = []
    for model, values in _PER_MODEL.items():
        total = values["ok"] + values["err"]
        health_percent = round(100 * values["ok"] / total, 2) if total else 0.0
        ms_per_ktoken = round(values["ms_sum"] / max(values["out"], 1) * 1000) if values["out"] else 0
        per_model.append({
            "model": model,
            "health": health_percent,
            "ms_per_ktoken": ms_per_ktoken,
            "requests": values["requests"],
            "errors": values["err"],
            "in": values["in"],
            "out": values["out"],
        })
    per_model.sort(key=lambda item: -item["requests"])
    total_requests = _TOTALS["requests"]
    success_rate = _TOTALS["ok"] / total_requests * 100 if total_requests else 0.0
    return {
        "boot": _BOOT,
        "uptime_s": int(now - _BOOT),
        "totals": dict(_TOTALS),
        "success_rate": round(success_rate, 2),
        "avg_ms": round(sum(item["ms"] for item in _REQS) / max(len(_REQS), 1), 0),
        "per_model": per_model,
        "recent": list(_REQS)[:15],
    }


@app.get("/health")
async def health():
    pools = {}
    for model, pool in POOLS.items():
        try:
            connected = bool(pool.browser and pool.browser.is_connected())
        except Exception:
            connected = False
        pools[model] = {"tabs": len(pool.tabs), "connected": connected}
    return {
        "status": "ok",
        "models": len(CATALOG),
        "catalog_live": CATALOG_OK,
        "uptime_s": int(time.time() - _BOOT),
        "pools": pools,
    }


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
