from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import urllib.request
from typing import AsyncIterator, Optional

from playwright.async_api import async_playwright

logger = logging.getLogger("duckai")

BASE = os.getenv("DUCKAI_BASE", "https://duck.ai").rstrip("/")
CHROME_PATH = os.getenv("DUCKAI_CHROME_PATH", "/usr/bin/google-chrome-stable")
POOL_SIZE = max(1, min(8, int(os.getenv("DUCKAI_POOL_SIZE", "3"))))
WARM_MIN = max(0.0, float(os.getenv("DUCKAI_WARM_MIN", "2.0")))
WARM_MAX = max(WARM_MIN, float(os.getenv("DUCKAI_WARM_MAX", "7.0")))
BAN_COOLDOWN = max(5.0, float(os.getenv("DUCKAI_BAN_COOLDOWN", "300")))
PROMPT_LIMIT = max(1000, int(os.getenv("DUCKAI_PROMPT_LIMIT", "12000")))
REQ_TIMEOUT = max(10.0, float(os.getenv("DUCKAI_REQ_TIMEOUT", "180")))
STREAM_POLL_MS = max(100, int(os.getenv("DUCKAI_STREAM_POLL_MS", "200")))

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
)

DEFAULT_MODEL = "gpt-5.6-luna"
MODEL_ALIASES = {
    "gpt-5.6": "gpt-5.6-luna",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.4": "gpt-5.4-mini",
    "gpt-4o": "gpt-5.4-mini",
    "gpt-4o-mini": "gpt-5.4-mini",
    "o3-mini": "gpt-5.4-mini",
    "claude-3-5-sonnet": "claude-sonnet-4-6",
    "claude-sonnet": "claude-sonnet-4-6",
    "claude-sonnet-4-6": "claude-sonnet-4-6",
    "claude-3-haiku": "claude-haiku-4-5",
    "claude-haiku": "claude-haiku-4-5",
    "claude-haiku-4-5": "claude-haiku-4-5",
    "claude-3-opus": "claude-opus-4-8",
    "claude-opus": "claude-opus-4-8",
    "claude-opus-4-8": "claude-opus-4-8",
    "mistral-small": "mistral-small-2603",
    "mistral-small-2603": "mistral-small-2603",
    "gpt-oss-120b": "tinfoil/gpt-oss-120b",
    "tinfoil/gpt-oss-120b": "tinfoil/gpt-oss-120b",
    "gemma4-31b": "tinfoil/gemma4-31b",
    "tinfoil/gemma4-31b": "tinfoil/gemma4-31b",
    "gpt-image-1": "gpt-5.6-luna",
    "gpt-image-2": "gpt-5.6-luna",
    "dall-e-3": "gpt-5.6-luna",
}

FALLBACK_MODELS = [
    {"id": "gpt-5.6-luna", "name": "GPT-5.6 Luna", "accessTier": ["free"]},
    {"id": "gpt-5.4-mini", "name": "GPT-5.4 mini", "accessTier": ["free"]},
    {"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6", "accessTier": ["free"]},
    {"id": "claude-haiku-4-5", "name": "Claude Haiku 4.5", "accessTier": ["free"]},
    {"id": "claude-opus-4-8", "name": "Claude Opus 4.8", "accessTier": ["free"]},
    {"id": "mistral-small-2603", "name": "Mistral Small 4", "accessTier": ["free"]},
    {"id": "tinfoil/gpt-oss-120b", "name": "gpt-oss 120B", "accessTier": ["free"]},
    {"id": "tinfoil/gemma4-31b", "name": "Gemma 4 31B", "accessTier": ["free"]},
]

LIVE_MODELS: dict[str, str] = {}


def note_live_models(models: list) -> None:
    fresh = {}
    for model in models:
        model_id = (model.get("id") or "").strip()
        if model_id:
            fresh[model_id.lower()] = model_id
    if fresh:
        LIVE_MODELS.clear()
        LIVE_MODELS.update(fresh)


def resolve_model(name: Optional[str]) -> str:
    if not name:
        return DEFAULT_MODEL
    name = name.strip()
    low = name.lower()
    if low in LIVE_MODELS:
        return LIVE_MODELS[low]
    if low in MODEL_ALIASES:
        return MODEL_ALIASES[low]
    return name


def fetch_model_catalog(timeout: float = 10.0) -> list:
    request = urllib.request.Request(
        f"{BASE}/duckchat/v1/models",
        headers={"accept": "application/json", "user-agent": UA},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    models = payload.get("models") or []
    note_live_models(models)
    return models


class DuckAIError(Exception):
    pass


class DuckAIBan(DuckAIError):
    pass


_INIT_SCRIPT = r"""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
window.__duckai = { chunks: [], done: false, err: null };
(function () {
  const originalFetch = window.fetch;
  window.fetch = async function (url, options) {
    options = options || {};
    if (typeof url === 'string' && url.includes('/duckchat/v1/chat')) {
      const data = window.__duckai;
      data.chunks = []; data.done = false; data.err = null;
      const response = await originalFetch.call(this, url, options);
      try {
        const reader = response.clone().body.getReader();
        const decoder = new TextDecoder();
        (async function pump() {
          try {
            while (true) {
              const result = await reader.read();
              if (result.done) {
                data.chunks.push(decoder.decode()); data.done = true; break;
              }
              const part = decoder.decode(result.value, { stream: true });
              data.chunks.push(part);
              if (part.includes('"action":"error"') || part.includes('ERR_BN_LIMIT') || part.includes('ERR_CHALLENGE')) {
                data.err = part;
              }
            }
          } catch (error) { data.err = String(error); data.done = true; }
        })();
      } catch (error) { data.err = String(error); data.done = true; }
      return response;
    }
    return originalFetch.apply(this, arguments);
  };
})();
"""

_RESET_JS = r"() => { window.__duckai = { chunks: [], done: false, err: null }; }"
_POLL_JS = r"""
(n) => {
  const data = window.__duckai || { chunks: [], done: false, err: null };
  return { n: data.chunks.length, text: data.chunks.slice(n).join(''), done: data.done, err: data.err };
}
"""


def _parse_event(line: str) -> Optional[dict]:
    line = line.strip()
    if line.startswith("data:"):
        line = line[5:].strip()
    elif not line.startswith("{"):
        return None
    if line == "[DONE]" or not line.startswith("{"):
        return None
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _clamp(prompt: str, limit: int) -> str:
    marker = "\n...[context truncated]...\n"
    if limit <= len(marker) + 40:
        return prompt[:limit]
    head = max(1, limit // 10)
    tail = limit - head - len(marker)
    return prompt[:head] + marker + prompt[-tail:]


def _norm_proxy(proxy: str) -> dict:
    match = re.match(
        r"^(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*)://(?:(?P<user>[^:@]+):(?P<pwd>[^@]*)@)?(?P<host>.+)$",
        proxy.strip(),
    )
    if not match:
        return {"server": f"http://{proxy.strip()}"}
    result = {"server": f"{match.group('scheme')}://{match.group('host')}"}
    if match.group("user"):
        result["username"] = match.group("user")
        result["password"] = match.group("pwd") or ""
    return result


class Tab:
    def __init__(self, context, model: str) -> None:
        self.ctx = context
        self.model = model
        self.page = None
        self._last_prompt: Optional[str] = None
        self.lock = asyncio.Lock()
        self.banned_until = 0.0

    async def _new_page(self):
        page = await self.ctx.new_page()
        await page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
        opened = time.monotonic()
        deadline = opened + WARM_MAX
        while time.monotonic() < deadline:
            if time.monotonic() - opened >= WARM_MIN:
                try:
                    if await page.query_selector("textarea") is not None:
                        break
                except Exception:
                    break
            await page.wait_for_timeout(200)
        return page

    async def _open_warm(self) -> None:
        if self.page is not None and not self.page.is_closed():
            return
        self.page = await self._new_page()
        self._last_prompt = None

    async def _drop_page(self) -> None:
        if self.page is not None:
            try:
                await self.page.close()
            except Exception:
                pass
            self.page = None
        self._last_prompt = None

    def _continuation(self, prompt: str) -> Optional[str]:
        if self._last_prompt is None or self.page is None:
            return None
        if not prompt.startswith(self._last_prompt):
            return None
        tail = prompt[len(self._last_prompt):].strip()
        marks = list(re.finditer(r"(?m)^Human: ", tail))
        if len(marks) != 1:
            return None
        return tail[marks[0].end():].strip() or None

    def _raise_err(self, text: str) -> None:
        if "ERR_BN_LIMIT" in text:
            self.banned_until = time.monotonic() + BAN_COOLDOWN
            raise DuckAIBan("ERR_BN_LIMIT (418)")
        if "ERR_CHALLENGE" in text:
            raise DuckAIError("ERR_CHALLENGE (challenge required)")
        match = re.search(r'"type"\s*:\s*"([^"]+)"', text)
        raise DuckAIError(f"Duck.ai error: {match.group(1) if match else 'unknown'}")

    async def _iter_events(self, prompt: str, timeout: float) -> AsyncIterator[dict]:
        text = self._continuation(prompt) if self.page is not None else None
        if text is None:
            await self._drop_page()
            await self._open_warm()
            text = prompt
        page = self.page
        await page.evaluate(_RESET_JS)
        try:
            await self._type_and_send(page, text)
        except Exception:
            await self._drop_page()
            raise

        buffer = ""
        index = 0
        saw_data = False
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = await page.evaluate(_POLL_JS, index)
            index = state["n"]
            if state.get("err"):
                await self._drop_page()
                self._raise_err(state["err"])
            new_text = state.get("text") or ""
            if new_text:
                saw_data = True
                buffer += new_text
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    event = _parse_event(line)
                    if event is not None:
                        yield event
            if state.get("done"):
                if buffer.strip():
                    event = _parse_event(buffer)
                    if event is not None:
                        yield event
                if not saw_data:
                    await self._drop_page()
                    raise DuckAIError("empty stream")
                self._last_prompt = prompt
                return
            await page.wait_for_timeout(STREAM_POLL_MS if saw_data else 500)
        await self._drop_page()
        raise DuckAIError("timed out waiting for Duck.ai reply")

    async def _type_and_send(self, page, prompt: str) -> None:
        textarea = await page.query_selector("textarea")
        if textarea is None:
            raise DuckAIError("textarea not found (Duck.ai UI may have changed)")
        raw_limit = await textarea.get_attribute("maxlength")
        limit = int(raw_limit) if raw_limit and raw_limit.isdigit() and int(raw_limit) > 0 else PROMPT_LIMIT
        if len(prompt) > limit:
            logger.warning("Prompt has %d chars; clamping to %d", len(prompt), limit)
            prompt = _clamp(prompt, limit)
        await textarea.click()
        await textarea.fill(prompt)
        await page.wait_for_timeout(300)
        sent = False
        for button in await page.query_selector_all("button"):
            label = (await button.inner_text() or "").strip()
            aria = await button.get_attribute("aria-label") or ""
            if label in ("Ask", "Send") or aria in ("Ask", "Send", "Ask AI", "Submit"):
                await button.click(force=True)
                sent = True
                break
        if not sent:
            await textarea.press("Enter")

    async def stream(self, prompt: str, timeout: float) -> AsyncIterator[str]:
        async with self.lock:
            async for event in self._iter_events(prompt, timeout):
                message = event.get("message")
                if isinstance(message, str) and message:
                    yield message

    async def close(self) -> None:
        await self._drop_page()


IMAGE_INSTRUCTION = (
    "Generate an image directly using your image generation tool based on the "
    "following description. Do not ask questions, do not explain the process. "
    "Image description: {prompt}{size_hint}"
)


def build_image_prompt(prompt: str, size: Optional[str] = None) -> str:
    hint = ""
    if size:
        dimensions = re.split(r"[x*]", str(size))
        try:
            width, height = int(dimensions[0]), int(dimensions[1])
            hint = (
                " (landscape composition)" if width > height else
                " (portrait composition)" if height > width else
                " (square composition)"
            )
        except (ValueError, IndexError):
            pass
    return IMAGE_INSTRUCTION.format(prompt=prompt, size_hint=hint)


class ModelPool:
    def __init__(self, model: str, proxies: Optional[list] = None) -> None:
        self.model = model
        self.proxies = [p.strip() for p in (proxies or []) if p and p.strip()] or [None]
        self._pw = None
        self.browser = None
        self.ctx = None
        self.tabs: list[Tab] = []
        self._pidx = 0

    async def _ensure(self) -> None:
        if self.ctx is None or self.browser is None or not self.browser.is_connected():
            await self._launch(self._pidx)
        while len(self.tabs) < POOL_SIZE:
            self.tabs.append(Tab(self.ctx, self.model))

    async def _launch(self, proxy_index: int) -> None:
        if self._pw is None:
            self._pw = await async_playwright().start()
        launch_options: dict = {"headless": True, "channel": "chrome"}
        if CHROME_PATH and os.path.exists(CHROME_PATH):
            launch_options["executable_path"] = CHROME_PATH
        launch_options["args"] = ["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"]
        proxy = self.proxies[proxy_index]
        if proxy:
            launch_options["proxy"] = _norm_proxy(proxy)
        try:
            browser = await self._pw.chromium.launch(**launch_options)
        except Exception as error:
            if len(self.proxies) > 1:
                next_index = (proxy_index + 1) % len(self.proxies)
                logger.warning("Chrome launch with proxy %d failed; trying proxy %d next request: %s", proxy_index, next_index, error)
                self._pidx = next_index
                raise DuckAIError(f"Chrome launch failed with configured proxy: {error}") from error
            raise
        context = await browser.new_context(user_agent=UA)
        await context.add_init_script(_INIT_SCRIPT)
        if self.browser is not None:
            try:
                await self.browser.close()
            except Exception:
                pass
        self.browser = browser
        self.ctx = context
        self.tabs = []

    @staticmethod
    def _load(t: Tab) -> int:
        waiters = t.lock._waiters
        return (1 if t.lock.locked() else 0) + (len(waiters) if waiters else 0)

    async def stream(self, prompt: str, timeout: Optional[float] = None) -> AsyncIterator[str]:
        timeout = timeout or REQ_TIMEOUT
        deadline = time.monotonic() + timeout
        last_ban: Optional[DuckAIBan] = None
        backoff = 0.5
        while True:
            now = time.monotonic()
            if now > deadline:
                if last_ban:
                    raise last_ban
                raise DuckAIError("timed out (all tabs busy or banned)")
            await self._ensure()
            available = [tab for tab in self.tabs if tab.banned_until <= now]
            if not available:
                wait = min(tab.banned_until for tab in self.tabs) - now
                remaining = deadline - now
                if wait < remaining:
                    logger.info("All tabs banned; retrying after %.1fs", wait)
                    await asyncio.sleep(wait + 0.2)
                    continue
                raise last_ban or DuckAIBan(f"all tabs banned; cooldown {wait:.0f}s")
            tab = min(available, key=self._load)
            try:
                async for token in tab.stream(prompt, max(1.0, deadline - time.monotonic())):
                    yield token
                return
            except DuckAIBan as error:
                last_ban = error
                tab.banned_until = time.monotonic() + BAN_COOLDOWN
                await tab._drop_page()
                logger.warning("Tab banned; cooling down for %.0fs", BAN_COOLDOWN)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
            except DuckAIError:
                raise
            except Exception as error:
                try:
                    if self.browser:
                        await self.browser.close()
                except Exception:
                    pass
                self.browser = None
                self.ctx = None
                self.tabs = []
                raise DuckAIError(f"browser transport failed: {error}") from error

    async def prewarm(self) -> bool:
        try:
            await self._ensure()
            async with self.tabs[0].lock:
                await self.tabs[0]._open_warm()
            return True
        except Exception as error:
            logger.warning("Prewarm failed; lazy startup will be used: %s", error)
            return False

    async def close(self) -> None:
        for tab in self.tabs:
            await tab.close()
        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self.browser = None
        self.ctx = None
        self.tabs = []
        self._pw = None
