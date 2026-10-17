# DuckAI Web-to-API v2 — Railway deploy

A FastAPI service that exposes a small OpenAI-compatible chat-completions API backed by the Duck.ai web interface. The Railway image installs Google Chrome because this implementation automates a real browser through Playwright.

## Deploy on Railway

1. Extract this ZIP into a folder.
2. Push the folder contents to a GitHub repository. Keep `Dockerfile`, `main.py`, `duckai.py`, and `requirements.txt` in the repository root.
3. In Railway, create a project and deploy the GitHub repository. Railway automatically detects the root `Dockerfile`.
4. In the service's **Variables** tab, add at least:

   ```env
   DUCKAI_API_KEY=use-a-long-random-secret-here
   DUCKAI_PREWARM=1
   DUCKAI_POOL_SIZE=2
   ```

   Keep the generated API key private. Add `DUCKAI_PROXIES` only if you have working proxy URLs; separate multiple proxies with commas. Proxy support depends on the proxy provider and Duck.ai's availability.

5. In **Settings → Networking**, generate a public domain.
6. Optionally configure the healthcheck path as `/health` in the Railway service settings.

Railway injects `PORT`; the app binds to `0.0.0.0:$PORT` automatically. You do not need to set a custom start command.

## Endpoints

- `GET /` — service information
- `GET /health` — health/readiness response
- `GET /v1/models` — cached Duck.ai model catalog (Bearer key required when configured)
- `POST /v1/chat/completions` — non-streaming and Server-Sent Events streaming
- `GET /stats` — in-memory request metrics (Bearer key required when configured)
- `GET /docs` — interactive FastAPI API documentation

If `DUCKAI_API_KEY` is set, send it as `Authorization: Bearer YOUR_KEY`. If it is empty, API authentication is disabled, so do not expose the service publicly without setting a key.

## Example request

```bash
curl https://YOUR-RAILWAY-DOMAIN/v1/chat/completions \
  -H 'Authorization: Bearer YOUR_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"Reply with pong"}],"stream":false}'
```

For streaming, change `"stream": false` to `"stream": true` and use `curl -N`.

## Environment variables

| Variable | Default | Purpose |
|---|---:|---|
| `PORT` | `8080` locally | Port; Railway supplies this value |
| `HOST` | `0.0.0.0` | Bind address |
| `DUCKAI_API_KEY` | empty | Optional Bearer key; strongly recommended on public deployments |
| `DUCKAI_MODEL` | `gpt-5.6-luna` | Model used for warm-up/default model |
| `DUCKAI_PREWARM` | `1` | Open a browser page during startup |
| `DUCKAI_POOL_SIZE` | `3` | Browser tabs per model pool; start with `1` or `2` on low-memory plans |
| `DUCKAI_PROXIES` | empty | Comma-separated proxy URLs |
| `DUCKAI_CATALOG_TTL` | `3600` | Seconds between catalog refreshes (minimum 60) |
| `DUCKAI_BAN_COOLDOWN` | `300` | Tab cooldown after a rate/ban signal |
| `DUCKAI_PROMPT_LIMIT` | `12000` | Maximum prompt characters if the UI has no maxlength |
| `DUCKAI_REQ_TIMEOUT` | `180` | Request deadline in seconds |
| `DUCKAI_LOG_LEVEL` | `INFO` | Python log level |
| `DUCKAI_BASE` | `https://duck.ai` | Duck.ai base URL |
| `DUCKAI_CHROME_PATH` | `/usr/bin/google-chrome-stable` | Chrome executable path |

## Important limitations

- This relies on Duck.ai's web UI and frontend request format, neither of which this project controls. If Duck.ai changes its UI, streaming format, access policy, or anti-abuse checks, the relay may stop working or return challenge/rate-limit errors. This is not a guaranteed uptime service.
- The current browser automation fills the chat prompt and clicks the UI send button. It does **not** explicitly select a requested model in the Duck.ai UI. The `model` parameter is resolved and returned in API responses, but actual model selection depends on the current Duck.ai page state. Do not assume that sending a different model ID necessarily changes the model used by Duck.ai.
- `/v1/images/generations` returns HTTP `501` deliberately. The source version only read text tokens and did not extract an actual image URL/base64 payload, so reporting a successful image generation would be misleading.
- Model catalog and statistics are in memory and reset on redeploy/restart. This build is intended as a small single-instance relay, not a durable multi-replica analytics service.
- The browser is started on demand for each model pool. Requesting many distinct model IDs can consume additional memory; avoid arbitrarily varying model names.

## Local run

Requires Docker for parity with Railway:

```bash
docker build -t duckai-railway .
docker run --rm -p 8080:8080 --env-file .env duckai-railway
```

Alternatively, install Python requirements and Google Chrome manually, then run `python main.py`.
