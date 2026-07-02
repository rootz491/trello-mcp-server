# AGENTS.md

## Cursor Cloud specific instructions

This repo is a single-file Cloudflare Worker MCP server for Trello (`src/index.ts`). It is a headless HTTP/SSE service — there is no GUI, so verify it with `curl` against the local dev server.

### Running the dev server
- Start with `npm run dev` (runs `tsc` then `wrangler dev`). Wrangler serves on `http://localhost:8787`.
- On the very first run in a fresh VM, Wrangler downloads its `workerd` runtime, so startup can take longer than subsequent runs. Wait for the `[wrangler:info] Ready on http://localhost:8787` line before sending requests.
- Run it in a long-lived tmux session; `wrangler dev` is a foreground watcher, not a one-shot command.

### Lint / test / build
- There is no lint config and no automated test suite in this repo.
- Type-check: `npm run typecheck` (`tsc --noEmit`). Build: `npm run build` (`tsc` → `dist/index.js`).

### Exercising the MCP server (no secrets required)
- MCP handshake and tool discovery work without any credentials:
  - `POST /messages` with a JSON-RPC `initialize` body returns server info.
  - `POST /messages` with `{"method":"tools/list"}` lists the four Trello tools.
  - `GET /sse` returns an `event: endpoint` announcement with a generated `sessionId`.
- `tools/call` (e.g. `list_boards`) proxies to the real Trello REST API. Without `TRELLO_API_KEY` / `TRELLO_TOKEN` it returns a graceful JSON-RPC error (`Trello API error: 401 invalid key`). To fully exercise tool calls, create a gitignored `.dev.vars` with `TRELLO_API_KEY`, `TRELLO_TOKEN`, and (optionally) `MCP_AUTH_TOKEN` as documented in `README.md`.
- Auth is optional: when `MCP_AUTH_TOKEN` is unset, all requests are authorized; when set, requests need `Authorization: Bearer <token>` (or a `?token=`/`?auth=` query param for EventSource).
