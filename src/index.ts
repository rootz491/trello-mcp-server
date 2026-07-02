/**
 * Cloudflare Worker MCP Server for Trello with stateless HTTP messages
 * 
 * Supports Poke's MCP requirements:
 * - GET /sse: Establish EventSource connection and announce message endpoint
 * - POST /messages: Receive and answer JSON-RPC messages without isolate-local state
 */

interface Env {
  TRELLO_API_KEY: string;
  TRELLO_TOKEN: string;
  MCP_AUTH_TOKEN?: string; // Kept for backward compatibility or optional use
}

const TRELLO_BASE_URL = 'https://api.trello.com/1';
const MCP_PROTOCOL_VERSION = '2024-11-05';
const SSE_KEEP_ALIVE_INTERVAL_MS = 15000;

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Mcp-Session-Id, Last-Event-ID',
  'Access-Control-Expose-Headers': 'Mcp-Session-Id',
};

type JsonRpcId = string | number | null;

interface JsonRpcError {
  code: number;
  message: string;
}

interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: JsonRpcId;
  result?: unknown;
  error?: JsonRpcError;
}

class JsonRpcException extends Error {
  constructor(
    readonly code: number,
    message: string,
  ) {
    super(message);
    this.name = 'JsonRpcException';
  }
}

// Memory log buffer for debugging
const debugLogs: string[] = [];
function debugLog(msg: string) {
  const formatted = `[${new Date().toISOString()}] ${msg}`;
  console.log(formatted);
  debugLogs.push(formatted);
  if (debugLogs.length > 200) {
    debugLogs.shift();
  }
}

function unauthorized(): Response {
  return new Response('Unauthorized', {
    status: 401,
    headers: { ...CORS_HEADERS }
  });
}

function isAuthorized(request: Request, env: Env): boolean {
  if (!env.MCP_AUTH_TOKEN) return true;

  const auth = request.headers.get('Authorization');
  if (auth === `Bearer ${env.MCP_AUTH_TOKEN}`) return true;

  // Query parameter fallback for EventSource which may not support headers
  const url = new URL(request.url);
  const token = url.searchParams.get('token') || url.searchParams.get('auth');
  return token === env.MCP_AUTH_TOKEN;
}

async function trelloFetch(path: string, env: Env, params: Record<string, string> = {}) {
  const url = new URL(`${TRELLO_BASE_URL}${path}`);
  url.searchParams.set('key', env.TRELLO_API_KEY);
  url.searchParams.set('token', env.TRELLO_TOKEN);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url.toString());
  if (!response.ok) {
    throw new Error(`Trello API error: ${response.status} ${await response.text()}`);
  }
  return response.json();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isJsonRpcId(value: unknown): value is JsonRpcId {
  return value === null || typeof value === 'string' || typeof value === 'number';
}

function hasOwn(value: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function makeJsonRpcResult(id: JsonRpcId, result: unknown): JsonRpcResponse {
  return { jsonrpc: '2.0', id, result };
}

function makeJsonRpcError(id: JsonRpcId, code: number, message: string): JsonRpcResponse {
  return { jsonrpc: '2.0', id, error: { code, message } };
}

function jsonResponse(payload: unknown, status = 200, extraHeaders: HeadersInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...CORS_HEADERS,
      ...extraHeaders,
    },
  });
}

function responseHeaders(sessionId: string | null = null): HeadersInit {
  return sessionId
    ? { ...CORS_HEADERS, 'Mcp-Session-Id': sessionId }
    : { ...CORS_HEADERS };
}

function getStringArg(args: Record<string, unknown>, key: string): string | null {
  const value = args[key];
  return typeof value === 'string' && value.trim() ? value : null;
}

function getLimitArg(args: Record<string, unknown>): string {
  const value = args.limit;
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === 'string' && value.trim()) {
    return value;
  }
  return '10';
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const timeoutId = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timeoutId);
      resolve();
    };

    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function createSseResponse(request: Request): Response {
  const requestUrl = new URL(request.url);
  const sessionId = crypto.randomUUID();
  const messageUrl = new URL('/messages', request.url);
  messageUrl.searchParams.set('sessionId', sessionId);

  for (const authParam of ['token', 'auth']) {
    const value = requestUrl.searchParams.get(authParam);
    if (value) {
      messageUrl.searchParams.set(authParam, value);
    }
  }

  const encoder = new TextEncoder();
  let cleanup: (() => void) | null = null;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      let closed = false;

      const close = () => {
        if (closed) {
          return;
        }
        closed = true;
        try {
          controller.close();
        } catch (err: unknown) {
          debugLog(`[GET /sse] Stream already closed for session ${sessionId}: ${String(err)}`);
        }
      };

      const write = (chunk: string): boolean => {
        if (closed) {
          return false;
        }

        try {
          controller.enqueue(encoder.encode(chunk));
          return true;
        } catch (err: unknown) {
          debugLog(`[GET /sse] Failed writing to SSE stream for session ${sessionId}: ${String(err)}`);
          closed = true;
          try {
            controller.error(err);
          } catch {
            // The stream may already be closed by the runtime.
          }
          return false;
        }
      };

      const onAbort = () => {
        debugLog(`[GET /sse] Connection aborted/closed by client for session: ${sessionId}`);
        close();
      };

      cleanup = () => {
        request.signal.removeEventListener('abort', onAbort);
        close();
      };

      request.signal.addEventListener('abort', onAbort, { once: true });

      void (async () => {
        try {
          debugLog(`[GET /sse] Announcing stateless endpoint for session: ${sessionId}`);
          write(`event: endpoint\ndata: ${messageUrl.toString()}\n\n`);

          while (!request.signal.aborted && !closed) {
            await sleep(SSE_KEEP_ALIVE_INTERVAL_MS, request.signal);
            if (!request.signal.aborted && !closed) {
              write(':\n\n');
            }
          }
        } finally {
          debugLog(`[GET /sse] Writer cleanup running for session: ${sessionId}`);
          cleanup?.();
        }
      })();
    },

    cancel(reason) {
      debugLog(`[GET /sse] Stream cancelled for session ${sessionId}: ${String(reason)}`);
      cleanup?.();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      ...responseHeaders(sessionId),
    },
  });
}

async function handleToolCall(params: unknown, env: Env): Promise<unknown> {
  if (!isRecord(params) || typeof params.name !== 'string') {
    throw new JsonRpcException(-32602, 'Invalid params: tools/call requires a string "name"');
  }

  const toolName = params.name;
  const toolArgs = isRecord(params.arguments) ? params.arguments : {};
  debugLog(`[JSON-RPC] Calling tool: "${toolName}" with args: ${JSON.stringify(toolArgs)}`);

  let data: unknown;
  if (toolName === 'list_boards') {
    data = await trelloFetch('/members/me/boards', env, { fields: 'name,url' });
  } else if (toolName === 'get_lists') {
    const boardId = getStringArg(toolArgs, 'boardId');
    if (!boardId) {
      throw new JsonRpcException(-32602, 'Invalid params: get_lists requires "boardId"');
    }
    data = await trelloFetch(`/boards/${boardId}/lists`, env, { fields: 'name' });
  } else if (toolName === 'list_cards') {
    const listId = getStringArg(toolArgs, 'listId');
    if (!listId) {
      throw new JsonRpcException(-32602, 'Invalid params: list_cards requires "listId"');
    }
    data = await trelloFetch(`/lists/${listId}/cards`, env, { fields: 'name,desc,url' });
  } else if (toolName === 'check_updates') {
    const boardId = getStringArg(toolArgs, 'boardId');
    if (!boardId) {
      throw new JsonRpcException(-32602, 'Invalid params: check_updates requires "boardId"');
    }
    data = await trelloFetch(`/boards/${boardId}/actions`, env, { limit: getLimitArg(toolArgs) });
  } else {
    throw new JsonRpcException(-32601, `Unknown tool: ${toolName}`);
  }

  return {
    content: [{ type: 'text', text: JSON.stringify(data, null, 2) }],
  };
}

async function handleJsonRpcMessage(message: unknown, env: Env): Promise<JsonRpcResponse | null> {
  if (!isRecord(message)) {
    return makeJsonRpcError(null, -32600, 'Invalid Request');
  }

  const id = isJsonRpcId(message.id) ? message.id : null;
  const isNotification = !hasOwn(message, 'id');

  if (typeof message.method !== 'string') {
    return isNotification ? null : makeJsonRpcError(id, -32600, 'Invalid Request');
  }

  const { method, params } = message;
  debugLog(`[JSON-RPC] Received method: "${method}", id: ${isNotification ? 'notification' : String(id)}`);

  try {
    let result: unknown;

    switch (method) {
      case 'initialize':
        result = {
          protocolVersion: MCP_PROTOCOL_VERSION,
          capabilities: {
            tools: {}
          },
          serverInfo: { name: 'trello-mcp-worker', version: '1.1.0' },
        };
        break;

      case 'notifications/initialized':
        return null;

      case 'ping':
        result = {};
        break;

      case 'tools/list':
      case 'list_tools':
        result = {
          tools: [
            {
              name: 'list_boards',
              description: 'List all boards the user has access to.',
              inputSchema: { type: 'object', properties: {} },
            },
            {
              name: 'get_lists',
              description: 'Get lists on a specific Trello board.',
              inputSchema: {
                type: 'object',
                properties: { boardId: { type: 'string' } },
                required: ['boardId'],
              },
            },
            {
              name: 'list_cards',
              description: 'Get cards within a specific list.',
              inputSchema: {
                type: 'object',
                properties: { listId: { type: 'string' } },
                required: ['listId'],
              },
            },
            {
              name: 'check_updates',
              description: 'Check for recent actions/updates on a board.',
              inputSchema: {
                type: 'object',
                properties: { boardId: { type: 'string' }, limit: { type: 'number', default: 10 } },
                required: ['boardId'],
              },
            },
          ],
        };
        break;

      case 'resources/list':
        result = { resources: [] };
        break;

      case 'prompts/list':
        result = { prompts: [] };
        break;

      case 'tools/call':
      case 'call_tool':
        result = await handleToolCall(params, env);
        break;

      default:
        return isNotification ? null : makeJsonRpcError(id, -32601, `Method not found: ${method}`);
    }

    return isNotification ? null : makeJsonRpcResult(id, result);
  } catch (err: unknown) {
    const messageText = err instanceof Error ? err.message : 'Error processing request';
    const code = err instanceof JsonRpcException ? err.code : -32603;
    console.error('[JSON-RPC] Processing error:', err);
    return isNotification ? null : makeJsonRpcError(id, code, messageText);
  }
}

async function handleJsonRpcPayload(payload: unknown, env: Env): Promise<unknown | null> {
  if (Array.isArray(payload)) {
    if (payload.length === 0) {
      return makeJsonRpcError(null, -32600, 'Invalid Request');
    }

    const responses = await Promise.all(payload.map((message) => handleJsonRpcMessage(message, env)));
    return responses.filter((response): response is JsonRpcResponse => response !== null);
  }

  return handleJsonRpcMessage(payload, env);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const url = new URL(request.url);

      // Handle CORS preflight
      if (request.method === 'OPTIONS') {
        return new Response(null, {
          status: 204,
          headers: {
            ...CORS_HEADERS,
            'Access-Control-Max-Age': '86400',
          },
        });
      }

      // Handle Debug Logs route
      if (request.method === 'GET' && url.pathname === '/debug') {
        if (!isAuthorized(request, env)) {
          return unauthorized();
        }
        return new Response(debugLogs.join('\n'), {
          headers: {
            'Content-Type': 'text/plain',
            ...CORS_HEADERS,
          },
        });
      }

      // 1. Handle SSE Connection (GET /sse or GET /)
      if (request.method === 'GET' && (url.pathname === '/sse' || url.pathname === '/')) {
        debugLog('[GET /sse] SSE Request received');
        if (!isAuthorized(request, env)) {
          debugLog('[GET /sse] Unauthorized SSE request');
          return unauthorized();
        }

        return createSseResponse(request);
      }

      // Handle POST /sse
      if (request.method === 'POST' && url.pathname === '/sse') {
        debugLog('[POST /sse] Request received');
        if (!isAuthorized(request, env)) {
          debugLog('[POST /sse] Unauthorized request');
          return unauthorized();
        }

        let body: unknown = {};
        try {
          const text = await request.text();
          if (text.trim()) {
            body = JSON.parse(text);
          }
        } catch (err: unknown) {
          const message = err instanceof Error ? err.message : String(err);
          debugLog(`[POST /sse] Non-JSON or empty body accepted: ${message}`);
        }

        debugLog(`[POST /sse] Handled body: ${JSON.stringify(body)}`);

        return new Response(JSON.stringify({
          endpoint: "/messages",
          capabilities: {
            tools: { listChanged: true }
          },
        }), {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            ...CORS_HEADERS,
          },
        });
      }

      // 2. Handle Messages (POST /messages)
      if (request.method === 'POST' && url.pathname === '/messages') {
        debugLog('[POST /messages] Request received');
        if (!isAuthorized(request, env)) {
          debugLog('[POST /messages] Unauthorized request');
          return unauthorized();
        }

        const sessionId = url.searchParams.get('sessionId') || request.headers.get('Mcp-Session-Id');
        debugLog(`[POST /messages] sessionId (optional/stateless): ${sessionId || 'none'}`);

        try {
          const body = await request.json() as unknown;
          const responsePayload = await handleJsonRpcPayload(body, env);

          if (Array.isArray(responsePayload) && responsePayload.length === 0) {
            debugLog('[POST /messages] Notification batch processed without response');
            return new Response(null, {
              status: 202,
              headers: responseHeaders(sessionId),
            });
          }

          if (responsePayload === null) {
            debugLog('[POST /messages] Notification processed without response');
            return new Response(null, {
              status: 202,
              headers: responseHeaders(sessionId),
            });
          }

          debugLog(`[POST /messages] Returning stateless JSON-RPC response: ${JSON.stringify(responsePayload)}`);
          return jsonResponse(responsePayload, 200, sessionId ? { 'Mcp-Session-Id': sessionId } : {});
        } catch (err: unknown) {
          console.error('[POST /messages] Parse/Processing error:', err);
          return jsonResponse(
            makeJsonRpcError(null, -32700, 'Parse error: invalid JSON request body'),
            400,
            sessionId ? { 'Mcp-Session-Id': sessionId } : {},
          );
        }
      }

      console.log(`[GET/POST] Route not matched: ${request.method} ${url.pathname}`);
      return new Response('Not Found', {
        status: 404,
        headers: { ...CORS_HEADERS },
      });
    } catch (error: unknown) {
      console.error('[Fetch Handler] Fatal error:', error);
      const message = error instanceof Error ? error.message : 'Internal Server Error';
      return new Response(message, {
        status: 500,
        headers: { ...CORS_HEADERS },
      });
    }
  },
};
