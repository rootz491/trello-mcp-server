/**
 * Cloudflare Worker MCP Server for Trello with SSE Transport
 * 
 * Supports Poke's MCP requirements:
 * - GET /sse: Establish EventSource connection
 * - POST /messages: Receive JSON-RPC messages
 */

interface Env {
  TRELLO_API_KEY: string;
  TRELLO_TOKEN: string;
  MCP_AUTH_TOKEN?: string; // Kept for backward compatibility or optional use
}

const TRELLO_BASE_URL = 'https://api.trello.com/1';

function unauthorized(): Response {
  return new Response('Unauthorized', { status: 401 });
}

function isAuthorized(request: Request, env: Env): boolean {
  // If MCP_AUTH_TOKEN is set, enforce it.
  if (!env.MCP_AUTH_TOKEN) return true;
  const auth = request.headers.get('Authorization');
  return auth === `Bearer ${env.MCP_AUTH_TOKEN}`;
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

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // 1. Handle SSE Connection (GET /sse)
    if (request.method === 'GET' && (url.pathname === '/sse' || url.pathname === '/')) {
      // Security check for SSE if token is required
      if (!isAuthorized(request, env)) {
        // Note: SSE sometimes handles auth via query params or cookies if headers aren't possible,
        // but keeping consistency with the previous implementation's header-based auth.
        return unauthorized();
      }

      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      const encoder = new TextEncoder();

      // Send the endpoint event immediately so the client knows where to POST messages
      const messageUrl = new URL('/messages', request.url).toString();
      const endpointMessage = `event: endpoint\ndata: ${messageUrl}\n\n`;
      await writer.write(encoder.encode(endpointMessage));

      return new Response(readable, {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
          'Access-Control-Allow-Origin': '*',
        },
      });
    }

    // 2. Handle Messages (POST /messages)
    if (request.method === 'POST') {
      if (!isAuthorized(request, env)) {
        return unauthorized();
      }

      try {
        const body = await request.json() as any;
        const { method, params, id } = body;

        let result;
        switch (method) {
          case 'initialize':
            result = {
              protocolVersion: '2024-11-05',
              capabilities: {
                tools: {}
              },
              serverInfo: { name: 'trello-mcp-worker', version: '1.1.0' },
            };
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
            const toolName = params.name;
            const toolArgs = params.arguments || {};

            let data;
            if (toolName === 'list_boards') {
              data = await trelloFetch('/members/me/boards', env, { fields: 'name,url' });
            } else if (toolName === 'get_lists') {
              data = await trelloFetch(`/boards/${toolArgs.boardId}/lists`, env, { fields: 'name' });
            } else if (toolName === 'list_cards') {
              data = await trelloFetch(`/lists/${toolArgs.listId}/cards`, env, { fields: 'name,desc,url' });
            } else if (toolName === 'check_updates') {
              data = await trelloFetch(`/boards/${toolArgs.boardId}/actions`, env, { limit: toolArgs.limit?.toString() || '10' });
            } else {
              return Response.json({ error: { code: -32601, message: `Unknown tool: ${toolName}` }, id }, { status: 404 });
            }

            result = {
              content: [{ type: 'text', text: JSON.stringify(data, null, 2) }],
            };
            break;

          default:
            return Response.json({ error: { code: -32601, message: 'Method not found' }, id }, { status: 404 });
        }

        return Response.json({ jsonrpc: '2.0', result, id });

      } catch (err: any) {
        return Response.json({ error: { code: -32603, message: err.message } }, { status: 500 });
      }
    }

    return new Response('Not Found', { status: 404 });
  },
};
