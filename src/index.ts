/**
 * Cloudflare Worker MCP Server Boilerplate for Trello
 * 
 * Features:
 * - list_boards: List user boards
 * - get_lists: Get lists for a specific board
 * - list_cards: List cards in a specific list
 * - check_updates: Get recent actions for a board (activity feed)
 */

interface Env {
  TRELLO_API_KEY: string;
  TRELLO_TOKEN: string;
  MCP_AUTH_TOKEN: string;
}

function unauthorized(): Response {
  return new Response('Unauthorized', { status: 401 });
}

function isAuthorized(request: Request, env: Env): boolean {
  const auth = request.headers.get('Authorization');
  return auth === `Bearer ${env.MCP_AUTH_TOKEN}`;
}

const TRELLO_BASE_URL = 'https://api.trello.com/1';

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
    if (request.method !== 'POST') {
      return new Response('MCP Server: Use POST', { status: 405 });
    }

    if (!isAuthorized(request, env)) {
      return unauthorized();
    }

    try {
      const { method, params } = await request.json() as any;

      switch (method) {
        case 'initialize':
          return Response.json({
            protocolVersion: '2024-11-05',
            capabilities: {},
            serverInfo: { name: 'trello-mcp-worker', version: '1.0.0' },
          });

        case 'list_tools':
          return Response.json({
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
          });

        case 'call_tool':
          const toolName = params.name;
          const toolArgs = params.arguments || {};

          let result;
          if (toolName === 'list_boards') {
            result = await trelloFetch('/members/me/boards', env, { fields: 'name,url' });
          } else if (toolName === 'get_lists') {
            result = await trelloFetch(`/boards/${toolArgs.boardId}/lists`, env, { fields: 'name' });
          } else if (toolName === 'list_cards') {
            result = await trelloFetch(`/lists/${toolArgs.listId}/cards`, env, { fields: 'name,desc,url' });
          } else if (toolName === 'check_updates') {
            result = await trelloFetch(`/boards/${toolArgs.boardId}/actions`, env, { limit: toolArgs.limit?.toString() || '10' });
          } else {
            return Response.json({ error: `Unknown tool: ${toolName}` }, { status: 404 });
          }

          return Response.json({
            content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
          });

        default:
          return Response.json({ error: 'Method not found' }, { status: 404 });
      }
    } catch (err: any) {
      return Response.json({ error: err.message }, { status: 500 });
    }
  },
};
