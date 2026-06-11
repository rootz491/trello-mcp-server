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

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

interface Session {
  queue: string[];
}

// Global session tracking map (shared within the same isolate)
const sessions = new Map<string, Session>();

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

        const sessionId = crypto.randomUUID();
        debugLog(`[GET /sse] Created sessionId: ${sessionId}`);

        const session: Session = {
          queue: [],
        };
        sessions.set(sessionId, session);

        const encoder = new TextEncoder();

        // Periodically enqueue a keep-alive comment block
        const keepAliveIntervalId = setInterval(() => {
          if (sessions.has(sessionId)) {
            debugLog(`[Keep-Alive] Enqueuing ping for session: ${sessionId}`);
            session.queue.push(':\n\n');
          }
        }, 15000);

        const cleanUp = () => {
          debugLog(`[GET /sse] Cleanup running for session: ${sessionId}`);
          clearInterval(keepAliveIntervalId);
          sessions.delete(sessionId);
        };

        // Cleanup resources when client disconnects
        request.signal.addEventListener('abort', () => {
          debugLog(`[GET /sse] Connection aborted/closed by client for session: ${sessionId}`);
          cleanUp();
        });

        // Create a native ReadableStream using a pull() lifecycle with 100ms polling.
        // This ensures all controller.enqueue writes are executed by the runtime inside
        // the active GET request context, satisfying Cloudflare Workers request isolation
        // and avoiding cross-request context association issues.
        const stream = new ReadableStream({
          start(controller) {
            debugLog(`[Stream Start] Queueing initial endpoint for session: ${sessionId}`);
            const messageUrl = new URL(`/messages?sessionId=${sessionId}`, request.url).toString();
            const endpointMessage = `event: endpoint\ndata: ${messageUrl}\n\n`;
            controller.enqueue(encoder.encode(endpointMessage));
          },

          async pull(controller) {
            debugLog(`[Stream Pull] Pulling messages for session: ${sessionId}`);
            
            // Loop until we have enqueued at least one message or the session is terminated.
            // This prevents pull() from returning without enqueuing anything, which would
            // cause the runtime to stop calling pull() and hang the stream.
            while (sessions.has(sessionId) && !request.signal.aborted) {
              if (session.queue.length > 0) {
                while (session.queue.length > 0) {
                  const msg = session.queue.shift()!;
                  debugLog(`[Stream Pull] Dequeueing and enqueuing message to client: ${msg.trim()}`);
                  controller.enqueue(encoder.encode(msg));
                }
                return; // Success, we enqueued data
              }

              // Wait 100ms before checking the queue again
              await new Promise<void>((resolve) => {
                setTimeout(resolve, 100);
              });
            }
            debugLog(`[Stream Pull] Exited loop (session deleted or aborted) for session: ${sessionId}`);
          },

          cancel() {
            debugLog(`[Stream Cancel] Cancelled by client for session: ${sessionId}`);
            cleanUp();
          }
        });

        return new Response(stream, {
          headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            ...CORS_HEADERS,
          },
        });
      }

      // 2. Handle Messages (POST /messages)
      if (request.method === 'POST') {
        debugLog('[POST /messages] Request received');
        if (!isAuthorized(request, env)) {
          debugLog('[POST /messages] Unauthorized request');
          return unauthorized();
        }

        const sessionId = url.searchParams.get('sessionId');
        debugLog(`[POST /messages] sessionId from URL: ${sessionId}`);
        
        if (!sessionId) {
          debugLog('[POST /messages] Missing sessionId');
          return new Response('Missing sessionId query parameter', {
            status: 400,
            headers: { ...CORS_HEADERS },
          });
        }

        const session = sessions.get(sessionId);
        if (!session) {
          debugLog(`[POST /messages] Session ${sessionId} not found or expired`);
          return new Response(`Session ${sessionId} not found or expired`, {
            status: 400,
            headers: { ...CORS_HEADERS },
          });
        }

        try {
          const body = await request.json() as any;
          const { method, params, id } = body;
          debugLog(`[POST /messages] Received JSON-RPC method: "${method}", id: ${id}`);

          let result;
          let error;

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
              debugLog(`[POST /messages] Calling tool: "${toolName}" with args: ${JSON.stringify(toolArgs)}`);

              try {
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
                  error = { code: -32601, message: `Unknown tool: ${toolName}` };
                }

                if (!error) {
                  result = {
                    content: [{ type: 'text', text: JSON.stringify(data, null, 2) }],
                  };
                }
              } catch (err: any) {
                console.error('[POST /messages] Error in tool call:', err);
                error = { code: -32603, message: err.message || 'Error executing tool' };
              }
              break;

            default:
              error = { code: -32601, message: `Method not found: ${method}` };
          }

          // Build the JSON-RPC response payload
          const responsePayload: any = { jsonrpc: '2.0', id };
          if (error) {
            responsePayload.error = error;
          } else {
            responsePayload.result = result;
          }

          // Enqueue the message and let the pull check pick it up
          const sseMessage = `event: message\ndata: ${JSON.stringify(responsePayload)}\n\n`;
          debugLog(`[POST /messages] Enqueuing response: ${sseMessage.trim()}`);
          session.queue.push(sseMessage);

          // Acknowledge receipt of the POST message
          return new Response(null, {
            status: 202,
            headers: { ...CORS_HEADERS },
          });

        } catch (err: any) {
          console.error('[POST /messages] Parse/Processing error:', err);
          return new Response(JSON.stringify({ error: { code: -32603, message: err.message || 'Invalid request format' } }), {
            status: 400,
            headers: {
              'Content-Type': 'application/json',
              ...CORS_HEADERS,
            },
          });
        }
      }

      console.log(`[GET/POST] Route not matched: ${request.method} ${url.pathname}`);
      return new Response('Not Found', {
        status: 404,
        headers: { ...CORS_HEADERS },
      });
    } catch (error: any) {
      console.error('[Fetch Handler] Fatal error:', error);
      return new Response(error.message || 'Internal Server Error', {
        status: 500,
        headers: { ...CORS_HEADERS },
      });
    }
  },
};
