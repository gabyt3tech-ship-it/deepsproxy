/*
 * File: index.ts
 * Project: deepsproxy
 * Author: Pedro Farias
 * Created: 2026-05-09
 * 
 * Last Modified: Sat May 09 2026
 * Modified By: Pedro Farias
 */

import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { chatCompletions } from './routes/chat.ts';
import * as dotenv from 'dotenv';
import { initPlaywright, activePage } from './services/playwright.ts';
import { debug, debugError } from './utils/debug.ts';

dotenv.config();
debug('Environment loaded');

export const app = new Hono();

app.use('*', cors());

app.use('*', async (c, next) => {
  const apiKey = process.env.API_KEY;
  if (apiKey) {
    const authHeader = c.req.header('Authorization');
    const xApiKey = c.req.header('X-API-Key');
    const providedKey = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : xApiKey;
    if (!providedKey || providedKey !== apiKey) {
      debug('Unauthorized request to', c.req.path);
      return c.json({ error: 'Unauthorized' }, 401);
    }
  }
  debug('Request:', c.req.method, c.req.path);
  await next();
});

// Health check with service status
app.get('/health', (c) => {
  return c.json({
    status: 'ok',
    services: {
      playwright: activePage !== null ? 'connected' : 'disconnected',
    },
    uptime: process.uptime(),
  });
});

// OpenAI compatible routes
app.post('/v1/chat/completions', chatCompletions);

app.get('/v1/models', (c) => {
  return c.json({
    object: 'list',
    data: [
      {
        id: 'deepseek-v4-flash',
        object: 'model',
        created: Math.floor(Date.now() / 1000),
        owned_by: 'deepseek',
        permission: [],
        root: 'deepseek-v4-flash',
        parent: null,
      },
      {
        id: 'deepseek-v4-flash-thinking',
        object: 'model',
        created: Math.floor(Date.now() / 1000),
        owned_by: 'deepseek',
        permission: [],
        root: 'deepseek-v4-flash-thinking',
        parent: null,
      },
      {
        id: 'deepseek-v4-pro',
        object: 'model',
        created: Math.floor(Date.now() / 1000),
        owned_by: 'deepseek',
        permission: [],
        root: 'deepseek-v4-pro',
        parent: null,
      },
      {
        id: 'deepseek-v4-pro-thinking',
        object: 'model',
        created: Math.floor(Date.now() / 1000),
        owned_by: 'deepseek',
        permission: [],
        root: 'deepseek-v4-pro-thinking',
        parent: null,
      }
    ]
  });
});

// Initialize playwright when server starts
import { fileURLToPath } from 'url';

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  initPlaywright().then(() => {
    debug('Playwright initialized.');
    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;
    debug(`Server is running on port ${port}`);
    console.log(`Server is running on port ${port}`);

    serve({
      fetch: app.fetch,
      port
    });
  }).catch((err: any) => {
    debugError('Failed to initialize playwright:', err);
    console.error('Failed to initialize playwright:', err);
    process.exit(1);
  });
}
