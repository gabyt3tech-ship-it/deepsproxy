/*
 * File: playwright.ts
 * Project: deepsproxy
 * Author: Pedro Farias
 * Created: 2026-05-09
 * 
 * Last Modified: Sat May 09 2026
 * Modified By: Pedro Farias
 */

import { chromium, BrowserContext, Page } from 'playwright';
import path from 'path';

let context: BrowserContext | null = null;
export let activePage: Page | null = null;
let currentHeaders: Record<string, string> = {};

export async function initPlaywright(headless = true) {
  if (process.env.TEST_MOCK_PLAYWRIGHT) return;
  if (context) {
    return;
  }

  const profilePath = path.resolve('deepseek_profile');
  
  context = await chromium.launchPersistentContext(profilePath, {
    headless,
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
  });

  // Keep an active page to fetch PoW headers on demand
  activePage = await context.newPage();
}

export async function closePlaywright() {
  if (process.env.TEST_MOCK_PLAYWRIGHT) return;
  if (context) {
    await context.close();
    context = null;
    activePage = null;
  }
}

/**
 * Ensures the session is valid and extracts headers, PoW, and session ID.
 */
export async function getDeepSeekHeaders(): Promise<{ headers: Record<string, string>, chatSessionId: string, parentMessageId: number | null }> {
  if (process.env.TEST_MOCK_PLAYWRIGHT) {
    // Generate a unique session ID if requested for testing isolation
    const mockSessionId = process.env.TEST_SESSION_ID || 'mock-session';
    return { headers: { authorization: 'Bearer MOCK' }, chatSessionId: mockSessionId, parentMessageId: null };
  }

  if (!activePage) {
    throw new Error('Playwright not initialized');
  }

  // Navigate if not on deepseek chat
  if (!activePage.url().includes('chat.deepseek.com')) {
    await activePage.goto('https://chat.deepseek.com/', { waitUntil: 'domcontentloaded' });
  }

  // Wait for the textarea
  await activePage.waitForSelector('textarea', { timeout: 30000 }).catch(() => {
    throw new Error('Timeout waiting for chat input. Are you logged in?');
  });

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timeout waiting for PoW headers')), 30000);

    const routeHandler = async (route: any, request: any) => {
      clearTimeout(timeout);
      
      const reqHeaders = request.headers();
      let uiSessionId = '';
      let uiParentMessageId: number | null = null;

      const postData = request.postData();
      if (postData) {
        try {
          const payload = JSON.parse(postData);
          if (payload.chat_session_id) {
            uiSessionId = payload.chat_session_id;
          }
          if (payload.parent_message_id !== undefined) {
            uiParentMessageId = payload.parent_message_id;
          }
        } catch (e) {
          // ignore parsing error
        }
      }

      const extractedHeaders = {
        'x-ds-pow-response': reqHeaders['x-ds-pow-response'] || '',
        'x-hif-dliq': reqHeaders['x-hif-dliq'] || '',
        'x-hif-leim': reqHeaders['x-hif-leim'] || '',
        'authorization': reqHeaders['authorization'] || '',
        'cookie': reqHeaders['cookie'] || ''
      };

      currentHeaders = extractedHeaders;

      // Abort to prevent polluting chat history
      await route.abort('aborted');
      
      // Cleanup route
      await activePage!.unroute('**/api/v0/chat/completion', routeHandler);

      resolve({ headers: extractedHeaders, chatSessionId: uiSessionId, parentMessageId: uiParentMessageId });
    };

    activePage!.route('**/api/v0/chat/completion', routeHandler).then(() => {
      // Trigger PoW generation by typing and hitting enter
      activePage!.fill('textarea', 'a').then(() => {
        activePage!.keyboard.press('Enter');
      });
    });
  });
}
