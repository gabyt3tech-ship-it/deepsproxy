/*
 * File: deepseek.ts
 * Project: deepsproxy
 * Author: Pedro Farias
 * Created: 2026-05-09
 * 
 * Last Modified: Sat May 09 2026
 * Modified By: Pedro Farias
 */

import { getDeepSeekHeaders } from './playwright.ts';
import { debug, debugError } from '../utils/debug.ts';

// In-memory state to track the last message ID per session to avoid overwriting
// Use globalThis to ensure it survives module reloads in some test environments
const sessionStates: Record<string, number | null> = (globalThis as any)._sessionStates || {};
(globalThis as any)._sessionStates = sessionStates;

export function updateSessionParent(sessionId: string, parentId: number | null) {
  if (sessionId) {
    debug(`updateSessionParent: session=${sessionId}, parentId=${parentId}`);
    sessionStates[sessionId] = parentId;
  }
}

export interface DeepSeekPayload {
  chat_session_id?: string;
  parent_message_id?: number | null;
  model_type: string | null;
  prompt: string;
  ref_file_ids: string[];
  thinking_enabled: boolean;
  search_enabled: boolean;
  preempt: boolean;
}

export async function createDeepSeekStream(
  prompt: string,
  enableThinking: boolean,
  isProModel: boolean = false,
  forcedParentId?: number | null
): Promise<{ stream: ReadableStream, headers: Record<string, string>, uiSessionId: string }> {
  // Obtain fresh headers/PoW from Playwright
  // If forcedParentId is null, it means we are explicitly starting a new session
  const { headers, chatSessionId, parentMessageId } = await getDeepSeekHeaders(forcedParentId === null);

  // Determine the actual parent ID:
  // 1. If forcedParentId is provided (even if null), use it.
  // 2. If tracked parent ID is available for this session, use it.
  // 3. Fallback to Playwright's state.
  let actualParentId: number | null = parentMessageId;
  
  if (forcedParentId !== undefined) {
    actualParentId = forcedParentId;
  } else if (chatSessionId && sessionStates[chatSessionId] !== undefined) {
    actualParentId = sessionStates[chatSessionId];
  }

  const payload: DeepSeekPayload = {
    chat_session_id: chatSessionId || undefined,
    parent_message_id: actualParentId,
    model_type: isProModel ? 'expert' : null,
    prompt: prompt,
    ref_file_ids: [],
    thinking_enabled: enableThinking,
    search_enabled: true,
    preempt: false
  };

  debug('Creating DeepSeek stream:', { model_type: payload.model_type, thinking_enabled: payload.thinking_enabled, sessionId: payload.chat_session_id });

  const DEEPSEEK_API = 'https://chat.deepseek.com/api/v0/chat/completion';
  const FETCH_TIMEOUT = 60_000;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(new DOMException('DeepSeek request timed out', 'TimeoutError')), FETCH_TIMEOUT);

  try {
    const response = await fetch(DEEPSEEK_API, {
      signal: controller.signal,
      method: 'POST',
      headers: {
        'accept': '*/*',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': headers['authorization'],
        'content-type': 'application/json',
        'origin': 'https://chat.deepseek.com',
        'x-ds-pow-response': headers['x-ds-pow-response'],
        'x-hif-dliq': headers['x-hif-dliq'],
        'x-hif-leim': headers['x-hif-leim'],
        'x-app-version': '2.0.0',
        'x-client-locale': 'pt_BR',
        'x-client-platform': 'web',
        'x-client-version': '2.0.0'
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok || !response.body) {
      const errText = await response.text().catch(() => '');
      debugError('DeepSeek API error:', response.status, response.statusText, errText);
      throw new Error(`DeepSeek API: ${response.status} ${response.statusText} - ${errText}`);
    }

    debug('DeepSeek stream created successfully, session:', chatSessionId);
    return { stream: response.body, headers, uiSessionId: chatSessionId };
  } finally {
    clearTimeout(timeoutId);
  }
}
