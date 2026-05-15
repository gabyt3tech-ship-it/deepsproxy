import { Context } from 'hono';
import { stream as honoStream } from 'hono/streaming';
import { v4 as uuidv4 } from 'uuid';
import { createDeepSeekStream, updateSessionParent } from '../services/deepseek.ts';
import { OpenAIRequest, ChoiceDelta } from '../types/openai.ts';
import { robustParseJSON } from '../utils/json.ts';
import { debug, debugError } from '../utils/debug.ts';

interface StreamResult {
  content: string;
  reasoningContent: string;
  toolCalls: { name: string; arguments: Record<string, unknown>; id: string }[];
  completionTokens: number;
}

const TOOL_START = '<' + 'tool_call>';
const TOOL_END = '</' + 'tool_call>';

async function readDeepSeekStream(
  stream: ReadableStream,
  uiSessionId: string,
  onChunk?: (delta: ChoiceDelta, index: number) => void | Promise<void>
): Promise<StreamResult> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();

  let contentEmitBuffer = '';
  let reasoningBuffer = '';
  let insideTool = false;
  let emittedToolCallCount = 0;

  let buffer = '';
  let completionTokens = 0;
  let currentAppendPath = '';
  let currentFragmentType = '';
  const toolCalls: { name: string; arguments: Record<string, unknown>; id: string }[] = [];

  const emit = (delta: ChoiceDelta, index: number) => {
    if (onChunk) {
      const result = onChunk(delta, index);
      if (result && typeof (result as any).then === 'function') {
        return result;
      }
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || !trimmed.startsWith('data: ')) continue;

      const dataStr = trimmed.slice(6);
      if (dataStr === '[DONE]') continue;

      try {
        const chunk = JSON.parse(dataStr);

        let dsMessageId: any = null;
        if (chunk.response_message_id) {
          dsMessageId = chunk.response_message_id;
        } else if (chunk.v && typeof chunk.v === 'object') {
          if (chunk.v.response && chunk.v.response.message_id) {
            dsMessageId = chunk.v.response.message_id;
          } else if (chunk.v.message_id) {
            dsMessageId = chunk.v.message_id;
          }
        } else if (chunk.message_id) {
          dsMessageId = chunk.message_id;
        }

        if (dsMessageId) {
          updateSessionParent(uiSessionId, dsMessageId);
        }

        let vStr = '';
        let foundStr = false;
        let isThinkingChunk = false;

        if (typeof chunk.p === 'string') {
          currentAppendPath = chunk.p;
          if (chunk.p === 'response/accumulated_token_usage' && typeof chunk.v === 'number') {
            completionTokens = chunk.v;
          }
        }

        if (typeof chunk.v === 'string') {
          vStr = chunk.v;
          foundStr = true;
        } else if (chunk.v && typeof chunk.v === 'object') {
          if (chunk.v.response && chunk.v.response.fragments && chunk.v.response.fragments.length > 0) {
            const frag = chunk.v.response.fragments[0];
            if (typeof frag.content === 'string') {
              vStr = frag.content;
              foundStr = true;
              currentAppendPath = frag.type === 'THINK' ? 'response/thinking_content' : 'response/content';
              currentFragmentType = frag.type || '';
            }
          } else if (Array.isArray(chunk.v) && chunk.v.length > 0) {
            const firstObj = chunk.v[0];
            if (typeof firstObj.content === 'string') {
              vStr = firstObj.content;
              foundStr = true;
              currentAppendPath = firstObj.type === 'THINK' ? 'response/thinking_content' : 'response/content';
              currentFragmentType = firstObj.type || '';
            }
          }
        }

        if (chunk.p === 'response/fragments' && Array.isArray(chunk.v)) {
          const lastFrag = chunk.v[chunk.v.length - 1];
          if (lastFrag && lastFrag.type) {
            currentFragmentType = lastFrag.type;
          }
        }

        if (currentAppendPath.includes('thinking_content') ||
            currentAppendPath.includes('THINK') ||
            (currentAppendPath.includes('fragments/-1/content') && currentFragmentType === 'THINK')) {
          isThinkingChunk = true;
        }

        if (foundStr && vStr !== '') {
          if (vStr === 'FINISHED') continue;

          if (isThinkingChunk) {
            reasoningBuffer += vStr;
            await emit({ reasoning_content: vStr }, emittedToolCallCount);
          } else {
            contentEmitBuffer += vStr;

            while (contentEmitBuffer.length > 0) {
              if (!insideTool) {
                const startIdx = contentEmitBuffer.indexOf(TOOL_START);
                if (startIdx !== -1) {
                  const textToEmit = contentEmitBuffer.substring(0, startIdx);
                  if (textToEmit && emittedToolCallCount === 0) {
                    await emit({ content: textToEmit }, emittedToolCallCount);
                  }
                  insideTool = true;
                  contentEmitBuffer = contentEmitBuffer.substring(startIdx + TOOL_START.length);
                  continue;
                } else {
                  let flushIndex = contentEmitBuffer.length;
                  for (let i = 1; i <= TOOL_START.length; i++) {
                    if (contentEmitBuffer.endsWith(TOOL_START.substring(0, i))) {
                      flushIndex = contentEmitBuffer.length - i;
                      break;
                    }
                  }

                  const textToEmit = contentEmitBuffer.substring(0, flushIndex);
                  if (textToEmit && emittedToolCallCount === 0) {
                    await emit({ content: textToEmit }, emittedToolCallCount);
                  }
                  contentEmitBuffer = contentEmitBuffer.substring(flushIndex);
                  break;
                }
              } else {
                const endIdx = contentEmitBuffer.indexOf(TOOL_END);
                if (endIdx !== -1) {
                  const toolJsonStr = contentEmitBuffer.substring(0, endIdx).trim();

                  try {
                    const toolCallObj = robustParseJSON(toolJsonStr);

                    if (toolCallObj) {
                      const nameMatch = toolJsonStr.match(/<tool_call\s+name="([^"]+)"/);
                      const toolName = nameMatch ? nameMatch[1] : toolCallObj.name || '';

                      let toolArgs: Record<string, unknown> = {};
                      if (toolCallObj.arguments && typeof toolCallObj.arguments === 'object') {
                        toolArgs = toolCallObj.arguments;
                      } else {
                        const keys = Object.keys(toolCallObj).filter(k => k !== 'name');
                        for (const k of keys) {
                          toolArgs[k] = toolCallObj[k];
                        }
                      }

                      const toolId = 'call_' + uuidv4();
                      toolCalls.push({ name: toolName, arguments: toolArgs, id: toolId });

                      await emit({
                        role: 'assistant',
                        content: null,
                        tool_calls: [{
                          index: emittedToolCallCount,
                          id: toolId,
                          type: 'function',
                          function: {
                            name: toolName,
                            arguments: JSON.stringify(toolArgs),
                          },
                        }],
                      }, emittedToolCallCount);
                      emittedToolCallCount++;
                    }
                  } catch (e) {
                    if (emittedToolCallCount === 0) {
                      await emit({ content: TOOL_START + toolJsonStr + TOOL_END }, emittedToolCallCount);
                    }
                  }

                  insideTool = false;
                  contentEmitBuffer = contentEmitBuffer.substring(endIdx + TOOL_END.length);
                } else {
                  break;
                }
              }
            }
          }
        }
      } catch (e) {
        debug('Ignored partial chunk parse error');
      }
    }
  }

  // Flush remaining content
  if (!insideTool && contentEmitBuffer.length > 0 && emittedToolCallCount === 0) {
    await emit({ content: contentEmitBuffer }, emittedToolCallCount);
  }

  return {
    content: contentEmitBuffer,
    reasoningContent: reasoningBuffer,
    toolCalls,
    completionTokens,
  };
}

export async function chatCompletions(c: Context) {
  try {
    const body: OpenAIRequest = await c.req.json();

    if (!body.model || typeof body.model !== 'string') {
      return c.json({ error: { message: 'model is required and must be a string' } }, 400);
    }
    if (!body.messages || !Array.isArray(body.messages) || body.messages.length === 0) {
      return c.json({ error: { message: 'messages is required and must be a non-empty array' } }, 400);
    }

    debug('Request body model:', body.model, 'stream:', body.stream, 'messages count:', body.messages.length);
    const isStream = body.stream ?? false;

    let prompt = '';
    const messages = body.messages || [];
    let systemPrompt = '';

    for (let i = 0; i < messages.length; i++) {
      const msg = messages[i];
      let contentStr = '';
      if (Array.isArray(msg.content)) {
        contentStr = msg.content.map((c: any) => c.text || JSON.stringify(c)).join('\n');
      } else if (typeof msg.content === 'object' && msg.content !== null) {
        contentStr = JSON.stringify(msg.content);
      } else {
        contentStr = msg.content || '';
      }

      if (msg.role === 'system') {
        systemPrompt += contentStr + '\n\n';
      } else if (msg.role === 'user') {
        prompt += `User: ${contentStr}\n\n`;
      } else if (msg.role === 'assistant') {
        let assistantContent = contentStr;
        if ((msg as any).reasoning_content) {
          assistantContent = `\n${(msg as any).reasoning_content}\n\n${assistantContent}`;
        }
        if (msg.tool_calls && Array.isArray(msg.tool_calls)) {
          for (const tc of msg.tool_calls) {
            let args = tc.function?.arguments || '{}';
            if (typeof args !== 'string') args = JSON.stringify(args);
            assistantContent += `\n<tool_call>{"name": "${tc.function?.name}", "arguments": ${args}}</tool_call>\n`;
          }
        }
        prompt += `Assistant: ${assistantContent.trim()}\n\n`;
      } else if (msg.role === 'tool' || msg.role === 'function') {
        prompt += `Tool Response (${msg.name || 'tool'}): ${contentStr}\n\n`;
      }
    }

    if (body.tools && body.tools.length > 0) {
      const formattedTools = body.tools.map((t) => {
        if (t.type === 'function') {
          return {
            name: t.function.name,
            description: t.function.description || '',
            parameters: t.function.parameters,
          };
        }
        return t;
      });
      const toolsJson = JSON.stringify(formattedTools, null, 2);

      const toolNames = formattedTools.map((t: any) => `"${t.name}"`).join(', ');

      systemPrompt += `\n\n## Tools

You have access to these tools. When a task requires using a tool, call it using the format below. If the task is simple (answer a question, give an explanation), respond directly.

### Tool Definitions

\`\`\`json
${toolsJson}
\`\`\`

### How to call a tool

Output EXACTLY this format (no extra text, no markdown code blocks):

<tool_call>{"name": "tool_name", "arguments": {"param_name": "value"}}</tool_call>

### When to call

- Information gathering, file operations, analysis, code changes → call the right tool
- Simple Q&A, explanations → respond directly
- Multiple tools needed → call them one after another
- If unsure, prefer using a tool

### Rules

1. Valid JSON with "name" and "arguments" only
2. Arguments must match the tool's parameter schema
3. After calling a tool, wait for the result
4. NEVER wrap tool calls in markdown code blocks or add text around them\n\n`;

      const toolChoice = body.tool_choice;
      if (toolChoice && typeof toolChoice === 'object' && 'function' in toolChoice) {
        const forcedTool = (toolChoice as { function: { name: string } }).function.name;
        systemPrompt += `CRITICAL: You MUST call the tool "${forcedTool}" in this response. Output only the <tool_call> block.\n\n`;
      }
    }

    const finalPrompt = systemPrompt ? `${systemPrompt}\n${prompt}` : prompt;

    const isThinkingModel = body.model.includes('thinking');
    const isProModel = body.model.includes('pro');
    const isNewSession = !messages.some(m => m.role === 'assistant');

    debug('Final prompt length:', finalPrompt.length, 'isThinking:', isThinkingModel, 'isPro:', isProModel, 'isNewSession:', isNewSession);

    let deepSeekStream: ReadableStream;
    let uiSessionId = '';
    const MAX_RETRIES = 3;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const result = await createDeepSeekStream(finalPrompt, isThinkingModel, isProModel, isNewSession ? null : undefined);
        deepSeekStream = result.stream;
        uiSessionId = result.uiSessionId;
        break;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        debugError(`DeepSeek attempt ${attempt}/${MAX_RETRIES} failed:`, message);
        if (attempt === MAX_RETRIES) throw err;
        const delay = Math.min(1000 * Math.pow(2, attempt - 1) + Math.random() * 500, 10000);
        await new Promise(r => setTimeout(r, delay));
      }
    }

    const completionId = 'chatcmpl-' + uuidv4();
    const promptTokens = Math.ceil(finalPrompt.length / 3.5);

    // === NON-STREAM MODE ===
    if (!isStream) {
      let fullContent = '';
      let fullReasoning = '';

      const result = await readDeepSeekStream(
        deepSeekStream!,
        uiSessionId,
        (delta) => {
          if (delta.reasoning_content) {
            fullReasoning += delta.reasoning_content;
          }
          if (delta.content) {
            fullContent += delta.content;
          }
        }
      );

      const hasToolCalls = result.toolCalls.length > 0;
      const message: any = { role: 'assistant' };

      if (result.reasoningContent) {
        message.reasoning_content = result.reasoningContent;
      }

      if (hasToolCalls) {
        message.content = fullContent || null;
        message.tool_calls = result.toolCalls.map((tc) => ({
          id: tc.id,
          type: 'function',
          function: {
            name: tc.name,
            arguments: JSON.stringify(tc.arguments),
          },
        }));
      } else {
        message.content = fullContent;
      }

      return c.json({
        id: completionId,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [{
          index: 0,
          message,
          logprobs: null,
          finish_reason: hasToolCalls ? 'tool_calls' : 'stop',
        }],
        usage: {
          prompt_tokens: promptTokens,
          completion_tokens: result.completionTokens,
          total_tokens: promptTokens + result.completionTokens,
          prompt_tokens_details: { cached_tokens: 0 },
        },
      });
    }

    // === STREAM MODE ===
    c.header('Content-Type', 'text/event-stream');
    c.header('Cache-Control', 'no-cache');
    c.header('Connection', 'keep-alive');

    return honoStream(c, async (streamWriter: any) => {
      const writeEvent = async (data: any) => {
        await streamWriter.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      const makeChoice = (delta: any, finishReason: string | null = null) => ({
        index: 0,
        delta,
        logprobs: null,
        finish_reason: finishReason,
      });

      await writeEvent({
        id: completionId,
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [makeChoice({ role: 'assistant', content: '' })],
      });

      let emittedToolCallCount = 0;

      const streamResult = await readDeepSeekStream(
        deepSeekStream!,
        uiSessionId,
        async (delta) => {
          if (delta.tool_calls) {
            emittedToolCallCount++;
          }
          await writeEvent({
            id: completionId,
            object: 'chat.completion.chunk',
            created: Math.floor(Date.now() / 1000),
            model: body.model,
            choices: [{
              index: (delta.tool_calls && delta.tool_calls[0]) ? delta.tool_calls[0].index : 0,
              delta,
              logprobs: null,
              finish_reason: null,
            }],
          });
        }
      );

      await writeEvent({
        id: completionId,
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [makeChoice({}, emittedToolCallCount > 0 ? 'tool_calls' : 'stop')],
        usage: {
          prompt_tokens: promptTokens,
          completion_tokens: streamResult.completionTokens,
          total_tokens: promptTokens + streamResult.completionTokens,
          prompt_tokens_details: { cached_tokens: 0 },
        },
      });

      await streamWriter.write('data: [DONE]\n\n');
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    debugError('Error in chatCompletions:', message);
    return c.json({ error: { message } }, 500);
  }
}
