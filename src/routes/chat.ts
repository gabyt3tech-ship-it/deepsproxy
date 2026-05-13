/*
 * File: chat.ts
 * Project: deepsproxy
 * Author: Pedro Farias
 * Created: 2026-05-09
 * 
 * Last Modified: Sat May 09 2026
 * Modified By: Pedro Farias
 */

import { Context } from 'hono';
import { stream as honoStream } from 'hono/streaming';
import { v4 as uuidv4 } from 'uuid';
import { createDeepSeekStream, updateSessionParent } from '../services/deepseek.ts';
import { OpenAIRequest, ChoiceDelta, Message } from '../utils/types.ts';
import { registry } from '../tools/registry.ts';
import type { FunctionToolDefinition } from '../tools/types.ts';

export async function chatCompletions(c: Context) {
  try {
    const body: OpenAIRequest = await c.req.json();
    const isStream = body.stream ?? false;
    
    // Extract the prompt
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
      } else if (i === messages.length - 1) {
        if (msg.role === 'user') {
          prompt += `User: ${contentStr}\n\n`;
        } else if (msg.role === 'assistant') {
          let assistantContent = contentStr;
          if ((msg as any).reasoning_content) {
            assistantContent = `<think>\n${(msg as any).reasoning_content}\n</think>\n${assistantContent}`;
          }
          if (msg.tool_calls && Array.isArray(msg.tool_calls)) {
             for (const tc of msg.tool_calls) {
               let args = tc.function?.arguments || '{}';
               if (typeof args !== 'string') args = JSON.stringify(args);
               assistantContent += `\n<tool_call>{"name": "${tc.function?.name}", "arguments": ${args}}</tool_call>`;
             }
          }
          prompt += `Assistant: ${assistantContent.trim()}\n\n`;
        } else if (msg.role === 'tool' || msg.role === 'function') {
          prompt += `Tool Response (${msg.name || 'tool'}): ${contentStr}\n\n`;
        }
      }
    }

    // Inject tools instructions
    const bodyAny = body as any;
    if (bodyAny.tools && Array.isArray(bodyAny.tools) && bodyAny.tools.length > 0) {
      // Better formatting for tools
      const formattedTools = bodyAny.tools.map((t: any) => {
        if (t.type === 'function') {
          return {
            name: t.function.name,
            description: t.function.description || '',
            parameters: t.function.parameters
          };
        }
        return t;
      });
      const toolsJson = JSON.stringify(formattedTools, null, 2);
      
      systemPrompt += `\n\n# TOOLS AVAILABLE\nYou have access to the following tools:\n${toolsJson}\n\nTo use a tool, you MUST output a JSON object wrapped EXACTLY in these tags:\n<tool_call>\n{"name": "tool_name", "arguments": {"param_name": "value"}}\n</tool_call>\n\nRULES:\n1. You can call multiple tools by outputting multiple <tool_call> blocks consecutively.\n2. Do NOT output any other text after your <tool_call> blocks. Wait for the user to provide the tool response.\n3. The JSON must be valid and accurately follow the tool's parameters.\n\n`;
      
      if (bodyAny.tool_choice && typeof bodyAny.tool_choice === 'object' && bodyAny.tool_choice.function) {
        const forcedTool = bodyAny.tool_choice.function.name;
        systemPrompt += `CRITICAL: You MUST call the tool "${forcedTool}" in this response.\n\n`;
      }
    }

    const finalPrompt = systemPrompt ? `${systemPrompt}\n${prompt}` : prompt;

    const isThinkingModel = !body.model.includes('no-thinking');
    
    // A session is new if it doesn't have any assistant messages yet.
    // This handles cases where the first request has [System, User] messages.
    const isNewSession = !messages.some(m => m.role === 'assistant');

    // Empty response retry logic
    let stream: ReadableStream;
    let uiSessionId = '';
    let retries = 3;
    while (retries > 0) {
      try {
        // If it's a new session, force parent_message_id to null
        const result = await createDeepSeekStream(finalPrompt, isThinkingModel, isNewSession ? null : undefined);
        stream = result.stream;
        uiSessionId = result.uiSessionId;
        break; // Success
      } catch (err: any) {
        retries--;
        if (retries === 0) throw err;
        // Wait a bit before retrying
        await new Promise(r => setTimeout(r, 1000));
      }
    }

    c.header('Content-Type', 'text/event-stream');
    c.header('Cache-Control', 'no-cache');
    c.header('Connection', 'keep-alive');

    const completionId = 'chatcmpl-' + uuidv4();

    return honoStream(c, async (streamWriter: any) => {
      const writeEvent = async (data: any) => {
        await streamWriter.write(`data: ${JSON.stringify(data)}\n\n`);
      };

      const makeChoice = (delta: any, finishReason: string | null = null) => ({
        index: 0,
        delta,
        logprobs: null,
        finish_reason: finishReason
      });

      // Send initial chunk
      await writeEvent({
        id: completionId,
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [makeChoice({ role: 'assistant', content: '' })]
      });

      const reader = stream.getReader();
      const decoder = new TextDecoder();
      
      let inThinkingState = false;
      let thinkingFragments: Record<string, boolean> = {};
      let currentFragIndex = 0;
      let currentAppendPath = '';
      
      let reasoningBuffer = '';
      let contentEmitBuffer = '';
      let insideTool = false;
      let emittedToolCallCount = 0;
      const TOOL_START = '<tool_call>';
      const TOOL_END = '</tool_call>';

      let buffer = '';
      let completionTokens = 0;
      const promptTokens = Math.ceil(finalPrompt.length / 3.5);

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
          if (dataStr === '[DONE]') {
            await streamWriter.write('data: [DONE]\n\n');
            continue;
          }

          try {
            const chunk = JSON.parse(dataStr);

            // Extract message_id for session tracking to avoid overwriting messages
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

            // Extract string value
            if (typeof chunk.v === 'string') {
              vStr = chunk.v;
              foundStr = true;
            } else if (chunk.v && typeof chunk.v === 'object') {
              // Handle old fragments format if it ever occurs
              if (chunk.v.response && chunk.v.response.fragments && chunk.v.response.fragments.length > 0) {
                const frag = chunk.v.response.fragments[0];
                if (typeof frag.content === 'string') {
                  vStr = frag.content;
                  foundStr = true;
                  currentAppendPath = frag.type === 'THINK' ? 'response/thinking_content' : 'response/content';
                }
              } else if (Array.isArray(chunk.v) && chunk.v.length > 0) {
                const firstObj = chunk.v[0];
                if (typeof firstObj.content === 'string') {
                  vStr = firstObj.content;
                  foundStr = true;
                  currentAppendPath = firstObj.type === 'THINK' ? 'response/thinking_content' : 'response/content';
                }
              }
            }

            // Determine if it's thinking based on the current path
            if (currentAppendPath.includes('thinking_content') || currentAppendPath.includes('THINK')) {
              isThinkingChunk = true;
            }

            if (foundStr && vStr !== '') {
              if (vStr === 'FINISHED') continue;

              const delta: ChoiceDelta = {};
              
              // Map chunk to either reasoning_content or content
              if (isThinkingChunk) {
                inThinkingState = true;
                reasoningBuffer += vStr;
                delta.reasoning_content = vStr;

                await writeEvent({
                  id: completionId,
                  object: 'chat.completion.chunk',
                  created: Math.floor(Date.now() / 1000),
                  model: body.model,
                  choices: [makeChoice(delta)]
                });
              } else {
                inThinkingState = false;
                contentEmitBuffer += vStr;

                while (contentEmitBuffer.length > 0) {
                  if (!insideTool) {
                    const startIdx = contentEmitBuffer.indexOf(TOOL_START);
                    if (startIdx !== -1) {
                      // Found tool start. Emit everything before it as text
                      const textToEmit = contentEmitBuffer.substring(0, startIdx);
                      if (textToEmit && emittedToolCallCount === 0) {
                        await writeEvent({
                          id: completionId,
                          object: 'chat.completion.chunk',
                          created: Math.floor(Date.now() / 1000),
                          model: body.model,
                          choices: [makeChoice({ content: textToEmit })]
                        });
                      }
                      insideTool = true;
                      contentEmitBuffer = contentEmitBuffer.substring(startIdx + TOOL_START.length);
                      continue; // re-evaluate loop for tool end
                    } else {
                      // No full start tag. Check for partial match at the end
                      let flushIndex = contentEmitBuffer.length;
                      for (let i = 1; i <= TOOL_START.length; i++) {
                        if (contentEmitBuffer.endsWith(TOOL_START.substring(0, i))) {
                          flushIndex = contentEmitBuffer.length - i;
                          break;
                        }
                      }
                      
                      const textToEmit = contentEmitBuffer.substring(0, flushIndex);
                      if (textToEmit && emittedToolCallCount === 0) {
                        await writeEvent({
                          id: completionId,
                          object: 'chat.completion.chunk',
                          created: Math.floor(Date.now() / 1000),
                          model: body.model,
                          choices: [makeChoice({ content: textToEmit })]
                        });
                      }
                      contentEmitBuffer = contentEmitBuffer.substring(flushIndex);
                      break; // wait for more chunks
                    }
                  } else {
                    // Inside tool
                    const endIdx = contentEmitBuffer.indexOf(TOOL_END);
                    if (endIdx !== -1) {
                      let toolJsonStr = contentEmitBuffer.substring(0, endIdx).trim();
                      try {
                        // Robust JSON sanitization
                        toolJsonStr = toolJsonStr.replace(/```json/g, '').replace(/```/g, '').trim();
                        const startJ = toolJsonStr.indexOf('{');
                        const endJ = toolJsonStr.lastIndexOf('}');
                        if (startJ !== -1 && endJ !== -1 && endJ >= startJ) {
                          toolJsonStr = toolJsonStr.substring(startJ, endJ + 1);
                        }

                        const toolCallObj = JSON.parse(toolJsonStr);
                        const toolId = 'call_' + uuidv4();
                        
                        await writeEvent({
                          id: completionId,
                          object: 'chat.completion.chunk',
                          created: Math.floor(Date.now() / 1000),
                          model: body.model,
                          choices: [makeChoice({
                            tool_calls: [{
                              index: emittedToolCallCount,
                              id: toolId,
                              type: 'function',
                              function: {
                                name: toolCallObj.name || '',
                                arguments: typeof toolCallObj.arguments === 'object'
                                  ? JSON.stringify(toolCallObj.arguments)
                                  : String(toolCallObj.arguments || '')
                              }
                            }]
                          })]
                        });
                        emittedToolCallCount++;
                      } catch (e) {
                        // Failed to parse tool call JSON, emit as regular text
                        if (emittedToolCallCount === 0) {
                          await writeEvent({
                            id: completionId,
                            object: 'chat.completion.chunk',
                            created: Math.floor(Date.now() / 1000),
                            model: body.model,
                            choices: [makeChoice({ content: TOOL_START + toolJsonStr + TOOL_END })]
                          });
                        }
                      }
                      
                      insideTool = false;
                      contentEmitBuffer = contentEmitBuffer.substring(endIdx + TOOL_END.length);
                    } else {
                      // Waiting for TOOL_END, buffer the content
                      break;
                    }
                  }
                }
              }
            }
          } catch (e) {
            // parse error, ignore partial chunk
          }
        }
      }

      // Flush any remaining content emit buffer
      if (!insideTool && contentEmitBuffer.length > 0 && emittedToolCallCount === 0) {
        await writeEvent({
          id: completionId,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: body.model,
          choices: [makeChoice({ content: contentEmitBuffer })]
        });
      }
  
      // Send finish reason
      const usage = {
        prompt_tokens: promptTokens,
        completion_tokens: completionTokens,
        total_tokens: promptTokens + completionTokens,
        prompt_tokens_details: {
          cached_tokens: 0 // Mock cache compatibility
        }
      };
  
      const finalFinishReason = emittedToolCallCount > 0 ? 'tool_calls' : 'stop';
  
      await writeEvent({
        id: completionId,
        object: 'chat.completion.chunk',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [makeChoice({}, finalFinishReason)],
        usage: usage
      });
      await streamWriter.write('data: [DONE]\n\n');

    });
  } catch (err: any) {
    console.error('Error in chatCompletions:', err);
    return c.json({ error: { message: err.message } }, 500);
  }
}
