import { v4 as uuidv4 } from "uuid";
import type {
  AgentState,
  AgentPhase,
  AgentConfig,
  AgentEvent,
  AgentEventListener,
  LLMAdapter,
  LLMResponse,
} from "./types.ts";
import type {
  Message,
  ParsedToolCall,
  ToolCallResult,
  FunctionToolDefinition,
} from "../types/openai.ts";
import { registry } from "../tools/registry.ts";
import { SchemaValidationError } from "../tools/schema.ts";
import { debug, debugError } from "../utils/debug.ts";

const TOOL_START_TAG = '<' + 'tool_call>';
const TOOL_END_TAG = '</' + 'tool_call>';

function createInitialState(
  model: string,
  stream: boolean,
  messages: Message[],
  tools: FunctionToolDefinition[],
  config: AgentConfig
): AgentState {
  const now = Date.now();
  return {
    phase: "idle",
    runId: uuidv4(),
    model,
    stream,
    messages: [...messages],
    tools,
    turn: 0,
    maxTurns: config.maxTurns ?? 10,
    pendingToolCalls: [],
    toolResults: [],
    finalContent: null,
    finalReasoning: null,
    finishReason: null,
    usage: {
      promptTokens: 0,
      completionTokens: 0,
      totalTokens: 0,
      cachedTokens: 0,
    },
    error: null,
    timestamps: {
      created: now,
      started: undefined,
      completed: undefined,
      lastTurnAt: undefined,
      erroredAt: undefined,
    },
    state: config.initialState ? { ...config.initialState } : {},
  };
}

function parseToolCallsFromContent(content: string): {
  textContent: string;
  toolCalls: ParsedToolCall[];
} {
  const toolCalls: ParsedToolCall[] = [];
  let remaining = content;
  let textContent = '';

  while (true) {
    const startIdx = remaining.indexOf(TOOL_START_TAG);
    if (startIdx === -1) {
      textContent += remaining;
      break;
    }

    textContent += remaining.substring(0, startIdx);

    const endIdx = remaining.indexOf(TOOL_END_TAG, startIdx + TOOL_START_TAG.length);
    if (endIdx === -1) {
      textContent += remaining.substring(startIdx);
      break;
    }

    const jsonStr = remaining
      .substring(startIdx + TOOL_START_TAG.length, endIdx)
      .trim();

    try {
      const parsed = JSON.parse(jsonStr);
      if (!parsed) throw new Error('Empty tool call');

      toolCalls.push({
        id: 'call_' + uuidv4(),
        name: parsed.name || '',
        arguments: typeof parsed.arguments === 'string'
          ? JSON.parse(parsed.arguments)
          : (parsed.arguments || {}),
      });
    } catch {
      textContent += TOOL_START_TAG + jsonStr + TOOL_END_TAG;
    }

    remaining = remaining.substring(endIdx + TOOL_END_TAG.length);
  }

  return { textContent: textContent.trim(), toolCalls };
}

function buildToolMessage(result: ToolCallResult): Message {
  return {
    role: 'tool',
    tool_call_id: result.toolCallId,
    content: result.result,
  };
}

function buildAssistantToolCallMessage(
  content: string | null,
  reasoningContent: string | null | undefined,
  toolCalls: ParsedToolCall[]
): Message {
  const msg: Message = {
    role: 'assistant',
    content: content || null,
    tool_calls: toolCalls.map((tc) => ({
      id: tc.id,
      type: 'function',
      function: {
        name: tc.name,
        arguments: typeof tc.arguments === 'string'
          ? tc.arguments
          : JSON.stringify(tc.arguments),
      },
    })),
  };
  if (reasoningContent) {
    msg.reasoning_content = reasoningContent;
  }
  return msg;
}

function buildFinalMessage(
  content: string | null,
  reasoningContent: string | null,
  toolCalls: ParsedToolCall[]
): Message {
  const msg: Message = { role: 'assistant', content: content || null };
  if (reasoningContent) {
    msg.reasoning_content = reasoningContent;
  }
  if (toolCalls.length > 0) {
    msg.tool_calls = toolCalls.map((tc) => ({
      id: tc.id,
      type: 'function',
      function: {
        name: tc.name,
        arguments: JSON.stringify(tc.arguments),
      },
    }));
  }
  return msg;
}

export class Agent {
  private state: AgentState;
  private llm: LLMAdapter;
  private listeners: AgentEventListener[] = [];
  private aborted = false;

  constructor(
    llm: LLMAdapter,
    model: string,
    messages: Message[],
    tools: FunctionToolDefinition[],
    config: AgentConfig = {},
    stream = false
  ) {
    this.llm = llm;
    this.state = createInitialState(model, stream, messages, tools, config);
  }

  getState(): Readonly<AgentState> {
    return this.state;
  }

  onEvent(listener: AgentEventListener): void {
    this.listeners.push(listener);
  }

  abort(): void {
    this.aborted = true;
    this.transition('aborted');
  }

  private emit(event: AgentEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (e) {
        debugError('Agent listener error:', e);
      }
    }
  }

  private transition(phase: AgentPhase): void {
    const from = this.state.phase;
    const timestamp = Date.now();
    this.state.phase = phase;

    if (phase === 'planning') {
      this.state.timestamps.started = timestamp;
    }
    if (phase === 'completed') {
      this.state.timestamps.completed = timestamp;
    }
    if (phase === 'error' || phase === 'aborted') {
      this.state.timestamps.erroredAt = timestamp;
    }

    this.emit({
      type: 'phase_change',
      from,
      to: phase,
      timestamp,
    });
  }

  private async executeToolCalls(
    toolCalls: ParsedToolCall[]
  ): Promise<ToolCallResult[]> {
    const results: ToolCallResult[] = [];

    for (const tc of toolCalls) {
      const toolStart = Date.now();
      this.emit({
        type: 'tool_start',
        turn: this.state.turn,
        toolName: tc.name,
        toolCallId: tc.id,
        timestamp: toolStart,
      });

      try {
        if (!registry.has(tc.name)) {
          results.push({
            toolCallId: tc.id,
            name: tc.name,
            result: JSON.stringify({ error: `Unknown tool: '${tc.name}'` }),
            isError: true,
          });
          continue;
        }

        const context = {
          messages: this.state.messages as unknown[],
          turn: this.state.turn,
          model: this.state.model,
        };
        const result = await registry.execute(tc.name, tc.arguments, context);
        results.push({
          toolCallId: tc.id,
          name: tc.name,
          result,
          isError: false,
        });
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        const isValidation = err instanceof SchemaValidationError;
        results.push({
          toolCallId: tc.id,
          name: tc.name,
          result: JSON.stringify({
            error: isValidation ? 'Schema validation failed' : 'Tool execution error',
            details: message,
            ...(isValidation ? { path: (err as SchemaValidationError).path } : {}),
          }),
          isError: true,
        });
      }

      this.emit({
        type: 'tool_end',
        turn: this.state.turn,
        toolName: tc.name,
        toolCallId: tc.id,
        isError: results[results.length - 1].isError,
        duration: Date.now() - toolStart,
        timestamp: Date.now(),
      });
    }

    return results;
  }

  private setError(code: string, message: string, cause?: unknown): void {
    this.state.error = {
      code,
      message,
      phase: this.state.phase,
      recoverable: false,
      cause,
    };
    this.transition('error');
  }

  private updateUsage(llmUsage?: Partial<{ promptTokens: number; completionTokens: number; cachedTokens: number }>): void {
    if (llmUsage) {
      if (llmUsage.promptTokens) this.state.usage.promptTokens += llmUsage.promptTokens;
      if (llmUsage.completionTokens) this.state.usage.completionTokens += llmUsage.completionTokens;
      if (llmUsage.cachedTokens) this.state.usage.cachedTokens += llmUsage.cachedTokens;
      this.state.usage.totalTokens =
        this.state.usage.promptTokens + this.state.usage.completionTokens;
    }
  }

  async run(): Promise<string> {
    const startTime = Date.now();
    this.transition('planning');

    try {
      return await this.runLoop(startTime);
    } catch (err) {
      this.setError('runtime_error', err instanceof Error ? err.message : String(err), err);
      throw err;
    }
  }

  private async runLoop(startTime: number): Promise<string> {
    this.transition('calling_llm');

    for (let turn = 0; turn < this.state.maxTurns; turn++) {
      if (this.aborted) {
        throw new Error('Agent aborted');
      }

      this.state.turn = turn;
      this.state.timestamps.lastTurnAt = Date.now();

      this.emit({
        type: 'llm_request',
        turn,
        messageCount: this.state.messages.length,
        timestamp: Date.now(),
      });

      this.transition('calling_llm');

      const toolsList = this.state.tools.length > 0 ? this.state.tools : undefined;

      let response: LLMResponse;
      try {
        response = await this.llm.complete(
          this.state.messages,
          toolsList,
          this.state.model
        );
      } catch (err) {
        this.setError('llm_error', `LLM call failed: ${err instanceof Error ? err.message : String(err)}`, err);
        throw err;
      }

      this.updateUsage(response.usage);
      this.state.finalReasoning = response.reasoning ?? null;

      this.emit({
        type: 'llm_response',
        turn,
        contentLength: response.content?.length ?? 0,
        toolCallCount: response.toolCalls.length,
        timestamp: Date.now(),
      });

      this.transition('parsing');

      const hasStructuredToolCalls = response.toolCalls.length > 0;
      let parsedFromContent: { textContent: string; toolCalls: ParsedToolCall[] } | null = null;

      if (!hasStructuredToolCalls && response.content) {
        parsedFromContent = parseToolCallsFromContent(response.content);
      }

      const effectiveToolCalls = hasStructuredToolCalls
        ? response.toolCalls
        : parsedFromContent?.toolCalls || [];

      const effectiveContent = parsedFromContent
        ? parsedFromContent.textContent
        : response.content;

      debug(`Agent turn ${turn}: content length=${effectiveContent?.length ?? 0}, tool calls=${effectiveToolCalls.length}`);

      if (effectiveToolCalls.length === 0) {
        this.state.finalContent = effectiveContent || '';
        this.state.finishReason = 'stop';
        this.state.timestamps.completed = Date.now();
        this.transition('completed');

        this.emit({
          type: 'completed',
          turn,
          totalTokens: this.state.usage.totalTokens,
          duration: Date.now() - startTime,
          timestamp: Date.now(),
        });

        return this.state.finalContent;
      }

      this.transition('executing');

      const toolResults = await this.executeToolCalls(effectiveToolCalls);

      this.state.messages.push(
        buildAssistantToolCallMessage(effectiveContent, response.reasoning, effectiveToolCalls)
      );

      for (const result of toolResults) {
        this.state.messages.push(buildToolMessage(result));
      }

      this.state.pendingToolCalls = effectiveToolCalls;
      this.state.toolResults = toolResults;
    }

    this.setError(
      'max_turns_exceeded',
      `Execution loop exceeded maximum turns (${this.state.maxTurns}). The agent may be stuck in a cycle.`
    );
    throw new Error(this.state.error!.message);
  }
}

export async function runAgent(
  llm: LLMAdapter,
  model: string,
  messages: Message[],
  tools: FunctionToolDefinition[],
  config: AgentConfig = {},
  stream = false
): Promise<string> {
  const agent = new Agent(llm, model, messages, tools, config, stream);
  return agent.run();
}
