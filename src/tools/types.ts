import type { JsonSchema, FunctionToolDefinition, ParsedToolCall, ToolCallResult } from '../types/openai.ts';

export type { JsonSchema, FunctionToolDefinition, ParsedToolCall, ToolCallResult };

export interface ToolRegistration {
  name: string;
  description: string;
  parameters: JsonSchema;
  strict: boolean;
  handler: ToolHandler;
}

export type ToolHandler = (
  args: Record<string, unknown>,
  context: ToolContext
) => Promise<unknown>;

export interface ToolContext {
  messages: unknown[];
  turn: number;
  model: string;
}
