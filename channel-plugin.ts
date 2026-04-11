export interface ChannelNotification {
  content: string;
  meta: Record<string, string>;
}

export type NotifyFn = (notification: ChannelNotification) => Promise<void>;

export interface ToolDef {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, unknown>;
    required?: string[];
  };
}

export interface ToolCallResult {
  content: Array<{ type: "text"; text: string }>;
  [key: string]: unknown;
}

export interface EventTypeDef {
  name: string;
  description: string;
}

export interface ChannelPlugin {
  readonly name: string;
  readonly tools: ToolDef[];
  readonly eventTypes: EventTypeDef[];
  init(notify: NotifyFn): Promise<void>;
  start(): Promise<void>;
  stop(): Promise<void>;
  handleToolCall(
    name: string,
    args: Record<string, unknown>,
  ): Promise<ToolCallResult | null>;
}
