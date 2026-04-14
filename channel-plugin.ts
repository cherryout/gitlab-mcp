export interface OrchestrationMeta {
  correlation_key?: string;
  dedup_key?: string;
  entity_type?: string;
  entity_ref?: string;
  importance_hint?: string;
  source?: string;
  event_kind?: string;
  actor_ref?: string;
  title_hint?: string;
  source_ref?: string;
  thread_ref?: string;
}

export interface ChannelNotification {
  content: string;
  meta: Record<string, string>;
  orchestration?: OrchestrationMeta;
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

export interface WatchRegistration {
  watch_type: string;
  entity_type: string;
  entity_ref: string;
  correlation_key?: string;
  expires_at?: number;
}

export type OnWatchRegisteredFn = (watch: WatchRegistration) => void;

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
  setOnWatchRegistered?(fn: OnWatchRegisteredFn): void;
}
