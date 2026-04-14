import type { ToolDef, ToolCallResult } from "./channel-plugin.js";
import type { Orchestrator } from "./orchestrator.js";

export function getOrchestratorTools(): ToolDef[] {
  return [
    {
      name: "register_session",
      description: "Register a new orchestrator session. Returns session_id for use with watches and feeds.",
      inputSchema: {
        type: "object",
        properties: {
          session_name: { type: "string", description: "Human-friendly session name" },
          owner: { type: "string", description: "Session owner (default: 'default')" },
          role: { type: "string", description: "Session role: main, inbox, bugfix, review, ops, release-watch, background-investigation" },
          workspace: { type: "string", description: "Workspace path" },
          project: { type: "string", description: "Project name" },
          repository: { type: "string", description: "Repository path" },
          branch_hint: { type: "string", description: "Current branch hint" },
        },
      },
    },
    {
      name: "close_session",
      description: "Archive a session. Watches are cancelled, pending deliveries expire.",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session ID to close" },
        },
        required: ["session_id"],
      },
    },
    {
      name: "attach_runtime",
      description: "Attach a live runtime to a session. Enables live event delivery. Triggers replay of pending deliveries.",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session to attach to" },
          runtime_id: { type: "string", description: "Runtime ID (auto-generated if omitted)" },
          channel_name: { type: "string", description: "Channel name for delivery" },
        },
        required: ["session_id"],
      },
    },
    {
      name: "detach_runtime",
      description: "Detach a runtime from its session. Future events will be queued instead of delivered live.",
      inputSchema: {
        type: "object",
        properties: {
          runtime_id: { type: "string", description: "Runtime ID to detach" },
        },
        required: ["runtime_id"],
      },
    },
    {
      name: "add_watch",
      description: "Add a watch to a session. Future events matching entity_ref or correlation_key will be routed to this session.",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session that owns this watch" },
          watch_type: { type: "string", description: "Watch type: pipeline-chain, deploy-chain, merge-request, mr-comments, slack-thread, telegram-thread, targetprocess-item, branch, task-followup, release-monitor" },
          entity_type: { type: "string", description: "Entity type: pipeline, merge_request, todo, build, slack_thread, slack_channel, telegram_thread, tp_item" },
          entity_ref: { type: "string", description: "Entity reference key (e.g. 'gitlab:123:pipeline:456')" },
          correlation_key: { type: "string", description: "Broader correlation key (e.g. 'gitlab:123:ref:main')" },
          delivery_policy: { type: "string", description: "Delivery policy: live-or-queue (default), live-only, queue-and-digest, critical-fallback" },
          fallback_policy: { type: "string", description: "Fallback target: main, ops, owner-default, backlog-only, ui-alert" },
          expires_at: { type: "number", description: "Watch expiration timestamp (ms since epoch)" },
        },
        required: ["session_id", "watch_type", "entity_type", "entity_ref"],
      },
    },
    {
      name: "remove_watch",
      description: "Cancel a watch",
      inputSchema: {
        type: "object",
        properties: {
          watch_id: { type: "string", description: "Watch ID to cancel" },
        },
        required: ["watch_id"],
      },
    },
    {
      name: "list_watches",
      description: "List active watches for a session",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session ID" },
        },
        required: ["session_id"],
      },
    },
    {
      name: "list_session_feed",
      description: "List attention items (events routed to this session)",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session ID" },
          limit: { type: "number", description: "Max items to return (default: 50)" },
          since: { type: "number", description: "Only items created after this timestamp" },
        },
        required: ["session_id"],
      },
    },
    {
      name: "list_pending_deliveries",
      description: "List queued deliveries waiting for session resume",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session ID" },
        },
        required: ["session_id"],
      },
    },
    {
      name: "ack_attention",
      description: "Acknowledge an attention item (mark as seen)",
      inputSchema: {
        type: "object",
        properties: {
          attention_id: { type: "string", description: "Attention item ID" },
        },
        required: ["attention_id"],
      },
    },
    {
      name: "snooze_attention",
      description: "Snooze an attention item until a specified time",
      inputSchema: {
        type: "object",
        properties: {
          attention_id: { type: "string", description: "Attention item ID" },
          until: { type: "number", description: "Snooze until this timestamp (ms since epoch)" },
        },
        required: ["attention_id", "until"],
      },
    },
    {
      name: "resolve_attention",
      description: "Mark an attention item as resolved (no further action needed)",
      inputSchema: {
        type: "object",
        properties: {
          attention_id: { type: "string", description: "Attention item ID" },
        },
        required: ["attention_id"],
      },
    },
    {
      name: "list_unmatched_events",
      description: "List recent events that matched no watch (useful for debugging routing)",
      inputSchema: {
        type: "object",
        properties: {
          limit: { type: "number", description: "Max items (default: 50)" },
          since: { type: "number", description: "Events after this timestamp" },
        },
      },
    },
    {
      name: "get_delivery_summary",
      description: "Get a summary of pending deliveries grouped by importance and source",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session ID" },
        },
        required: ["session_id"],
      },
    },
    {
      name: "get_session_state",
      description: "Get full session state: identity, runtime, active watches, pending count, recent attention",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Session ID" },
        },
        required: ["session_id"],
      },
    },
  ];
}

function ok(data: unknown): ToolCallResult {
  return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
}

function err(message: string): ToolCallResult {
  return { content: [{ type: "text", text: `Error: ${message}` }] };
}

export async function handleOrchestratorToolCall(
  orchestrator: Orchestrator,
  name: string,
  args: Record<string, unknown>,
): Promise<ToolCallResult | null> {
  try {
    switch (name) {
      case "register_session":
        return ok(orchestrator.registerSession(args as {
          session_name?: string; owner?: string; role?: string;
          workspace?: string; project?: string; repository?: string; branch_hint?: string;
        }));

      case "close_session":
        orchestrator.closeSession(args.session_id as string);
        return ok({ status: "closed", session_id: args.session_id });

      case "attach_runtime":
        return ok(orchestrator.attachRuntime(
          args.session_id as string,
          args.runtime_id as string | undefined,
          args.channel_name as string | undefined,
        ));

      case "detach_runtime":
        orchestrator.detachRuntime(args.runtime_id as string);
        return ok({ status: "detached", runtime_id: args.runtime_id });

      case "add_watch":
        return ok(orchestrator.addWatch(args as {
          session_id: string; watch_type: string; entity_type: string; entity_ref: string;
          correlation_key?: string; delivery_policy?: string; fallback_policy?: string;
          expires_at?: number; metadata_json?: string;
        }));

      case "remove_watch":
        orchestrator.removeWatch(args.watch_id as string);
        return ok({ status: "cancelled", watch_id: args.watch_id });

      case "list_watches":
        return ok(orchestrator.listWatches(args.session_id as string));

      case "list_session_feed":
        return ok(orchestrator.listSessionFeed(args.session_id as string, {
          limit: args.limit as number | undefined,
          since: args.since as number | undefined,
        }));

      case "list_pending_deliveries":
        return ok(orchestrator.listPendingDeliveries(args.session_id as string));

      case "ack_attention":
        orchestrator.ackAttention(args.attention_id as string);
        return ok({ status: "acked", attention_id: args.attention_id });

      case "snooze_attention":
        orchestrator.snoozeAttention(args.attention_id as string, args.until as number);
        return ok({ status: "snoozed", attention_id: args.attention_id, until: args.until });

      case "resolve_attention":
        orchestrator.resolveAttention(args.attention_id as string);
        return ok({ status: "resolved", attention_id: args.attention_id });

      case "list_unmatched_events":
        return ok(orchestrator.listUnmatchedEvents({
          limit: args.limit as number | undefined,
          since: args.since as number | undefined,
        }));

      case "get_delivery_summary":
        return ok(orchestrator.getDeliverySummary(args.session_id as string));

      case "get_session_state":
        return ok(orchestrator.getSessionState(args.session_id as string));

      default:
        return null;
    }
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return err(message);
  }
}
