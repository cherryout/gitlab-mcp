import type { ToolDef, ToolCallResult } from "./channel-plugin.js";
import type { Orchestrator } from "./orchestrator.js";

export function getOrchestratorTools(): ToolDef[] {
  return [
    {
      name: "add_watch",
      description: "Watch an entity (pipeline, MR, thread). Future events matching entity_ref or correlation_key will be routed to this session.",
      inputSchema: {
        type: "object",
        properties: {
          watch_type: { type: "string", description: "Watch type: pipeline-chain, deploy-chain, merge-request, mr-comments, slack-thread, telegram-thread, targetprocess-item, branch, task-followup, release-monitor" },
          entity_type: { type: "string", description: "Entity type: pipeline, merge_request, todo, build, slack_thread, slack_channel, telegram_thread, tp_item" },
          entity_ref: { type: "string", description: "Entity reference key (e.g. 'gitlab:123:pipeline:456')" },
          correlation_key: { type: "string", description: "Broader correlation key (e.g. 'gitlab:123:ref:main')" },
          expires_at: { type: "number", description: "Watch expiration timestamp (ms since epoch)" },
        },
        required: ["watch_type", "entity_type", "entity_ref"],
      },
    },
    {
      name: "remove_watch",
      description: "Cancel a watch by ID",
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
      description: "List active watches for the current session",
      inputSchema: {
        type: "object",
        properties: {},
      },
    },
    {
      name: "list_session_feed",
      description: "List events routed to this session (recent history)",
      inputSchema: {
        type: "object",
        properties: {
          limit: { type: "number", description: "Max items to return (default: 50)" },
          since: { type: "number", description: "Only items created after this timestamp" },
        },
      },
    },
    {
      name: "get_session_state",
      description: "Full session state: identity, runtime, active watches, pending count, recent events",
      inputSchema: {
        type: "object",
        properties: {},
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
      name: "ack_attention",
      description: "Acknowledge an attention item: marks the underlying delivery as acked, suppresses replay/auto-surface duplicates. Call this when you have processed a channel notification so the orchestrator knows it was actually surfaced.",
      inputSchema: {
        type: "object",
        properties: {
          attention_id: { type: "string", description: "Attention ID to acknowledge" },
        },
        required: ["attention_id"],
      },
    },
    {
      name: "find_dropped_notifications",
      description: "Find live notifications that were emitted to a session but never surfaced to Claude (delivered-live with no acked_at after grace period). Returns enriched rows with session, summary, source, event kind, and age. Cross-session by default; pass session_id to scope. Use this to detect black-hole drops across all orchestrator sessions.",
      inputSchema: {
        type: "object",
        properties: {
          session_id: { type: "string", description: "Filter by session (omit for cross-session sweep)" },
          grace_ms: { type: "number", description: "Only count drops older than this (default: 60000ms = 60s)" },
          limit: { type: "number", description: "Max rows (default 100, hard cap 1000)" },
        },
      },
    },
    {
      name: "get_audit_trail",
      description: "Query the cross-session orchestrator audit log. Use to investigate delivery/watch/runtime issues forensically. Filters AND together; pass any subset. Returns a chronological (newest-first) list of audit entries with category, action, outcome, related IDs, and structured detail. Categories: session, runtime, watch, event, delivery, attention, replay, tool_call, maintenance, notify.",
      inputSchema: {
        type: "object",
        properties: {
          since: { type: "number", description: "Lower bound timestamp (ms epoch)" },
          until: { type: "number", description: "Upper bound timestamp (ms epoch)" },
          session_id: { type: "string", description: "Filter by session" },
          category: { type: "string", description: "Filter by category" },
          action: { type: "string", description: "Filter by action" },
          delivery_id: { type: "string", description: "Filter by delivery_id" },
          attention_id: { type: "string", description: "Filter by attention_id" },
          event_id: { type: "string", description: "Filter by event_id" },
          watch_id: { type: "string", description: "Filter by watch_id" },
          limit: { type: "number", description: "Max rows (default 200, hard cap 5000)" },
        },
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

function safeParse(s: string): unknown {
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}

export async function handleOrchestratorToolCall(
  orchestrator: Orchestrator,
  name: string,
  args: Record<string, unknown>,
  activeSessionId: string,
): Promise<ToolCallResult | null> {
  try {
    switch (name) {
      case "add_watch":
        return ok(orchestrator.addWatch({
          session_id: activeSessionId,
          watch_type: args.watch_type as string,
          entity_type: args.entity_type as string,
          entity_ref: args.entity_ref as string,
          correlation_key: args.correlation_key as string | undefined,
          expires_at: args.expires_at as number | undefined,
        }));

      case "remove_watch":
        orchestrator.removeWatch(args.watch_id as string);
        return ok({ status: "cancelled", watch_id: args.watch_id });

      case "list_watches":
        return ok(orchestrator.listWatches(activeSessionId));

      case "list_session_feed":
        return ok(orchestrator.listSessionFeed(activeSessionId, {
          limit: args.limit as number | undefined,
          since: args.since as number | undefined,
        }));

      case "get_session_state":
        return ok(orchestrator.getSessionState(activeSessionId));

      case "list_unmatched_events":
        return ok(orchestrator.listUnmatchedEvents({
          limit: args.limit as number | undefined,
          since: args.since as number | undefined,
        }));

      case "ack_attention": {
        const attentionId = args.attention_id as string;
        orchestrator.ackAttention(attentionId);
        orchestrator.ackDeliveryByAttention(attentionId);
        return ok({ status: "acked", attention_id: attentionId });
      }

      case "find_dropped_notifications": {
        const items = orchestrator.findDroppedNotifications({
          sessionId: args.session_id as string | undefined,
          graceMs: args.grace_ms as number | undefined,
          limit: args.limit as number | undefined,
        });
        return ok({ count: items.length, items });
      }

      case "get_audit_trail": {
        const trail = orchestrator.getAuditTrail({
          since: args.since as number | undefined,
          until: args.until as number | undefined,
          sessionId: args.session_id as string | undefined,
          category: args.category as string | undefined,
          action: args.action as string | undefined,
          deliveryId: args.delivery_id as string | undefined,
          attentionId: args.attention_id as string | undefined,
          eventId: args.event_id as string | undefined,
          watchId: args.watch_id as string | undefined,
          limit: args.limit as number | undefined,
        });
        const parsed = trail.map((row) => ({
          ...row,
          detail: row.detail_json ? safeParse(row.detail_json) : null,
        }));
        return ok({ count: parsed.length, total: orchestrator.countAudit(), items: parsed });
      }

      default:
        return null;
    }
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return err(message);
  }
}
