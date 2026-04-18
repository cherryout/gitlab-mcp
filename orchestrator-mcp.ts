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

      default:
        return null;
    }
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    return err(message);
  }
}
