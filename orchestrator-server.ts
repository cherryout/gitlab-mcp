#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { pino } from "pino";
import type {
  ChannelPlugin,
  ChannelNotification,
} from "./channel-plugin.js";
import { Orchestrator } from "./orchestrator.js";
import { getOrchestratorTools, handleOrchestratorToolCall } from "./orchestrator-mcp.js";

// ─── Configuration ───────────────────────────────────────────────────

const PLUGIN_MAP: Record<string, () => Promise<ChannelPlugin>> = {
  gitlab: async () => {
    const { GitLabChannelPlugin } = await import("./channels/gitlab-plugin.js");
    return new GitLabChannelPlugin();
  },
  slack: async () => {
    const { SlackChannelPlugin } = await import("./channels/slack-plugin.js");
    return new SlackChannelPlugin();
  },
  jenkins: async () => {
    const { JenkinsChannelPlugin } = await import("./channels/jenkins-plugin.js");
    return new JenkinsChannelPlugin();
  },
};

const LOG_DIR = join(homedir(), ".cache", "orchestrator");
mkdirSync(LOG_DIR, { recursive: true });
const LOG_PATH = process.env.ORCHESTRATOR_LOG_PATH || join(LOG_DIR, "orchestrator.log");
const LOG_LEVEL = process.env.ORCHESTRATOR_LOG_LEVEL || "info";

const logger = pino(
  { level: LOG_LEVEL },
  pino.destination({ dest: LOG_PATH, sync: false }),
);

const pluginNames = (process.env.CHANNEL_PLUGINS || "gitlab")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const autoStart = (process.env.ORCHESTRATOR_AUTO_START || "true") === "true";
const SESSION_OWNER = process.env.ORCHESTRATOR_SESSION_OWNER || "default";
const SESSION_ROLE = process.env.ORCHESTRATOR_SESSION_ROLE || "main";

logger.info({ plugins: pluginNames, autoStart }, "orchestrator starting");

// ─── Orchestrator Core ───────────────────────────────────────────────

const orchestrator = new Orchestrator({ logger });

let activeSessionId: string | null = null;
let activeRuntimeId: string | null = null;

// ─── Plugin State ────────────────────────────────────────────────────

interface PluginEntry {
  plugin: ChannelPlugin;
  active: boolean;
}

const registry = new Map<string, PluginEntry>();

for (const name of pluginNames) {
  const factory = PLUGIN_MAP[name];
  if (!factory) {
    logger.error({ name }, "unknown plugin");
    throw new Error(`Unknown channel plugin: ${name}`);
  }
  const plugin = await factory();
  registry.set(name, { plugin, active: false });
  logger.info({ name }, "plugin loaded");
}

// ─── MCP Server ──────────────────────────────────────────────────────

const pluginList = [...registry.values()].map((e) => e.plugin);

const mcp = new Server(
  { name: "orchestrator", version: "0.1.0" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `Session orchestrator with source adapters: ${pluginNames.join(", ")}.
Events arrive as <channel source="orchestrator" plugin="..." event_type="..." ...>.
Events are diff-based — you only receive CHANGES.

Orchestrator tools (session & watch management):
- add_watch: watch an entity (pipeline, MR, thread) — matching events route to your session
- remove_watch / list_watches: manage watches
- list_session_feed: see attention items routed to this session
- list_pending_deliveries / get_delivery_summary: see queued events
- ack_attention / snooze_attention / resolve_attention: manage attention lifecycle
- get_session_state: full session state dump
- list_unmatched_events: events that matched no watch
- get_audit_trail: cross-session forensic audit log — filter by session, category, action, delivery_id, attention_id, event_id, watch_id, time range. Use this to investigate any delivery/watch/runtime issue ("why didn't I get notified", "what happened to delivery X", "trace event Y end-to-end").

Session is auto-managed: created on first startup, resumed on reconnect.
Watches survive session restarts — events queue while you're away, replay on resume.

Source plugins (auto-started):
${pluginList.map((p) => `[${p.name}]\n  events: ${p.eventTypes.map((e) => e.name).join(", ")}\n  tools: ${p.tools.map((t) => t.name).join(", ")}`).join("\n")}`,
  },
);

// ─── Notification Forwarding (Orchestration Channel) ─────────────────

orchestrator.setNotifyCallback(async (sessionId, content, meta) => {
  await mcp.notification({
    method: "notifications/claude/channel",
    params: {
      content,
      meta: { ...meta, orchestrated: "true", session_id: sessionId },
    },
  });
});

const makeNotify =
  (entry: PluginEntry) =>
  async (n: ChannelNotification): Promise<void> => {
    if (!entry.active) return;

    const result = orchestrator.ingestEvent(n);
    logger.debug(
      { plugin: entry.plugin.name, eventType: n.meta.event_type, eventId: result.eventId, deduplicated: result.deduplicated, matchedWatches: result.matchedWatches },
      "event ingested",
    );

    if (n.orchestration?.event_kind === "pipeline_watch_completed" && n.orchestration.entity_ref) {
      orchestrator.completeWatchesByEntity("pipeline", n.orchestration.entity_ref);
      if (n.orchestration.correlation_key) {
        orchestrator.completeWatchesByEntity("pipeline", n.orchestration.correlation_key);
      }
    }

    // No watch = no delivery. Session gets only what it explicitly watches.
  };

for (const [, entry] of registry) {
  await entry.plugin.init(makeNotify(entry));
  if (entry.plugin.setOnWatchRegistered) {
    entry.plugin.setOnWatchRegistered((watch) => {
      if (!activeSessionId) return;
      orchestrator.addWatch({
        session_id: activeSessionId,
        ...watch,
      });
      logger.info({ plugin: entry.plugin.name, entityRef: watch.entity_ref }, "orchestrator watch created from plugin tool");
    });
  }
  logger.info({ name: entry.plugin.name, tools: entry.plugin.tools.length }, "plugin initialized");
}

// ─── Tool Handlers ───────────────────────────────────────────────────

mcp.setRequestHandler(ListToolsRequestSchema, async () => {
  const pluginTools = [...registry.values()]
    .filter((e) => e.active)
    .flatMap((e) => e.plugin.tools);
  return { tools: [...getOrchestratorTools(), ...pluginTools] };
});

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;
  const callStart = Date.now();
  logger.info({ tool: name }, "tool call");
  orchestrator.audit({
    category: "tool_call", action: "received", outcome: "success",
    sessionId: activeSessionId,
    detail: { tool: name, args: args || {} },
  });

  const surfaceUnacked = (result: { content: Array<{ type: "text"; text: string }>; [key: string]: unknown }) => {
    if (!activeSessionId) return result;
    const unacked = orchestrator.listUnackedDeliveredLive(activeSessionId);
    if (unacked.length === 0) return result;

    const attentionMap = new Map(
      unacked
        .map((d) => d.attention_id ? orchestrator.getAttention(d.attention_id) : null)
        .filter((a): a is NonNullable<typeof a> => a !== null)
        .map((a) => [a.attention_id, a]),
    );

    const lines = unacked.map((d) => {
      const att = d.attention_id ? attentionMap.get(d.attention_id) : null;
      const hint = att?.summary_hint || "(no summary)";
      const importance = att?.importance || "normal";
      return `- [${importance}] ${hint} (delivery_id=${d.delivery_id})`;
    });

    const banner =
      `\n\n[unacked-channel-events] ${unacked.length} live notification(s) had not been surfaced before this call:\n${lines.join("\n")}\n` +
      `(replayed because their fire-and-forget MCP notifications may have been dropped between turns)`;

    orchestrator.ackDeliveriesBySession(activeSessionId);

    return {
      ...result,
      content: [...result.content, { type: "text" as const, text: banner }],
    };
  };

  try {
    const orchResult = await handleOrchestratorToolCall(orchestrator, name, (args as Record<string, unknown>) || {}, activeSessionId!);
    if (orchResult) {
      if (name === "add_watch" && (args as Record<string, unknown>).watch_type === "pipeline-chain") {
        const entityRef = (args as Record<string, unknown>).entity_ref as string;
        const match = entityRef.match(/^gitlab:([^:]+):ref:(.+)$/);
        if (match) {
          const gitlabEntry = registry.get("gitlab");
          if (gitlabEntry?.active && "startPipelineWatch" in gitlabEntry.plugin) {
            (gitlabEntry.plugin as { startPipelineWatch: (p: string, r: string) => boolean }).startPipelineWatch(match[1], match[2]);
          }
        }
      }
      const finalResult = surfaceUnacked(orchResult);
      orchestrator.audit({
        category: "tool_call", action: "completed", outcome: "success",
        sessionId: activeSessionId,
        detail: { tool: name, source: "orchestrator", duration_ms: Date.now() - callStart },
      });
      return finalResult;
    }

    for (const [pluginName, entry] of registry) {
      if (!entry.active) continue;
      const result = await entry.plugin.handleToolCall(name, (args as Record<string, unknown>) || {});
      if (result) {
        const finalResult = surfaceUnacked(result);
        orchestrator.audit({
          category: "tool_call", action: "completed", outcome: "success",
          sessionId: activeSessionId,
          detail: { tool: name, source: pluginName, duration_ms: Date.now() - callStart },
        });
        return finalResult;
      }
    }

    logger.warn({ name }, "unknown tool");
    orchestrator.audit({
      category: "tool_call", action: "unknown_tool", outcome: "error",
      sessionId: activeSessionId,
      detail: { tool: name, duration_ms: Date.now() - callStart },
    });
    throw new Error(`Unknown tool: ${name}`);
  } catch (err) {
    orchestrator.audit({
      category: "tool_call", action: "failed", outcome: "error",
      sessionId: activeSessionId,
      detail: { tool: name, error: err instanceof Error ? err.message : String(err), duration_ms: Date.now() - callStart },
    });
    throw err;
  }
});

// ─── Start ───────────────────────────────────────────────────────────

await mcp.connect(new StdioServerTransport());
logger.info("MCP connected via stdio");

// ─── Session Auto-Management ─────────────────────────────────────────

const { session, resumed } = orchestrator.findOrCreateSession(SESSION_OWNER, {
  session_name: `orchestrator-${SESSION_ROLE}`,
  role: SESSION_ROLE,
});
activeSessionId = session.session_id;
logger.info({ sessionId: activeSessionId, resumed }, "session ready");

const runtime = orchestrator.attachRuntime(activeSessionId, undefined, "stdio");
activeRuntimeId = runtime.runtime_id;
logger.info({ runtimeId: activeRuntimeId }, "runtime attached");

{
  const replayCount = await orchestrator.replayOnResume(activeSessionId, async (content, meta) => {
    await mcp.notification({
      method: "notifications/claude/channel",
      params: { content, meta },
    });
  });
  if (replayCount > 0) {
    logger.info({ replayed: replayCount, resumed }, "deliveries replayed on attach");
  }
}

let maintenanceCycle = 0;
const maintenanceTimer = setInterval(() => {
  orchestrator.expireStaleWatches();
  orchestrator.expireStaleDeliveries();
  orchestrator.detachStaleRuntimes();
  if (activeRuntimeId) {
    orchestrator.heartbeat(activeRuntimeId);
  }
  maintenanceCycle++;
  if (maintenanceCycle % 60 === 0) {
    orchestrator.purgeOldEvents();
    const purgedAudit = orchestrator.purgeOldAudit();
    if (purgedAudit > 0) {
      logger.info({ purged: purgedAudit }, "old audit entries purged");
    }
  }
}, 60_000);

if (autoStart) {
  for (const [name, entry] of registry) {
    entry.active = true;
    await entry.plugin.start();
    logger.info({ name }, "plugin auto-started");
  }
}

const shutdown = async () => {
  logger.info("shutting down");
  clearInterval(maintenanceTimer);
  if (activeSessionId) {
    orchestrator.markSessionResumable(activeSessionId);
    logger.info({ sessionId: activeSessionId }, "session marked resumable");
  }
  for (const [, entry] of registry) {
    if (!entry.active) continue;
    try {
      await entry.plugin.stop();
    } catch (err) {
      logger.error({ err, name: entry.plugin.name }, "plugin stop error");
    }
  }
  orchestrator.close();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
