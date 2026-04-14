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
  ToolDef,
} from "./channel-plugin.js";
import { Orchestrator } from "./orchestrator.js";
import { getOrchestratorTools, handleOrchestratorToolCall } from "./orchestrator-mcp.js";

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

const LOG_DIR = join(homedir(), ".cache", "channel-hub");
mkdirSync(LOG_DIR, { recursive: true });
const LOG_PATH = process.env.CHANNEL_HUB_LOG_PATH || join(LOG_DIR, "hub.log");
const LOG_LEVEL = process.env.CHANNEL_HUB_LOG_LEVEL || "info";

const logger = pino(
  { level: LOG_LEVEL },
  pino.destination({ dest: LOG_PATH, sync: false }),
);

const pluginNames = (process.env.CHANNEL_PLUGINS || "gitlab")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const autoStart = (process.env.CHANNEL_HUB_AUTO_START || "true") === "true";

logger.info({ plugins: pluginNames, autoStart }, "channel-hub starting");

const orchestrator = new Orchestrator({ logger });

const SESSION_OWNER = process.env.CHANNEL_HUB_SESSION_OWNER || "default";
const SESSION_ROLE = process.env.CHANNEL_HUB_SESSION_ROLE || "main";
let activeSessionId: string | null = null;
let activeRuntimeId: string | null = null;

// ─── Plugin State ─────────────────────────────────────────────────────

interface PluginEntry {
  plugin: ChannelPlugin;
  active: boolean;
  eventFilter: Set<string> | null;
}

const registry = new Map<string, PluginEntry>();

for (const name of pluginNames) {
  const factory = PLUGIN_MAP[name];
  if (!factory) {
    logger.error({ name }, "unknown plugin");
    throw new Error(`Unknown channel plugin: ${name}`);
  }
  const plugin = await factory();
  registry.set(name, { plugin, active: false, eventFilter: null });
  logger.info({ name }, "plugin loaded");
}

// ─── Hub Tools ────────────────────────────────────────────────────────

const hubTools: ToolDef[] = [
  {
    name: "hub_subscribe",
    description:
      "Subscribe to a channel plugin. Optionally filter to specific event types. Available plugins: " +
      pluginNames.join(", "),
    inputSchema: {
      type: "object",
      properties: {
        plugin: {
          type: "string",
          description: `Plugin name: ${pluginNames.join(", ")}`,
        },
        events: {
          type: "array",
          items: { type: "string" },
          description: "Event types to receive (e.g. [\"todo_created\", \"pipeline_watch_completed\"]). Omit for all events.",
        },
      },
      required: ["plugin"],
    },
  },
  {
    name: "hub_unsubscribe",
    description: "Unsubscribe from a channel plugin (stop receiving events)",
    inputSchema: {
      type: "object",
      properties: {
        plugin: {
          type: "string",
          description: "Plugin name to unsubscribe from",
        },
      },
      required: ["plugin"],
    },
  },
  {
    name: "hub_status",
    description: "Show status of all channel plugins (active/inactive, tools)",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
];

// ─── MCP Server ───────────────────────────────────────────────────────

const pluginList = [...registry.values()].map((e) => e.plugin);

const mcp = new Server(
  { name: "channel-hub", version: "0.1.0" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `Channel hub multiplexing: ${pluginNames.join(", ")}.
Events arrive as <channel source="channel-hub" plugin="..." event_type="..." ...>.
Events are diff-based — you only receive CHANGES.

Hub tools:
- hub_subscribe: activate a plugin to start receiving events
- hub_unsubscribe: deactivate a plugin
- hub_status: show all plugins and their status

Orchestrator tools (session & watch management):
- add_watch: watch an entity (pipeline, MR, thread) — matching events route to your session
- remove_watch / list_watches: manage watches
- list_session_feed: see attention items routed to this session
- list_pending_deliveries / get_delivery_summary: see queued events
- ack_attention / snooze_attention / resolve_attention: manage attention lifecycle
- get_session_state: full session state dump
- list_unmatched_events: events that matched no watch

Session is auto-managed: created on first startup, resumed on reconnect. Watches survive session restarts.

Plugin tools are available once the plugin is subscribed.
${pluginList.map((p) => `[${p.name}]\n  events: ${p.eventTypes.map((e) => e.name).join(", ")}\n  tools: ${p.tools.map((t) => t.name).join(", ")}`).join("\n")}`,
  },
);

// ─── Notification Forwarding ──────────────────────────────────────────

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
    const eventType = n.meta.event_type;
    if (entry.eventFilter && !entry.eventFilter.has(eventType)) {
      logger.debug({ plugin: entry.plugin.name, eventType, filtered: true }, "event filtered out");
      return;
    }

    const result = orchestrator.ingestEvent(n);
    logger.debug(
      { plugin: entry.plugin.name, eventType, eventId: result.eventId, deduplicated: result.deduplicated, matchedWatches: result.matchedWatches },
      "event ingested",
    );

    if (result.matchedWatches === 0) {
      await mcp.notification({
        method: "notifications/claude/channel",
        params: {
          content: n.content,
          meta: { plugin: entry.plugin.name, ...n.meta },
        },
      });
    }
  };

for (const [, entry] of registry) {
  await entry.plugin.init(makeNotify(entry));
  logger.info(
    { name: entry.plugin.name, tools: entry.plugin.tools.length },
    "plugin initialized",
  );
}

// ─── Tool Handlers ────────────────────────────────────────────────────

mcp.setRequestHandler(ListToolsRequestSchema, async () => {
  const activeTools = [...registry.values()]
    .filter((e) => e.active)
    .flatMap((e) => e.plugin.tools);
  return { tools: [...hubTools, ...getOrchestratorTools(), ...activeTools] };
});

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;
  logger.info({ tool: name }, "tool call");

  if (name === "hub_subscribe") {
    const pluginName = (args as { plugin: string }).plugin;
    const entry = registry.get(pluginName);
    if (!entry) {
      return {
        content: [
          {
            type: "text" as const,
            text: `Unknown plugin "${pluginName}". Available: ${pluginNames.join(", ")}`,
          },
        ],
      };
    }
    const events = (args as { events?: string[] }).events;
    const filter = events ? new Set(events) : null;

    if (entry.active) {
      entry.eventFilter = filter;
      logger.info({ plugin: pluginName, events: events || "all" }, "event filter updated");
      return {
        content: [
          {
            type: "text" as const,
            text: filter
              ? `Updated "${pluginName}" filter to: ${[...filter].join(", ")}`
              : `Updated "${pluginName}" to receive all events`,
          },
        ],
      };
    }
    entry.active = true;
    entry.eventFilter = filter;
    await entry.plugin.start();
    logger.info({ plugin: pluginName, events: events || "all" }, "subscribed");
    const filterInfo = filter ? ` Filtering: ${[...filter].join(", ")}` : "";
    return {
      content: [
        {
          type: "text" as const,
          text: `Subscribed to "${pluginName}".${filterInfo} Tools: ${entry.plugin.tools.map((t) => t.name).join(", ")}`,
        },
      ],
    };
  }

  if (name === "hub_unsubscribe") {
    const pluginName = (args as { plugin: string }).plugin;
    const entry = registry.get(pluginName);
    if (!entry) {
      return {
        content: [
          {
            type: "text" as const,
            text: `Unknown plugin "${pluginName}"`,
          },
        ],
      };
    }
    if (!entry.active) {
      return {
        content: [
          {
            type: "text" as const,
            text: `Not subscribed to "${pluginName}"`,
          },
        ],
      };
    }
    entry.active = false;
    await entry.plugin.stop();
    logger.info({ plugin: pluginName }, "unsubscribed");
    return {
      content: [
        {
          type: "text" as const,
          text: `Unsubscribed from "${pluginName}". Events stopped.`,
        },
      ],
    };
  }

  if (name === "hub_status") {
    const lines = [...registry.entries()].map(([n, e]) => {
      const status = e.active ? "🟢 active" : "⚪ inactive";
      const tools = e.plugin.tools.map((t) => t.name).join(", ");
      const events = e.plugin.eventTypes.map((et) => et.name).join(", ");
      const filterInfo = e.eventFilter
        ? `filter: ${[...e.eventFilter].join(", ")}`
        : "filter: all";
      return `${n}: ${status} | ${filterInfo}\n  events: ${events}\n  tools: ${tools}`;
    });
    return {
      content: [{ type: "text" as const, text: lines.join("\n\n") }],
    };
  }

  const orchResult = await handleOrchestratorToolCall(orchestrator, name, (args as Record<string, unknown>) || {});
  if (orchResult) return orchResult;

  for (const [, entry] of registry) {
    if (!entry.active) continue;
    const result = await entry.plugin.handleToolCall(
      name,
      (args as Record<string, unknown>) || {},
    );
    if (result) return result;
  }

  logger.warn({ name }, "unknown tool");
  throw new Error(`Unknown tool: ${name}`);
});

// ─── Start ────────────────────────────────────────────────────────────

await mcp.connect(new StdioServerTransport());
logger.info("MCP connected via stdio");

// ─── Session Auto-Management ─────────────────────────────────────────

const { session, resumed } = orchestrator.findOrCreateSession(SESSION_OWNER, {
  session_name: `hub-${SESSION_ROLE}`,
  role: SESSION_ROLE,
});
activeSessionId = session.session_id;
logger.info({ sessionId: activeSessionId, resumed }, "orchestrator session ready");

const runtime = orchestrator.attachRuntime(activeSessionId, undefined, "stdio");
activeRuntimeId = runtime.runtime_id;
logger.info({ runtimeId: activeRuntimeId }, "runtime attached");

if (resumed) {
  const pending = orchestrator.listPendingDeliveries(activeSessionId);
  if (pending.length > 0) {
    logger.info({ pending: pending.length }, "replaying pending deliveries");
    await orchestrator.replayOnResume(activeSessionId, async (content, meta) => {
      await mcp.notification({
        method: "notifications/claude/channel",
        params: { content, meta },
      });
    });
  }
}

const maintenanceTimer = setInterval(() => {
  orchestrator.expireStaleWatches();
  orchestrator.expireStaleDeliveries();
  orchestrator.detachStaleRuntimes();
  if (activeRuntimeId) {
    orchestrator.heartbeat(activeRuntimeId);
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
    logger.info({ sessionId: activeSessionId }, "session marked resumable for next startup");
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
