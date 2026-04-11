#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { SlackChannelPlugin } from "./channels/slack-plugin.js";

const plugin = new SlackChannelPlugin();

const mcp = new Server(
  { name: "slack-channel", version: "0.1.0" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `Events from Slack arrive as <channel source="slack-channel" event_type="..." ...>.
Event types: message_received, mention, thread_reply, alert.`,
  },
);

await plugin.init(async (n) => {
  await mcp.notification({
    method: "notifications/claude/channel",
    params: { content: n.content, meta: n.meta },
  });
});

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: plugin.tools,
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const result = await plugin.handleToolCall(
    req.params.name,
    (req.params.arguments as Record<string, unknown>) || {},
  );
  if (!result) throw new Error(`Unknown tool: ${req.params.name}`);
  return result;
});

await mcp.connect(new StdioServerTransport());
await plugin.start();

process.on("SIGINT", async () => {
  await plugin.stop();
  process.exit(0);
});
