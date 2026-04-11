#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { GitLabChannelPlugin } from "./channels/gitlab-plugin.js";

const plugin = new GitLabChannelPlugin();

const mcp = new Server(
  { name: "gitlab-channel", version: "0.2.0" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `Events from GitLab arrive as <channel source="gitlab-channel" event_type="..." ...>.
Events are diff-based (like webhooks) — you only receive CHANGES, not full state.

Event types:
- "todo_created": new todo appeared (mention, review request, assignment). Attributes: todo_id, action, target_type, target_iid, author, project, target_url.
- "todo_resolved": a previously pending todo was resolved/done externally. Attributes: todo_id, project.
- "pipeline_status_changed": pipeline status changed (e.g. running→failed, pending→success). Attributes: pipeline_id, old_status, new_status, ref, project, web_url.
- "pipeline_watch_completed": a watched pipeline reached terminal status. Attributes: pipeline_id, status, ref, project, web_url.
- "pipeline_watch_expired": watch timed out after 30 minutes.

Tools: gitlab_reply (comment on MR/issue), gitlab_mark_todo_done (dismiss todo), gitlab_watch_pipeline (track branch pipeline until completion).`,
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
