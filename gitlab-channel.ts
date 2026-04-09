#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import Database from "better-sqlite3";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import nodeFetch from "node-fetch";
import { pino } from "pino";

const GITLAB_API_URL =
  process.env.GITLAB_API_URL || "https://gitlab.com/api/v4";
const GITLAB_TOKEN = process.env.GITLAB_PERSONAL_ACCESS_TOKEN!;
const POLL_INTERVAL_MS = parseInt(
  process.env.GITLAB_CHANNEL_POLL_INTERVAL || "30000",
  10,
);
const WATCH_POLL_MS = 10_000;
const WATCH_TIMEOUT_MS = 30 * 60 * 1000;
const NAMESPACE_FILTER = process.env.GITLAB_CHANNEL_NAMESPACE || "";

const headers = {
  "PRIVATE-TOKEN": GITLAB_TOKEN,
  "Content-Type": "application/json",
};

const TERMINAL_STATUSES = new Set([
  "success",
  "failed",
  "canceled",
  "skipped",
  "manual",
]);

// ─── SQLite State ─────────────────────────────────────────────────────

const DB_DIR = join(homedir(), ".cache", "gitlab-channel");
mkdirSync(DB_DIR, { recursive: true });
const DB_PATH = process.env.GITLAB_CHANNEL_DB_PATH || join(DB_DIR, "state.db");

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_iid INTEGER NOT NULL,
    target_title TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    project_path TEXT NOT NULL,
    author TEXT NOT NULL,
    author_name TEXT NOT NULL,
    body TEXT NOT NULL,
    target_url TEXT NOT NULL,
    seen_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS pipelines (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    status TEXT NOT NULL,
    ref TEXT NOT NULL,
    sha TEXT NOT NULL,
    web_url TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    seen_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    discovered_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS watches (
    project_id TEXT NOT NULL,
    ref TEXT NOT NULL,
    pipeline_id INTEGER,
    started_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, ref)
  );
`);

const stmts = {
  getTodo: db.prepare("SELECT * FROM todos WHERE id = ?"),
  upsertTodo: db.prepare(`
    INSERT INTO todos (id, state, action, target_type, target_iid, target_title, project_id, project_path, author, author_name, body, target_url, seen_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET state=excluded.state, seen_at=excluded.seen_at
  `),
  markTodoResolved: db.prepare("UPDATE todos SET state = 'done', seen_at = ? WHERE id = ? AND state = 'pending'"),
  getPendingTodoIds: db.prepare("SELECT id FROM todos WHERE state = 'pending'"),

  getPipeline: db.prepare("SELECT * FROM pipelines WHERE id = ?"),
  upsertPipeline: db.prepare(`
    INSERT INTO pipelines (id, project_id, status, ref, sha, web_url, updated_at, seen_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at, seen_at=excluded.seen_at
  `),

  getProjects: db.prepare("SELECT id FROM projects"),
  upsertProject: db.prepare(`
    INSERT INTO projects (id, path, discovered_at) VALUES (?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET path=excluded.path
  `),
  projectCount: db.prepare("SELECT COUNT(*) as count FROM projects"),

  getWatch: db.prepare("SELECT * FROM watches WHERE project_id = ? AND ref = ?"),
  insertWatch: db.prepare("INSERT OR IGNORE INTO watches (project_id, ref, pipeline_id, started_at) VALUES (?, ?, ?, ?)"),
  updateWatchPipeline: db.prepare("UPDATE watches SET pipeline_id = ? WHERE project_id = ? AND ref = ?"),
  deleteWatch: db.prepare("DELETE FROM watches WHERE project_id = ? AND ref = ?"),
  getAllWatches: db.prepare("SELECT * FROM watches"),
};

// ─── Logger ──────────────────────────────────────────────────────────

const LOG_PATH = process.env.GITLAB_CHANNEL_LOG_PATH || join(DB_DIR, "channel.log");
const LOG_LEVEL = process.env.GITLAB_CHANNEL_LOG_LEVEL || "info";

const logger = pino(
  { level: LOG_LEVEL },
  pino.destination({ dest: LOG_PATH, sync: false }),
);

logger.info({
  api: GITLAB_API_URL,
  pollInterval: POLL_INTERVAL_MS,
  watchPoll: WATCH_POLL_MS,
  namespace: NAMESPACE_FILTER || "(all)",
  dbPath: DB_PATH,
  logPath: LOG_PATH,
}, "gitlab-channel starting");

// ─── Types ────────────────────────────────────────────────────────────

interface GitLabTodo {
  id: number;
  action_name: string;
  target_type: string;
  target: { iid: number; title: string };
  project: { id: number; path_with_namespace: string };
  author: { username: string; name: string };
  body: string;
  state: string;
  target_url: string;
}

interface GitLabPipeline {
  id: number;
  iid: number;
  status: string;
  ref: string;
  sha: string;
  web_url: string;
  updated_at: string;
}

// ─── MCP Server ───────────────────────────────────────────────────────

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

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "gitlab_reply",
      description: "Post a comment on a GitLab merge request or issue",
      inputSchema: {
        type: "object" as const,
        properties: {
          project_id: { type: "string", description: "Project ID or URL-encoded path" },
          mr_iid: { type: "string", description: "Merge request IID (omit for issues)" },
          issue_iid: { type: "string", description: "Issue IID (omit for MRs)" },
          text: { type: "string", description: "Comment body (markdown)" },
        },
        required: ["project_id", "text"],
      },
    },
    {
      name: "gitlab_mark_todo_done",
      description: "Mark a GitLab todo as done",
      inputSchema: {
        type: "object" as const,
        properties: {
          todo_id: { type: "string", description: "The todo ID to mark as done" },
        },
        required: ["todo_id"],
      },
    },
    {
      name: "gitlab_watch_pipeline",
      description: "Watch a pipeline on a specific branch. Notifies when it finishes (success/failed/canceled). Auto-expires after 30 minutes.",
      inputSchema: {
        type: "object" as const,
        properties: {
          project_id: { type: "string", description: "Project ID or URL-encoded path" },
          ref: { type: "string", description: "Branch name to watch (e.g. 'int', 'main')" },
        },
        required: ["project_id", "ref"],
      },
    },
  ],
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;
  logger.info({ tool: name, args }, "tool called");

  if (name === "gitlab_reply") {
    const { project_id, mr_iid, issue_iid, text } = args as {
      project_id: string; mr_iid?: string; issue_iid?: string; text: string;
    };
    const entity = mr_iid ? `merge_requests/${mr_iid}` : `issues/${issue_iid}`;
    const url = `${GITLAB_API_URL}/projects/${encodeURIComponent(project_id)}/${entity}/notes`;
    const res = await nodeFetch(url, {
      method: "POST", headers, body: JSON.stringify({ body: text }),
    });
    if (!res.ok) {
      const body = await res.text();
      logger.error({ status: res.status, body, project_id, entity }, "gitlab_reply failed");
      throw new Error(`GitLab API error ${res.status}: ${body}`);
    }
    logger.info({ project_id, entity }, "comment posted");
    return { content: [{ type: "text" as const, text: "Comment posted" }] };
  }

  if (name === "gitlab_mark_todo_done") {
    const { todo_id } = args as { todo_id: string };
    const url = `${GITLAB_API_URL}/todos/${todo_id}/mark_as_done`;
    const res = await nodeFetch(url, { method: "POST", headers });
    if (!res.ok) {
      const body = await res.text();
      logger.error({ status: res.status, body, todo_id }, "mark_todo_done failed");
      throw new Error(`GitLab API error ${res.status}: ${body}`);
    }
    logger.info({ todo_id }, "todo marked done");
    return { content: [{ type: "text" as const, text: "Todo marked as done" }] };
  }

  if (name === "gitlab_watch_pipeline") {
    const { project_id, ref } = args as { project_id: string; ref: string };
    const existing = stmts.getWatch.get(project_id, ref) as { project_id: string } | undefined;
    if (existing) {
      logger.debug({ project_id, ref }, "duplicate watch rejected");
      return { content: [{ type: "text" as const, text: `Already watching branch "${ref}" on project ${project_id}` }] };
    }
    stmts.insertWatch.run(project_id, ref, null, Date.now());
    logger.info({ project_id, ref }, "watch started");
    return { content: [{ type: "text" as const, text: `Watching pipeline on branch "${ref}" for project ${project_id}. Will notify when it finishes.` }] };
  }

  logger.warn({ name }, "unknown tool called");
  throw new Error(`Unknown tool: ${name}`);
});

await mcp.connect(new StdioServerTransport());
logger.info("MCP connected via stdio");

let seededTodos = false;
let seededPipelines = false;

// ─── Polling: Todos (diff-based) ──────────────────────────────────────

async function pollTodos() {
  try {
    const url = `${GITLAB_API_URL}/todos?state=pending&per_page=100`;
    logger.debug("polling todos");
    const res = await nodeFetch(url, { headers });
    if (!res.ok) {
      logger.warn({ status: res.status }, "todos poll failed");
      return;
    }

    const todos = (await res.json()) as GitLabTodo[];
    const now = Date.now();
    const currentIds = new Set<number>();
    const isFirstRun = !seededTodos;

    logger.debug({ count: todos.length, isFirstRun }, "todos fetched");

    for (const todo of todos) {
      currentIds.add(todo.id);
      const existing = stmts.getTodo.get(todo.id) as { id: number; state: string } | undefined;

      if (!existing) {
        stmts.upsertTodo.run(
          todo.id, "pending", todo.action_name, todo.target_type,
          todo.target.iid, todo.target.title, todo.project.id,
          todo.project.path_with_namespace, todo.author.username,
          todo.author.name, todo.body, todo.target_url, now,
        );
        if (!isFirstRun) {
          logger.info({ todoId: todo.id, action: todo.action_name, author: todo.author.username, project: todo.project.path_with_namespace }, "todo_created event");
          await mcp.notification({
            method: "notifications/claude/channel",
            params: {
              content: `${todo.author.name} (@${todo.author.username}) — ${todo.action_name}: ${todo.target.title}\n\n${todo.body}`,
              meta: {
                event_type: "todo_created",
                todo_id: String(todo.id),
                action: todo.action_name,
                target_type: todo.target_type,
                target_iid: String(todo.target.iid),
                project: todo.project.path_with_namespace,
                project_id: String(todo.project.id),
                author: todo.author.username,
                target_url: todo.target_url,
              },
            },
          });
        }
      }
    }

    if (!isFirstRun) {
      const pendingRows = stmts.getPendingTodoIds.all() as Array<{ id: number }>;
      for (const row of pendingRows) {
        if (!currentIds.has(row.id)) {
          const todo = stmts.getTodo.get(row.id) as { project_path: string } | undefined;
          stmts.markTodoResolved.run(now, row.id);
          logger.info({ todoId: row.id, project: todo?.project_path }, "todo_resolved event");
          await mcp.notification({
            method: "notifications/claude/channel",
            params: {
              content: `Todo #${row.id} resolved`,
              meta: {
                event_type: "todo_resolved",
                todo_id: String(row.id),
                project: todo?.project_path || "",
              },
            },
          });
        }
      }
    }

    if (isFirstRun) {
      logger.info({ seeded: todos.length }, "todos seeded (first run, no events emitted)");
    }
    seededTodos = true;
  } catch (err) {
    logger.error({ err }, "pollTodos error");
  }
}

// ─── Polling: Pipelines (diff-based) ──────────────────────────────────

async function discoverProjects() {
  const { count } = stmts.projectCount.get() as { count: number };
  if (count > 0) {
    logger.debug({ cached: count }, "projects already discovered");
    return;
  }
  try {
    let page = 1;
    let total = 0;
    const now = Date.now();
    logger.info({ namespace: NAMESPACE_FILTER || "(all)" }, "discovering projects");
    while (true) {
      const url = `${GITLAB_API_URL}/projects?membership=true&simple=true&archived=false&per_page=100&page=${page}`;
      const res = await nodeFetch(url, { headers });
      if (!res.ok) {
        logger.warn({ status: res.status, page }, "project discovery page failed");
        break;
      }
      const projects = (await res.json()) as Array<{ id: number; path_with_namespace: string }>;
      if (projects.length === 0) break;
      for (const p of projects) {
        const path = p.path_with_namespace || String(p.id);
        if (NAMESPACE_FILTER && !path.startsWith(NAMESPACE_FILTER)) continue;
        stmts.upsertProject.run(p.id, path, now);
        total++;
      }
      page++;
    }
    logger.info({ total, pages: page - 1 }, "project discovery complete");
  } catch (err) {
    logger.error({ err }, "discoverProjects error");
  }
}

async function pollPipelines() {
  await discoverProjects();
  const projects = stmts.getProjects.all() as Array<{ id: number }>;
  if (projects.length === 0) return;

  logger.debug({ projects: projects.length }, "polling pipelines");
  const now = Date.now();

  for (const project of projects) {
    try {
      const url = `${GITLAB_API_URL}/projects/${encodeURIComponent(String(project.id))}/pipelines?per_page=10&order_by=updated_at&sort=desc`;
      const res = await nodeFetch(url, { headers });
      if (!res.ok) {
        logger.warn({ projectId: project.id, status: res.status }, "pipeline poll failed");
        continue;
      }

      const pipelines = (await res.json()) as GitLabPipeline[];
      for (const pipeline of pipelines) {
        const existing = stmts.getPipeline.get(pipeline.id) as { status: string } | undefined;
        const oldStatus = existing?.status || null;

        stmts.upsertPipeline.run(
          pipeline.id, String(project.id), pipeline.status,
          pipeline.ref, pipeline.sha, pipeline.web_url,
          pipeline.updated_at, now,
        );

        if (oldStatus !== null && oldStatus !== pipeline.status) {
          logger.info({ pipelineId: pipeline.id, ref: pipeline.ref, oldStatus, newStatus: pipeline.status, projectId: project.id }, "pipeline_status_changed event");
          await mcp.notification({
            method: "notifications/claude/channel",
            params: {
              content: `Pipeline #${pipeline.id} on "${pipeline.ref}": ${oldStatus} → ${pipeline.status} (${pipeline.sha.slice(0, 8)})`,
              meta: {
                event_type: "pipeline_status_changed",
                pipeline_id: String(pipeline.id),
                old_status: oldStatus,
                new_status: pipeline.status,
                ref: pipeline.ref,
                project: String(project.id),
                web_url: pipeline.web_url,
              },
            },
          });
        }
      }
    } catch (err) {
      logger.error({ err, projectId: project.id }, "pollPipelines error");
    }
  }
}

// ─── Polling: Watched Pipelines ───────────────────────────────────────

async function pollWatchedPipelines() {
  const watches = stmts.getAllWatches.all() as Array<{
    project_id: string; ref: string; pipeline_id: number | null; started_at: number;
  }>;
  if (watches.length === 0) return;
  logger.debug({ count: watches.length }, "polling watched pipelines");
  const now = Date.now();

  for (const watch of watches) {
    if (now - watch.started_at > WATCH_TIMEOUT_MS) {
      stmts.deleteWatch.run(watch.project_id, watch.ref);
      logger.info({ project: watch.project_id, ref: watch.ref }, "watch expired");
      await mcp.notification({
        method: "notifications/claude/channel",
        params: {
          content: `Pipeline watch expired for branch "${watch.ref}" on project ${watch.project_id} (30 min timeout)`,
          meta: { event_type: "pipeline_watch_expired", project: watch.project_id, ref: watch.ref },
        },
      });
      continue;
    }

    try {
      const url = `${GITLAB_API_URL}/projects/${encodeURIComponent(watch.project_id)}/pipelines?ref=${encodeURIComponent(watch.ref)}&per_page=1&order_by=updated_at&sort=desc`;
      const res = await nodeFetch(url, { headers });
      if (!res.ok) {
        logger.warn({ project: watch.project_id, ref: watch.ref, status: res.status }, "watch poll failed");
        continue;
      }

      const pipelines = (await res.json()) as GitLabPipeline[];
      if (pipelines.length === 0) {
        logger.debug({ project: watch.project_id, ref: watch.ref }, "no pipelines for watched ref");
        continue;
      }

      const latest = pipelines[0];
      logger.debug({ project: watch.project_id, ref: watch.ref, pipelineId: latest.id, status: latest.status }, "watch poll result");

      if (watch.pipeline_id !== latest.id) {
        stmts.updateWatchPipeline.run(latest.id, watch.project_id, watch.ref);
      }

      if (TERMINAL_STATUSES.has(latest.status)) {
        stmts.deleteWatch.run(watch.project_id, watch.ref);
        const emoji = latest.status === "success" ? "✅" : latest.status === "failed" ? "❌" : "⚠️";
        logger.info({ pipelineId: latest.id, status: latest.status, ref: watch.ref, project: watch.project_id }, "pipeline_watch_completed event");
        await mcp.notification({
          method: "notifications/claude/channel",
          params: {
            content: `${emoji} Pipeline #${latest.id} on branch "${watch.ref}" finished: ${latest.status} (${latest.sha.slice(0, 8)})`,
            meta: {
              event_type: "pipeline_watch_completed",
              pipeline_id: String(latest.id),
              status: latest.status,
              ref: watch.ref,
              project: watch.project_id,
              web_url: latest.web_url,
            },
          },
        });
      }
    } catch (err) {
      logger.error({ err, project: watch.project_id, ref: watch.ref }, "pollWatchedPipelines error");
    }
  }
}

// ─── Poll Loops ───────────────────────────────────────────────────────

async function pollLoop() {
  await pollTodos();
  await pollPipelines();
  setTimeout(pollLoop, POLL_INTERVAL_MS);
}

async function watchLoop() {
  await pollWatchedPipelines();
  setTimeout(watchLoop, WATCH_POLL_MS);
}

logger.info("starting poll loops");
pollLoop();
watchLoop();
