import type {
  ChannelPlugin,
  EventTypeDef,
  NotifyFn,
  ToolDef,
  ToolCallResult,
} from "../channel-plugin.js";
import Database from "better-sqlite3";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import nodeFetch from "node-fetch";
import { pino } from "pino";

const TERMINAL_STATUSES = new Set([
  "success",
  "failed",
  "canceled",
  "skipped",
  "manual",
]);

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

interface Stmts {
  getTodo: Database.Statement;
  upsertTodo: Database.Statement;
  markTodoResolved: Database.Statement;
  getPendingTodoIds: Database.Statement;
  getPipeline: Database.Statement;
  upsertPipeline: Database.Statement;
  getProjects: Database.Statement;
  upsertProject: Database.Statement;
  projectCount: Database.Statement;
  getWatch: Database.Statement;
  insertWatch: Database.Statement;
  updateWatchPipeline: Database.Statement;
  deleteWatch: Database.Statement;
  getAllWatches: Database.Statement;
}

export class GitLabChannelPlugin implements ChannelPlugin {
  readonly name = "gitlab";

  readonly eventTypes: EventTypeDef[] = [
    { name: "todo_created", description: "New todo (mention, review request, assignment)" },
    { name: "todo_resolved", description: "Previously pending todo resolved externally" },
    { name: "pipeline_status_changed", description: "Pipeline status changed (e.g. running→failed)" },
    { name: "pipeline_watch_completed", description: "Watched pipeline reached terminal status" },
    { name: "pipeline_watch_expired", description: "Pipeline watch timed out (30 min)" },
  ];

  readonly tools: ToolDef[] = [
    {
      name: "gitlab_reply",
      description: "Post a comment on a GitLab merge request or issue",
      inputSchema: {
        type: "object",
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
        type: "object",
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
        type: "object",
        properties: {
          project_id: { type: "string", description: "Project ID or URL-encoded path" },
          ref: { type: "string", description: "Branch name to watch (e.g. 'int', 'main')" },
        },
        required: ["project_id", "ref"],
      },
    },
  ];

  private notify!: NotifyFn;
  private db!: Database.Database;
  private stmts!: Stmts;
  private logger!: pino.Logger;
  private pollTimer?: ReturnType<typeof setTimeout>;
  private watchTimer?: ReturnType<typeof setTimeout>;
  private seededTodos = false;

  private apiUrl!: string;
  private token!: string;
  private headers!: Record<string, string>;
  private pollIntervalMs!: number;
  private watchPollMs!: number;
  private watchTimeoutMs!: number;
  private namespaceFilter!: string;

  async init(notify: NotifyFn) {
    this.notify = notify;

    this.apiUrl = process.env.GITLAB_API_URL || "https://gitlab.com/api/v4";
    this.token = process.env.GITLAB_PERSONAL_ACCESS_TOKEN!;
    this.pollIntervalMs = parseInt(process.env.GITLAB_CHANNEL_POLL_INTERVAL || "30000", 10);
    this.watchPollMs = parseInt(process.env.GITLAB_CHANNEL_WATCH_POLL || "10000", 10);
    this.watchTimeoutMs = parseInt(process.env.GITLAB_CHANNEL_WATCH_TIMEOUT || String(30 * 60 * 1000), 10);
    this.namespaceFilter = process.env.GITLAB_CHANNEL_NAMESPACE || "";

    this.headers = {
      "PRIVATE-TOKEN": this.token,
      "Content-Type": "application/json",
    };

    const dbDir = join(homedir(), ".cache", "gitlab-channel");
    mkdirSync(dbDir, { recursive: true });
    const dbPath = process.env.GITLAB_CHANNEL_DB_PATH || join(dbDir, "state.db");
    const logPath = process.env.GITLAB_CHANNEL_LOG_PATH || join(dbDir, "channel.log");
    const logLevel = process.env.GITLAB_CHANNEL_LOG_LEVEL || "info";

    this.logger = pino(
      { level: logLevel, name: "gitlab" },
      pino.destination({ dest: logPath, sync: false }),
    );

    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS todos (
        id INTEGER PRIMARY KEY, state TEXT NOT NULL, action TEXT NOT NULL,
        target_type TEXT NOT NULL, target_iid INTEGER NOT NULL, target_title TEXT NOT NULL,
        project_id INTEGER NOT NULL, project_path TEXT NOT NULL, author TEXT NOT NULL,
        author_name TEXT NOT NULL, body TEXT NOT NULL, target_url TEXT NOT NULL, seen_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS pipelines (
        id INTEGER PRIMARY KEY, project_id TEXT NOT NULL, status TEXT NOT NULL,
        ref TEXT NOT NULL, sha TEXT NOT NULL, web_url TEXT NOT NULL,
        updated_at TEXT NOT NULL, seen_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY, path TEXT NOT NULL, discovered_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS watches (
        project_id TEXT NOT NULL, ref TEXT NOT NULL, pipeline_id INTEGER,
        started_at INTEGER NOT NULL, PRIMARY KEY (project_id, ref)
      );
    `);

    this.stmts = {
      getTodo: this.db.prepare("SELECT * FROM todos WHERE id = ?"),
      upsertTodo: this.db.prepare(`
        INSERT INTO todos (id, state, action, target_type, target_iid, target_title, project_id, project_path, author, author_name, body, target_url, seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET state=excluded.state, seen_at=excluded.seen_at
      `),
      markTodoResolved: this.db.prepare("UPDATE todos SET state = 'done', seen_at = ? WHERE id = ? AND state = 'pending'"),
      getPendingTodoIds: this.db.prepare("SELECT id FROM todos WHERE state = 'pending'"),
      getPipeline: this.db.prepare("SELECT * FROM pipelines WHERE id = ?"),
      upsertPipeline: this.db.prepare(`
        INSERT INTO pipelines (id, project_id, status, ref, sha, web_url, updated_at, seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at, seen_at=excluded.seen_at
      `),
      getProjects: this.db.prepare("SELECT id FROM projects"),
      upsertProject: this.db.prepare("INSERT INTO projects (id, path, discovered_at) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET path=excluded.path"),
      projectCount: this.db.prepare("SELECT COUNT(*) as count FROM projects"),
      getWatch: this.db.prepare("SELECT * FROM watches WHERE project_id = ? AND ref = ?"),
      insertWatch: this.db.prepare("INSERT OR IGNORE INTO watches (project_id, ref, pipeline_id, started_at) VALUES (?, ?, ?, ?)"),
      updateWatchPipeline: this.db.prepare("UPDATE watches SET pipeline_id = ? WHERE project_id = ? AND ref = ?"),
      deleteWatch: this.db.prepare("DELETE FROM watches WHERE project_id = ? AND ref = ?"),
      getAllWatches: this.db.prepare("SELECT * FROM watches"),
    };

    this.logger.info({ api: this.apiUrl, pollInterval: this.pollIntervalMs, namespace: this.namespaceFilter || "(all)", dbPath, logPath }, "gitlab plugin initialized");
  }

  async start() {
    this.logger.info("starting poll loops");
    this.schedulePoll();
    this.scheduleWatch();
  }

  async stop() {
    if (this.pollTimer) clearTimeout(this.pollTimer);
    if (this.watchTimer) clearTimeout(this.watchTimer);
    this.db.close();
    this.logger.info("stopped");
  }

  async handleToolCall(name: string, args: Record<string, unknown>): Promise<ToolCallResult | null> {
    if (name === "gitlab_reply") {
      const { project_id, mr_iid, issue_iid, text } = args as {
        project_id: string; mr_iid?: string; issue_iid?: string; text: string;
      };
      if (!mr_iid && !issue_iid) {
        throw new Error("Either mr_iid or issue_iid is required");
      }
      const entity = mr_iid ? `merge_requests/${mr_iid}` : `issues/${issue_iid}`;
      const url = `${this.apiUrl}/projects/${encodeURIComponent(project_id)}/${entity}/notes`;
      const res = await nodeFetch(url, { method: "POST", headers: this.headers, body: JSON.stringify({ body: text }) });
      if (!res.ok) {
        const body = await res.text();
        this.logger.error({ status: res.status, body, project_id, entity }, "gitlab_reply failed");
        throw new Error(`GitLab API error ${res.status}: ${body}`);
      }
      this.logger.info({ project_id, entity }, "comment posted");
      return { content: [{ type: "text", text: "Comment posted" }] };
    }

    if (name === "gitlab_mark_todo_done") {
      const { todo_id } = args as { todo_id: string };
      const url = `${this.apiUrl}/todos/${todo_id}/mark_as_done`;
      const res = await nodeFetch(url, { method: "POST", headers: this.headers });
      if (!res.ok) {
        const body = await res.text();
        this.logger.error({ status: res.status, body, todo_id }, "mark_todo_done failed");
        throw new Error(`GitLab API error ${res.status}: ${body}`);
      }
      this.logger.info({ todo_id }, "todo marked done");
      return { content: [{ type: "text", text: "Todo marked as done" }] };
    }

    if (name === "gitlab_watch_pipeline") {
      const { project_id, ref } = args as { project_id: string; ref: string };
      const existing = this.stmts.getWatch.get(project_id, ref) as { project_id: string } | undefined;
      if (existing) {
        this.logger.debug({ project_id, ref }, "duplicate watch rejected");
        return { content: [{ type: "text", text: `Already watching branch "${ref}" on project ${project_id}` }] };
      }
      this.stmts.insertWatch.run(project_id, ref, null, Date.now());
      this.logger.info({ project_id, ref }, "watch started");
      return { content: [{ type: "text", text: `Watching pipeline on branch "${ref}" for project ${project_id}. Will notify when it finishes.` }] };
    }

    return null;
  }

  // ─── Polling ──────────────────────────────────────────────────────────

  private schedulePoll() {
    this.pollTimer = setTimeout(async () => {
      await this.pollTodos();
      await this.pollPipelines();
      this.schedulePoll();
    }, this.seededTodos ? this.pollIntervalMs : 0);
  }

  private scheduleWatch() {
    this.watchTimer = setTimeout(async () => {
      await this.pollWatchedPipelines();
      this.scheduleWatch();
    }, this.watchPollMs);
  }

  private async pollTodos() {
    try {
      const url = `${this.apiUrl}/todos?state=pending&per_page=100`;
      this.logger.debug("polling todos");
      const res = await nodeFetch(url, { headers: this.headers });
      if (!res.ok) { this.logger.warn({ status: res.status }, "todos poll failed"); return; }

      const todos = (await res.json()) as GitLabTodo[];
      const now = Date.now();
      const currentIds = new Set<number>();
      const isFirstRun = !this.seededTodos;

      this.logger.debug({ count: todos.length, isFirstRun }, "todos fetched");

      for (const todo of todos) {
        currentIds.add(todo.id);
        const existing = this.stmts.getTodo.get(todo.id) as { id: number } | undefined;
        if (!existing) {
          this.stmts.upsertTodo.run(
            todo.id, "pending", todo.action_name, todo.target_type,
            todo.target.iid, todo.target.title, todo.project.id,
            todo.project.path_with_namespace, todo.author.username,
            todo.author.name, todo.body, todo.target_url, now,
          );
          if (!isFirstRun) {
            this.logger.info({ todoId: todo.id, action: todo.action_name, author: todo.author.username }, "todo_created event");
            await this.notify({
              content: `${todo.author.name} (@${todo.author.username}) — ${todo.action_name}: ${todo.target.title}\n\n${todo.body}`,
              meta: {
                event_type: "todo_created", todo_id: String(todo.id), action: todo.action_name,
                target_type: todo.target_type, target_iid: String(todo.target.iid),
                project: todo.project.path_with_namespace, project_id: String(todo.project.id),
                author: todo.author.username, target_url: todo.target_url,
              },
              orchestration: {
                source: "gitlab",
                event_kind: "todo_created",
                entity_type: "todo",
                entity_ref: `gitlab:${todo.project.path_with_namespace}:todo:${todo.id}`,
                correlation_key: `gitlab:${todo.project.path_with_namespace}:${todo.target_type.toLowerCase()}:${todo.target.iid}`,
                dedup_key: `gitlab:todo:${todo.id}:created`,
                importance_hint: "normal",
                actor_ref: `gitlab:${todo.author.username}`,
                title_hint: `${todo.action_name}: ${todo.target.title}`,
                source_ref: todo.target_url,
                thread_ref: `gitlab:${todo.project.path_with_namespace}:${todo.target_type.toLowerCase()}:${todo.target.iid}`,
              },
            });
          }
        }
      }

      if (!isFirstRun) {
        const pendingRows = this.stmts.getPendingTodoIds.all() as Array<{ id: number }>;
        for (const row of pendingRows) {
          if (!currentIds.has(row.id)) {
            const todo = this.stmts.getTodo.get(row.id) as { project_path: string } | undefined;
            this.stmts.markTodoResolved.run(now, row.id);
            this.logger.info({ todoId: row.id }, "todo_resolved event");
            await this.notify({
              content: `Todo #${row.id} resolved`,
              meta: { event_type: "todo_resolved", todo_id: String(row.id), project: todo?.project_path || "" },
              orchestration: {
                source: "gitlab",
                event_kind: "todo_resolved",
                entity_type: "todo",
                entity_ref: `gitlab:todo:${row.id}`,
                dedup_key: `gitlab:todo:${row.id}:resolved`,
                importance_hint: "low",
              },
            });
          }
        }
      }

      if (isFirstRun) this.logger.info({ seeded: todos.length }, "todos seeded (first run)");
      this.seededTodos = true;
    } catch (err) {
      this.logger.error({ err }, "pollTodos error");
    }
  }

  private async discoverProjects() {
    const { count } = this.stmts.projectCount.get() as { count: number };
    if (count > 0) return;
    try {
      let page = 1;
      let total = 0;
      const now = Date.now();
      this.logger.info({ namespace: this.namespaceFilter || "(all)" }, "discovering projects");
      while (true) {
        const url = `${this.apiUrl}/projects?membership=true&simple=true&archived=false&per_page=100&page=${page}`;
        const res = await nodeFetch(url, { headers: this.headers });
        if (!res.ok) break;
        const projects = (await res.json()) as Array<{ id: number; path_with_namespace: string }>;
        if (projects.length === 0) break;
        for (const p of projects) {
          const path = p.path_with_namespace || String(p.id);
          if (this.namespaceFilter && !path.startsWith(this.namespaceFilter)) continue;
          this.stmts.upsertProject.run(p.id, path, now);
          total++;
        }
        page++;
      }
      this.logger.info({ total, pages: page - 1 }, "project discovery complete");
    } catch (err) {
      this.logger.error({ err }, "discoverProjects error");
    }
  }

  private async pollPipelines() {
    await this.discoverProjects();
    const projects = this.stmts.getProjects.all() as Array<{ id: number }>;
    if (projects.length === 0) return;

    this.logger.debug({ projects: projects.length }, "polling pipelines");
    const now = Date.now();

    for (const project of projects) {
      try {
        const url = `${this.apiUrl}/projects/${encodeURIComponent(String(project.id))}/pipelines?per_page=10&order_by=updated_at&sort=desc`;
        const res = await nodeFetch(url, { headers: this.headers });
        if (!res.ok) { this.logger.warn({ projectId: project.id, status: res.status }, "pipeline poll failed"); continue; }

        const pipelines = (await res.json()) as GitLabPipeline[];
        for (const pipeline of pipelines) {
          const existing = this.stmts.getPipeline.get(pipeline.id) as { status: string } | undefined;
          const oldStatus = existing?.status || null;
          this.stmts.upsertPipeline.run(pipeline.id, String(project.id), pipeline.status, pipeline.ref, pipeline.sha, pipeline.web_url, pipeline.updated_at, now);

          if (oldStatus !== null && oldStatus !== pipeline.status) {
            this.logger.info({ pipelineId: pipeline.id, ref: pipeline.ref, oldStatus, newStatus: pipeline.status }, "pipeline_status_changed event");
            await this.notify({
              content: `Pipeline #${pipeline.id} on "${pipeline.ref}": ${oldStatus} → ${pipeline.status} (${pipeline.sha.slice(0, 8)})`,
              meta: {
                event_type: "pipeline_status_changed", pipeline_id: String(pipeline.id),
                old_status: oldStatus, new_status: pipeline.status, ref: pipeline.ref,
                project: String(project.id), web_url: pipeline.web_url,
              },
              orchestration: {
                source: "gitlab",
                event_kind: "pipeline_status_changed",
                entity_type: "pipeline",
                entity_ref: `gitlab:${project.id}:pipeline:${pipeline.id}`,
                correlation_key: `gitlab:${project.id}:ref:${pipeline.ref}`,
                dedup_key: `gitlab:pipeline:${pipeline.id}:${pipeline.status}`,
                importance_hint: TERMINAL_STATUSES.has(pipeline.status)
                  ? (pipeline.status === "failed" ? "high" : "normal")
                  : "low",
                source_ref: pipeline.web_url,
                title_hint: `Pipeline #${pipeline.id} ${oldStatus} -> ${pipeline.status}`,
              },
            });
          }
        }
      } catch (err) {
        this.logger.error({ err, projectId: project.id }, "pollPipelines error");
      }
    }
  }

  private async pollWatchedPipelines() {
    const watches = this.stmts.getAllWatches.all() as Array<{
      project_id: string; ref: string; pipeline_id: number | null; started_at: number;
    }>;
    if (watches.length === 0) return;
    this.logger.debug({ count: watches.length }, "polling watched pipelines");
    const now = Date.now();

    for (const watch of watches) {
      if (now - watch.started_at > this.watchTimeoutMs) {
        this.stmts.deleteWatch.run(watch.project_id, watch.ref);
        this.logger.info({ project: watch.project_id, ref: watch.ref }, "watch expired");
        await this.notify({
          content: `Pipeline watch expired for branch "${watch.ref}" on project ${watch.project_id} (30 min timeout)`,
          meta: { event_type: "pipeline_watch_expired", project: watch.project_id, ref: watch.ref },
          orchestration: {
            source: "gitlab",
            event_kind: "pipeline_watch_expired",
            entity_type: "pipeline",
            entity_ref: `gitlab:${watch.project_id}:ref:${watch.ref}`,
            correlation_key: `gitlab:${watch.project_id}:ref:${watch.ref}`,
            dedup_key: `gitlab:watch_expired:${watch.project_id}:${watch.ref}:${Date.now()}`,
            importance_hint: "normal",
          },
        });
        continue;
      }

      try {
        const url = `${this.apiUrl}/projects/${encodeURIComponent(watch.project_id)}/pipelines?ref=${encodeURIComponent(watch.ref)}&per_page=1&order_by=updated_at&sort=desc`;
        const res = await nodeFetch(url, { headers: this.headers });
        if (!res.ok) { this.logger.warn({ project: watch.project_id, ref: watch.ref, status: res.status }, "watch poll failed"); continue; }

        const pipelines = (await res.json()) as GitLabPipeline[];
        if (pipelines.length === 0) continue;

        const latest = pipelines[0];
        this.logger.debug({ project: watch.project_id, ref: watch.ref, pipelineId: latest.id, status: latest.status }, "watch poll result");

        if (watch.pipeline_id !== latest.id) {
          this.stmts.updateWatchPipeline.run(latest.id, watch.project_id, watch.ref);
        }

        if (TERMINAL_STATUSES.has(latest.status)) {
          this.stmts.deleteWatch.run(watch.project_id, watch.ref);
          const emoji = latest.status === "success" ? "✅" : latest.status === "failed" ? "❌" : "⚠️";
          this.logger.info({ pipelineId: latest.id, status: latest.status, ref: watch.ref }, "pipeline_watch_completed event");
          await this.notify({
            content: `${emoji} Pipeline #${latest.id} on branch "${watch.ref}" finished: ${latest.status} (${latest.sha.slice(0, 8)})`,
            meta: {
              event_type: "pipeline_watch_completed", pipeline_id: String(latest.id),
              status: latest.status, ref: watch.ref, project: watch.project_id, web_url: latest.web_url,
            },
            orchestration: {
              source: "gitlab",
              event_kind: "pipeline_watch_completed",
              entity_type: "pipeline",
              entity_ref: `gitlab:${watch.project_id}:pipeline:${latest.id}`,
              correlation_key: `gitlab:${watch.project_id}:ref:${watch.ref}`,
              dedup_key: `gitlab:pipeline_watch:${latest.id}:${latest.status}`,
              importance_hint: latest.status === "failed" ? "high" : "normal",
              source_ref: latest.web_url,
              title_hint: `Pipeline #${latest.id} on "${watch.ref}": ${latest.status}`,
            },
          });
        }
      } catch (err) {
        this.logger.error({ err, project: watch.project_id, ref: watch.ref }, "pollWatchedPipelines error");
      }
    }
  }
}
