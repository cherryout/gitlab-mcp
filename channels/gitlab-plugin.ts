import type {
  ChannelPlugin,
  EventTypeDef,
  NotifyFn,
  OnWatchRegisteredFn,
  ToolDef,
  ToolCallResult,
} from "../channel-plugin.js";
import Database from "better-sqlite3";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { execSync } from "child_process";
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
  getMeta: Database.Statement;
  setMeta: Database.Statement;
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
      name: "gitlab_watch_branch",
      description: "Watch a GitLab pipeline on a specific branch. Routes pipeline events to this session. Auto-expires in 2h.",
      inputSchema: {
        type: "object",
        properties: {
          project: { type: "string", description: "Project path (e.g. 'banking/sdk.finance-backend')" },
          ref: { type: "string", description: "Branch name (e.g. 'int', 'main')" },
          expires_in_hours: { type: "number", description: "Expiration in hours (default: 2)" },
        },
        required: ["project", "ref"],
      },
    },
    {
      name: "gitlab_watch_mr",
      description: "Watch a GitLab merge request. Routes comments, approvals, and pipeline events for that MR to this session.",
      inputSchema: {
        type: "object",
        properties: {
          project: { type: "string", description: "Project path (e.g. 'banking/sdk.finance-backend')" },
          iid: { type: "number", description: "MR internal ID" },
        },
        required: ["project", "iid"],
      },
    },
    {
      name: "gitlab_watch_current_branch",
      description: "Watch the pipeline on the current git branch. Reads project path and branch from the working directory's git context. No arguments needed.",
      inputSchema: {
        type: "object",
        properties: {
          expires_in_hours: { type: "number", description: "Expiration in hours (default: 2)" },
        },
      },
    },
  ];

  private notify!: NotifyFn;
  private onWatchRegistered?: OnWatchRegisteredFn;
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
    const token = process.env.GITLAB_PERSONAL_ACCESS_TOKEN;
    if (!token) {
      throw new Error("GITLAB_PERSONAL_ACCESS_TOKEN env var is required for gitlab plugin");
    }
    this.token = token;
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
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY, value TEXT NOT NULL
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
      getProjects: this.db.prepare("SELECT id, path FROM projects"),
      upsertProject: this.db.prepare("INSERT INTO projects (id, path, discovered_at) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET path=excluded.path"),
      projectCount: this.db.prepare("SELECT COUNT(*) as count FROM projects"),
      getWatch: this.db.prepare("SELECT * FROM watches WHERE project_id = ? AND ref = ?"),
      insertWatch: this.db.prepare("INSERT OR IGNORE INTO watches (project_id, ref, pipeline_id, started_at) VALUES (?, ?, ?, ?)"),
      updateWatchPipeline: this.db.prepare("UPDATE watches SET pipeline_id = ? WHERE project_id = ? AND ref = ?"),
      deleteWatch: this.db.prepare("DELETE FROM watches WHERE project_id = ? AND ref = ?"),
      getAllWatches: this.db.prepare("SELECT * FROM watches"),
      getMeta: this.db.prepare("SELECT value FROM meta WHERE key = ?"),
      setMeta: this.db.prepare("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value"),
    };

    const seededRow = this.stmts.getMeta.get("seededTodos") as { value: string } | undefined;
    this.seededTodos = seededRow?.value === "1";

    this.logger.info({ api: this.apiUrl, pollInterval: this.pollIntervalMs, namespace: this.namespaceFilter || "(all)", dbPath, logPath }, "gitlab plugin initialized");
  }

  setOnWatchRegistered(fn: OnWatchRegisteredFn): void {
    this.onWatchRegistered = fn;
  }

  startPipelineWatch(projectId: string, ref: string): boolean {
    const existing = this.stmts.getWatch.get(projectId, ref) as { project_id: string } | undefined;
    if (existing) return false;
    this.stmts.insertWatch.run(projectId, ref, null, Date.now());
    this.logger.info({ projectId, ref }, "pipeline watch started");
    return true;
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
    if (name === "gitlab_watch_branch") {
      const project = args.project as string;
      const ref = args.ref as string;
      const expiresInHours = (args.expires_in_hours as number) ?? 2;
      return this.createBranchWatch(project, ref, expiresInHours);
    }

    if (name === "gitlab_watch_mr") {
      const project = args.project as string;
      const iid = args.iid as number;
      if (!this.onWatchRegistered) {
        return { content: [{ type: "text", text: "Error: orchestrator callback not configured" }] };
      }
      const resolved = await this.resolveProject(project);
      if (!resolved.ok) return resolved.errorResult;
      this.onWatchRegistered({
        watch_type: "merge-request",
        entity_type: "merge_request",
        entity_ref: `gitlab:${resolved.canonicalPath}:mr:${iid}`,
        correlation_key: `gitlab:${resolved.canonicalPath}:mr:${iid}`,
      });
      this.logger.info({ project: resolved.canonicalPath, iid }, "mr watch registered");
      return { content: [{ type: "text", text: `Watching MR !${iid} on ${resolved.canonicalPath}. Comments, approvals, and pipeline events will route to this session.` }] };
    }

    if (name === "gitlab_watch_current_branch") {
      const expiresInHours = (args.expires_in_hours as number) ?? 2;
      try {
        const remoteUrl = execSync("git config --get remote.origin.url", { encoding: "utf-8" }).trim();
        const branch = execSync("git branch --show-current", { encoding: "utf-8" }).trim();
        const project = this.parseGitLabProject(remoteUrl);
        if (!project) {
          return { content: [{ type: "text", text: `Error: could not parse GitLab project from remote URL: ${remoteUrl}` }] };
        }
        if (!branch) {
          return { content: [{ type: "text", text: "Error: no current branch (detached HEAD?)" }] };
        }
        return this.createBranchWatch(project, branch, expiresInHours);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { content: [{ type: "text", text: `Error reading git context: ${msg}` }] };
      }
    }

    return null;
  }

  private async resolveProject(project: string): Promise<
    | { ok: true; canonicalPath: string; projectId: number }
    | { ok: false; errorResult: ToolCallResult }
  > {
    if (process.env.GITLAB_PLUGIN_SKIP_VALIDATION === "true") {
      return { ok: true, canonicalPath: project, projectId: -1 };
    }
    try {
      const url = `${this.apiUrl}/projects/${encodeURIComponent(project)}`;
      const res = await nodeFetch(url, { headers: this.headers });
      if (res.status === 404) {
        const suggestions = this.suggestProjects(project);
        const hint = suggestions.length > 0
          ? `\n\nDid you mean:\n  ${suggestions.join("\n  ")}`
          : "";
        const msg = `GitLab project "${project}" not found (404). Check spelling or use the full namespaced path (e.g. group/subgroup/project).${hint}`;
        this.logger.warn({ project }, "watch rejected: project not found");
        return { ok: false, errorResult: { content: [{ type: "text", text: `Error: ${msg}` }] } };
      }
      if (res.status === 401 || res.status === 403) {
        const msg = `GitLab returned ${res.status} for "${project}" — check GITLAB_PERSONAL_ACCESS_TOKEN scope/validity.`;
        this.logger.warn({ project, status: res.status }, "watch rejected: auth failure");
        return { ok: false, errorResult: { content: [{ type: "text", text: `Error: ${msg}` }] } };
      }
      if (!res.ok) {
        const msg = `GitLab returned ${res.status} resolving "${project}".`;
        return { ok: false, errorResult: { content: [{ type: "text", text: `Error: ${msg}` }] } };
      }
      const proj = (await res.json()) as { id: number; path_with_namespace: string };
      this.stmts.upsertProject.run(proj.id, proj.path_with_namespace, Date.now());
      return { ok: true, canonicalPath: proj.path_with_namespace, projectId: proj.id };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.error({ err, project }, "project resolve failed");
      return { ok: false, errorResult: { content: [{ type: "text", text: `Error: failed to resolve "${project}": ${msg}` }] } };
    }
  }

  private async resolveBranch(projectId: number, projectPath: string, ref: string): Promise<
    | { ok: true }
    | { ok: false; errorResult: ToolCallResult }
  > {
    if (process.env.GITLAB_PLUGIN_SKIP_VALIDATION === "true") {
      return { ok: true };
    }
    try {
      const branchUrl = `${this.apiUrl}/projects/${projectId}/repository/branches/${encodeURIComponent(ref)}`;
      const res = await nodeFetch(branchUrl, { headers: this.headers });
      if (res.status === 200) return { ok: true };
      if (res.status === 404) {
        const suggestions = await this.suggestBranches(projectId, ref);
        const hint = suggestions.length > 0
          ? `\n\nDid you mean:\n  ${suggestions.join("\n  ")}`
          : "";
        const msg = `Branch "${ref}" not found on project "${projectPath}" (404).${hint}`;
        this.logger.warn({ projectPath, ref }, "watch rejected: branch not found");
        return { ok: false, errorResult: { content: [{ type: "text", text: `Error: ${msg}` }] } };
      }
      if (res.status === 401 || res.status === 403) {
        return { ok: false, errorResult: { content: [{ type: "text", text: `Error: GitLab returned ${res.status} resolving branch "${ref}" — check token scope.` }] } };
      }
      return { ok: false, errorResult: { content: [{ type: "text", text: `Error: GitLab returned ${res.status} resolving branch "${ref}".` }] } };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.error({ err, projectPath, ref }, "branch resolve failed");
      return { ok: false, errorResult: { content: [{ type: "text", text: `Error: failed to resolve branch "${ref}" on "${projectPath}": ${msg}` }] } };
    }
  }

  private async suggestBranches(projectId: number, query: string): Promise<string[]> {
    try {
      const url = `${this.apiUrl}/projects/${projectId}/repository/branches?search=${encodeURIComponent(query)}&per_page=5`;
      const res = await nodeFetch(url, { headers: this.headers });
      if (!res.ok) return [];
      const branches = (await res.json()) as Array<{ name: string }>;
      return branches.map((b) => b.name).slice(0, 5);
    } catch {
      return [];
    }
  }

  private suggestProjects(query: string): string[] {
    const known = this.db.prepare("SELECT path FROM projects").all() as Array<{ path: string }>;
    if (known.length === 0) return [];
    const q = query.toLowerCase();
    const tail = q.split("/").pop() || q;
    const scored = known
      .map((p) => {
        const path = p.path.toLowerCase();
        let score = 0;
        if (path === q) score += 100;
        if (path.endsWith("/" + tail)) score += 50;
        if (path.includes(tail)) score += 20;
        if (path.includes(q)) score += 10;
        return { path: p.path, score };
      })
      .filter((s) => s.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, 5)
      .map((s) => s.path);
    return scored;
  }

  private async createBranchWatch(project: string, ref: string, expiresInHours: number): Promise<ToolCallResult> {
    if (!this.onWatchRegistered) {
      return { content: [{ type: "text", text: "Error: orchestrator callback not configured" }] };
    }
    const resolved = await this.resolveProject(project);
    if (!resolved.ok) return resolved.errorResult;
    const canonical = resolved.canonicalPath;

    const branchCheck = await this.resolveBranch(resolved.projectId, canonical, ref);
    if (!branchCheck.ok) return branchCheck.errorResult;

    this.startPipelineWatch(canonical, ref);
    this.onWatchRegistered({
      watch_type: "pipeline-chain",
      entity_type: "pipeline",
      entity_ref: `gitlab:${canonical}:ref:${ref}`,
      correlation_key: `gitlab:${canonical}:ref:${ref}`,
      expires_at: Date.now() + expiresInHours * 60 * 60 * 1000,
    });
    const note = canonical !== project ? ` (resolved from "${project}")` : "";
    this.logger.info({ project: canonical, originalProject: project, ref, expiresInHours }, "branch watch registered");
    return { content: [{ type: "text", text: `Watching pipeline on branch "${ref}" for project ${canonical}${note}. Expires in ${expiresInHours}h.` }] };
  }

  private parseGitLabProject(remoteUrl: string): string | null {
    // git@host:namespace/project.git OR https://host/namespace/project.git
    const sshMatch = remoteUrl.match(/:(.+?)(?:\.git)?$/);
    if (remoteUrl.startsWith("git@") && sshMatch) {
      return sshMatch[1].replace(/\.git$/, "");
    }
    const httpsMatch = remoteUrl.match(/https?:\/\/[^/]+\/(.+?)(?:\.git)?$/);
    if (httpsMatch) {
      return httpsMatch[1].replace(/\.git$/, "");
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

      if (isFirstRun) {
        this.logger.info({ seeded: todos.length }, "todos seeded (first run)");
        this.stmts.setMeta.run("seededTodos", "1");
      }
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

  private async pollPipelines(): Promise<void> {
    await this.discoverProjects();
    const projects = this.stmts.getProjects.all() as Array<{ id: number; path: string | null }>;
    if (projects.length === 0) return;

    this.logger.debug({ projects: projects.length }, "polling pipelines");
    const now = Date.now();

    for (const project of projects) {
      const projectKey = project.path || String(project.id);
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
            this.logger.info({ pipelineId: pipeline.id, ref: pipeline.ref, oldStatus, newStatus: pipeline.status, projectKey }, "pipeline_status_changed event");
            await this.notify({
              content: `Pipeline #${pipeline.id} on "${pipeline.ref}": ${oldStatus} → ${pipeline.status} (${pipeline.sha.slice(0, 8)})`,
              meta: {
                event_type: "pipeline_status_changed", pipeline_id: String(pipeline.id),
                old_status: oldStatus, new_status: pipeline.status, ref: pipeline.ref,
                project: projectKey, project_id: String(project.id), web_url: pipeline.web_url,
              },
              orchestration: {
                source: "gitlab",
                event_kind: "pipeline_status_changed",
                entity_type: "pipeline",
                entity_ref: `gitlab:${projectKey}:pipeline:${pipeline.id}`,
                correlation_key: `gitlab:${projectKey}:ref:${pipeline.ref}`,
                dedup_key: `gitlab:pipeline:${pipeline.id}:${pipeline.status}:${pipeline.updated_at}`,
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
            dedup_key: `gitlab:watch_expired:${watch.project_id}:${watch.ref}:${watch.started_at}`,
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
              dedup_key: `gitlab:pipeline_watch:${latest.id}:${latest.status}:${latest.updated_at}`,
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
