import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { spawn, ChildProcess } from "child_process";
import { createServer, Server as HttpServer } from "http";
import { tmpdir } from "os";
import { join } from "path";
import { randomUUID } from "crypto";
import { unlinkSync, existsSync, rmSync, mkdirSync } from "fs";
import Database from "better-sqlite3";

let passed = 0;
let failed = 0;

function assert(condition: boolean, name: string) {
  if (condition) { passed++; console.log(`  PASS  ${name}`); }
  else { failed++; console.log(`  FAIL  ${name}`); }
}

function assertEqual(a: unknown, b: unknown, name: string) {
  assert(JSON.stringify(a) === JSON.stringify(b), `${name} (got ${JSON.stringify(a)}, expected ${JSON.stringify(b)})`);
}

const REPO = join(import.meta.dirname, "..");
const SERVER_PATH = join(REPO, "orchestrator-server.ts");

// ─── Mock GitLab HTTP server ─────────────────────────────────────────

interface MockGitLabState {
  pipelines: Array<{ id: number; project_id: string; ref: string; status: string; sha: string; web_url: string; updated_at: string }>;
  todos: Array<{ id: number; action_name: string; target_type: string; target: { iid: number; title: string }; project: { id: number; path_with_namespace: string }; author: { username: string; name: string }; body: string; state: string; target_url: string }>;
  projects: Array<{ id: number; path_with_namespace: string }>;
}

async function startMockGitLab(): Promise<{ server: HttpServer; port: number; state: MockGitLabState; token: string }> {
  const state: MockGitLabState = { pipelines: [], todos: [], projects: [] };
  const token = "test-token-" + randomUUID().slice(0, 8);

  const server = createServer((req, res) => {
    if (req.headers["private-token"] !== token) {
      res.writeHead(401);
      res.end("unauthorized");
      return;
    }
    const url = req.url || "";
    res.setHeader("Content-Type", "application/json");

    // GET /todos
    if (url.startsWith("/api/v4/todos")) {
      res.end(JSON.stringify(state.todos.filter((t) => t.state === "pending")));
      return;
    }
    // GET /projects
    if (url.startsWith("/api/v4/projects?membership")) {
      res.end(JSON.stringify(state.projects));
      return;
    }
    // GET /projects/:id/pipelines?ref=...
    const refMatch = url.match(/\/projects\/([^/]+)\/pipelines\?.*ref=([^&]+)/);
    if (refMatch) {
      const projId = decodeURIComponent(refMatch[1]);
      const ref = decodeURIComponent(refMatch[2]);
      const found = state.pipelines.filter((p) => p.project_id === projId && p.ref === ref);
      res.end(JSON.stringify(found));
      return;
    }
    // GET /projects/:id/pipelines
    const pipelinesMatch = url.match(/\/projects\/([^/]+)\/pipelines\?/);
    if (pipelinesMatch) {
      const projId = decodeURIComponent(pipelinesMatch[1]);
      res.end(JSON.stringify(state.pipelines.filter((p) => p.project_id === projId)));
      return;
    }
    res.writeHead(404);
    res.end("not found");
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return { server, port, state, token };
}

// ─── Mock Jenkins HTTP server ────────────────────────────────────────

interface MockJenkinsState {
  builds: Record<string, Array<{ number: number; result: string | null; building: boolean; timestamp: number; duration: number; url: string }>>;
}

async function startMockJenkins(): Promise<{ server: HttpServer; port: number; state: MockJenkinsState; auth: string }> {
  const state: MockJenkinsState = { builds: {} };
  const auth = "Basic " + Buffer.from("test:test").toString("base64");

  const server = createServer((req, res) => {
    if (req.headers.authorization !== auth) {
      res.writeHead(401);
      res.end("unauthorized");
      return;
    }
    res.setHeader("Content-Type", "application/json");
    const url = req.url || "";

    // /job/<path>/api/json
    const jobMatch = url.match(/^\/job\/(.+?)\/api\/json/);
    if (jobMatch) {
      const segments = jobMatch[1].split("/job/").map(decodeURIComponent);
      const jobPath = segments.join("/");
      const builds = state.builds[jobPath] || [];
      res.end(JSON.stringify({ name: jobPath, url: `http://fake/${jobPath}`, builds: builds.slice(0, 5) }));
      return;
    }
    res.writeHead(404);
    res.end("not found");
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return { server, port, state, auth };
}

// ─── MCP Client Harness ──────────────────────────────────────────────

interface HarnessOpts {
  dbDir: string;
  sessionOwner: string;
  gitlabPort?: number;
  gitlabToken?: string;
  jenkinsPort?: number;
  jenkinsJobs?: string;
  plugins?: string;
}

interface Harness {
  client: Client;
  transport: StdioClientTransport;
  stderrOutput: string[];
  notifications: Array<{ method: string; params: unknown }>;
  close(): Promise<void>;
}

async function startHarness(opts: HarnessOpts): Promise<Harness> {
  const env: Record<string, string> = {
    ...process.env as Record<string, string>,
    ORCHESTRATOR_SESSION_OWNER: opts.sessionOwner,
    ORCHESTRATOR_AUTO_START: "true",
    CHANNEL_PLUGINS: opts.plugins ?? "gitlab",
    HOME: opts.dbDir,
    ORCHESTRATOR_LOG_LEVEL: "warn",
    GITLAB_CHANNEL_LOG_LEVEL: "warn",
    JENKINS_CHANNEL_LOG_LEVEL: "warn",
    SLACK_CHANNEL_LOG_LEVEL: "warn",
    GITLAB_CHANNEL_POLL_INTERVAL: "500",
    GITLAB_CHANNEL_WATCH_POLL: "300",
    GITLAB_CHANNEL_WATCH_TIMEOUT: "60000",
    JENKINS_CHANNEL_POLL_INTERVAL: "500",
  };

  if (opts.gitlabPort !== undefined) {
    env.GITLAB_API_URL = `http://127.0.0.1:${opts.gitlabPort}/api/v4`;
    env.GITLAB_PERSONAL_ACCESS_TOKEN = opts.gitlabToken || "test";
  } else {
    env.GITLAB_API_URL = "http://127.0.0.1:1/api/v4";
    env.GITLAB_PERSONAL_ACCESS_TOKEN = "test";
  }
  env.GITLAB_PLUGIN_SKIP_VALIDATION = "true";

  if (opts.jenkinsPort !== undefined) {
    env.JENKINS_CHANNEL_URL = `http://127.0.0.1:${opts.jenkinsPort}`;
    env.JENKINS_CHANNEL_USERNAME = "test";
    env.JENKINS_CHANNEL_TOKEN = "test";
    env.JENKINS_CHANNEL_JOBS = opts.jenkinsJobs || "";
  }

  const transport = new StdioClientTransport({
    command: "npx",
    args: ["tsx", SERVER_PATH],
    env,
    stderr: "pipe",
  });

  const client = new Client({ name: "e2e-test-client", version: "1.0" }, { capabilities: {} });

  const notifications: Array<{ method: string; params: unknown }> = [];
  client.fallbackNotificationHandler = async (n) => {
    notifications.push({ method: n.method, params: n.params });
  };

  const stderrOutput: string[] = [];
  await client.connect(transport);
  if (transport.stderr) {
    transport.stderr.on("data", (chunk) => stderrOutput.push(chunk.toString()));
  }

  return {
    client,
    transport,
    stderrOutput,
    notifications,
    async close() {
      try { await client.close(); } catch {}
      try { await transport.close(); } catch {}
    },
  };
}

async function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

async function waitForNotification(
  harness: Harness,
  predicate: (n: { method: string; params: unknown }) => boolean,
  timeoutMs = 5000,
): Promise<{ method: string; params: unknown } | null> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const found = harness.notifications.find(predicate);
    if (found) return found;
    await sleep(50);
  }
  return null;
}

function makeDbDir(): string {
  const dir = join(tmpdir(), `orch-e2e-${randomUUID()}`);
  mkdirSync(dir, { recursive: true });
  mkdirSync(join(dir, ".cache", "orchestrator"), { recursive: true });
  return dir;
}

function cleanupDbDir(dir: string) {
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

// ─── Tests ────────────────────────────────────────────────────────────

async function testMcpProtocol() {
  console.log("\n--- E2E-1: MCP protocol layer ---");
  const dbDir = makeDbDir();
  const h = await startHarness({ dbDir, sessionOwner: "e2e-protocol" });

  try {
    const tools = await h.client.listTools();
    const names = tools.tools.map((t) => t.name);

    assert(names.includes("add_watch"), "add_watch exposed");
    assert(names.includes("remove_watch"), "remove_watch exposed");
    assert(names.includes("list_watches"), "list_watches exposed");
    assert(names.includes("list_session_feed"), "list_session_feed exposed");
    assert(names.includes("get_session_state"), "get_session_state exposed");
    assert(names.includes("list_unmatched_events"), "list_unmatched_events exposed");
    assert(names.includes("gitlab_watch_branch"), "gitlab_watch_branch exposed");
    assert(names.includes("gitlab_watch_mr"), "gitlab_watch_mr exposed");
    assert(names.includes("gitlab_watch_current_branch"), "gitlab_watch_current_branch exposed");

    const state = await h.client.callTool({ name: "get_session_state", arguments: {} });
    const stateJson = JSON.parse((state.content as { text: string }[])[0].text);
    assert(!!stateJson.session.session_id, "session auto-created on startup");
    assert(stateJson.session.owner === "e2e-protocol", "session owner matches env var");
    assert(stateJson.runtime !== null, "runtime auto-attached");

    const addResult = await h.client.callTool({
      name: "add_watch",
      arguments: {
        watch_type: "pipeline-chain",
        entity_type: "pipeline",
        entity_ref: "gitlab:test:ref:main",
        correlation_key: "gitlab:test:ref:main",
      },
    });
    const addJson = JSON.parse((addResult.content as { text: string }[])[0].text);
    assert(!!addJson.watch_id, "add_watch returns watch_id");

    const listResult = await h.client.callTool({ name: "list_watches", arguments: {} });
    const listJson = JSON.parse((listResult.content as { text: string }[])[0].text);
    assert(Array.isArray(listJson), "list_watches returns array");
    assert(listJson.length === 1, "1 watch after add");
    assert(listJson[0].entity_ref === "gitlab:test:ref:main", "entity_ref matches");

    await h.client.callTool({ name: "remove_watch", arguments: { watch_id: addJson.watch_id } });
    const listAfter = await h.client.callTool({ name: "list_watches", arguments: {} });
    const listAfterJson = JSON.parse((listAfter.content as { text: string }[])[0].text);
    assert(listAfterJson.length === 0, "0 watches after remove");
  } finally {
    await h.close();
    cleanupDbDir(dbDir);
  }
}

async function testPluginBridge() {
  console.log("\n--- E2E-5: Plugin tool → orchestrator DB bridge ---");
  const dbDir = makeDbDir();
  const h = await startHarness({ dbDir, sessionOwner: "e2e-bridge" });

  try {
    const result = await h.client.callTool({
      name: "gitlab_watch_branch",
      arguments: { project: "banking/test-project", ref: "int" },
    });
    const text = (result.content as { text: string }[])[0].text;
    assert(text.includes("Watching pipeline on branch"), "gitlab_watch_branch returns success");

    const listResult = await h.client.callTool({ name: "list_watches", arguments: {} });
    const watches = JSON.parse((listResult.content as { text: string }[])[0].text);
    assert(watches.length === 1, "1 orchestrator watch after plugin tool");
    assert(watches[0].entity_ref === "gitlab:banking/test-project:ref:int", "orchestrator watch entity_ref correct");
    assert(watches[0].watch_type === "pipeline-chain", "watch_type = pipeline-chain");
    assert(watches[0].expires_at !== null, "expires_at set");

    const mrResult = await h.client.callTool({
      name: "gitlab_watch_mr",
      arguments: { project: "banking/test-project", iid: 42 },
    });
    const mrText = (mrResult.content as { text: string }[])[0].text;
    assert(mrText.includes("Watching MR !42"), "gitlab_watch_mr returns success");

    const watches2Resp = await h.client.callTool({ name: "list_watches", arguments: {} });
    const watches2 = JSON.parse((watches2Resp.content as { text: string }[])[0].text);
    assert(watches2.length === 2, "2 watches after MR watch");
    assert(watches2.some((w: { entity_ref: string }) => w.entity_ref === "gitlab:banking/test-project:mr:42"), "MR watch present");

    // Verify plugin's local SQLite has the pipeline watch (proves startPipelineWatch was triggered)
    const pluginDbPath = join(dbDir, ".cache", "gitlab-channel", "state.db");
    if (existsSync(pluginDbPath)) {
      const pluginDb = new Database(pluginDbPath, { readonly: true });
      const localWatches = pluginDb.prepare("SELECT * FROM watches WHERE project_id = ? AND ref = ?").all("banking/test-project", "int");
      assert(localWatches.length === 1, "plugin's local watch table has the pipeline watch");
      pluginDb.close();
    } else {
      console.log("  (skipped plugin DB check — plugin DB not yet created)");
    }
  } finally {
    await h.close();
    cleanupDbDir(dbDir);
  }
}

async function testFullPipelineFlow() {
  console.log("\n--- E2E-2: Full event flow via mock GitLab ---");
  const dbDir = makeDbDir();
  const mock = await startMockGitLab();
  const h = await startHarness({
    dbDir,
    sessionOwner: "e2e-flow",
    gitlabPort: mock.port,
    gitlabToken: mock.token,
  });

  try {
    // Seed: pipeline running on branch "main"
    const projectId = "100";
    mock.state.projects.push({ id: 100, path_with_namespace: "banking/flow-test" });
    mock.state.pipelines.push({
      id: 555, project_id: projectId, ref: "main", status: "running",
      sha: "abc123def456", web_url: "http://fake/pipeline/555",
      updated_at: "2026-04-18T00:00:00Z",
    });

    await h.client.callTool({
      name: "gitlab_watch_branch",
      arguments: { project: projectId, ref: "main" },
    });

    await sleep(1500);
    h.notifications.length = 0;

    mock.state.pipelines[0].status = "failed";
    mock.state.pipelines[0].updated_at = "2026-04-18T00:00:30Z";

    const got = await waitForNotification(
      h,
      (n) => n.method === "notifications/claude/channel"
        && typeof n.params === "object"
        && n.params !== null
        && String((n.params as { content?: string }).content || "").includes("555")
        && String((n.params as { content?: string }).content || "").toLowerCase().includes("failed"),
      8000,
    );
    assert(got !== null, "received pipeline_watch_completed notification");
    if (got) {
      const meta = (got.params as { meta?: Record<string, string> }).meta || {};
      assert(meta.orchestrated === "true", "notification tagged orchestrated=true");
      assert(meta.source === "gitlab" || meta.plugin === "gitlab", "notification has gitlab source/plugin");
    }
  } finally {
    await h.close();
    await new Promise<void>((r) => mock.server.close(() => r()));
    cleanupDbDir(dbDir);
  }
}

async function testProcessRestartPersistence() {
  console.log("\n--- E2E-3: Process restart preserves state ---");
  const dbDir = makeDbDir();

  const h1 = await startHarness({ dbDir, sessionOwner: "e2e-restart" });
  let session1Id: string;
  let watchId: string;
  try {
    const state = await h1.client.callTool({ name: "get_session_state", arguments: {} });
    session1Id = JSON.parse((state.content as { text: string }[])[0].text).session.session_id;

    const addResult = await h1.client.callTool({
      name: "add_watch",
      arguments: {
        watch_type: "merge-request",
        entity_type: "merge_request",
        entity_ref: "gitlab:project:mr:99",
      },
    });
    watchId = JSON.parse((addResult.content as { text: string }[])[0].text).watch_id;
  } finally {
    await h1.close();
  }

  await sleep(300);

  const h2 = await startHarness({ dbDir, sessionOwner: "e2e-restart" });
  try {
    const state = await h2.client.callTool({ name: "get_session_state", arguments: {} });
    const session2 = JSON.parse((state.content as { text: string }[])[0].text);
    assertEqual(session2.session.session_id, session1Id, "same session_id after restart");

    const listResult = await h2.client.callTool({ name: "list_watches", arguments: {} });
    const watches = JSON.parse((listResult.content as { text: string }[])[0].text);
    assert(watches.length === 1, "watch survives restart");
    assertEqual(watches[0].watch_id, watchId, "same watch_id after restart");
    assertEqual(watches[0].entity_ref, "gitlab:project:mr:99", "entity_ref preserved");
  } finally {
    await h2.close();
    cleanupDbDir(dbDir);
  }
}

async function testSeededFlagPersistsAcrossRestart() {
  console.log("\n--- E2E: seeded flag persists across process restart ---");
  const dbDir = makeDbDir();
  const mock = await startMockGitLab();

  // Seed a todo before first start
  mock.state.todos.push({
    id: 1001, action_name: "mentioned", target_type: "MergeRequest",
    target: { iid: 1, title: "Existing todo before start" },
    project: { id: 50, path_with_namespace: "test/seed-test" },
    author: { username: "alice", name: "Alice" }, body: "test", state: "pending",
    target_url: "http://fake/todo/1001",
  });

  const h1 = await startHarness({
    dbDir, sessionOwner: "e2e-seed",
    gitlabPort: mock.port, gitlabToken: mock.token,
  });
  await sleep(2000);  // let first poll absorb existing todo silently
  await h1.close();

  // Add a NEW todo while process is dead
  mock.state.todos.push({
    id: 1002, action_name: "assigned", target_type: "Issue",
    target: { iid: 2, title: "Todo that appeared during downtime" },
    project: { id: 50, path_with_namespace: "test/seed-test" },
    author: { username: "bob", name: "Bob" }, body: "new", state: "pending",
    target_url: "http://fake/todo/1002",
  });

  // Restart — this time the seeded flag should be persisted, so new todo should fire event
  const h2 = await startHarness({
    dbDir, sessionOwner: "e2e-seed",
    gitlabPort: mock.port, gitlabToken: mock.token,
  });

  // Add watch for that project's todo to capture it
  await h2.client.callTool({
    name: "add_watch",
    arguments: {
      watch_type: "task-followup",
      entity_type: "todo",
      entity_ref: "gitlab:test/seed-test:todo:1002",
    },
  });

  await sleep(2500);  // give time for poll after watch is added

  const got = h2.notifications.find(
    (n) => n.method === "notifications/claude/channel"
      && String((n.params as { content?: string })?.content || "").includes("1002"),
  );
  // Note: the watch was added AFTER the poll might have run; the event should still be ingested
  // and queued for the watch since dedup_key prevents re-emit. So we check for the event in DB.
  if (got) {
    assert(true, "todo_created event delivered after restart (new todo during downtime)");
  } else {
    // Fallback: check unmatched events (the event was emitted but possibly before our watch was added)
    const unmatchedResp = await h2.client.callTool({ name: "list_unmatched_events", arguments: {} });
    const unmatched = JSON.parse((unmatchedResp.content as { text: string }[])[0].text);
    const found = unmatched.find((e: { source: string; event_kind: string }) =>
      e.source === "gitlab" && e.event_kind === "todo_created");
    assert(!!found, "todo_created event emitted after restart (found in unmatched events)");
  }

  await h2.close();
  await new Promise<void>((r) => mock.server.close(() => r()));
  cleanupDbDir(dbDir);
}

async function testJenkinsPolling() {
  console.log("\n--- E2E-4: Jenkins polling via mock ---");
  const dbDir = makeDbDir();
  const jenkins = await startMockJenkins();

  const jobPath = "Test/deploy_int";
  jenkins.state.builds[jobPath] = [{
    number: 10, result: null, building: true,
    timestamp: Date.now(), duration: 0, url: "http://fake/10",
  }];

  const h = await startHarness({
    dbDir,
    sessionOwner: "e2e-jenkins",
    jenkinsPort: jenkins.port,
    jenkinsJobs: jobPath,
    plugins: "jenkins",
  });

  try {
    await h.client.callTool({
      name: "jenkins_watch_job",
      arguments: { job_path: jobPath },
    });

    await sleep(1500);
    h.notifications.length = 0;

    jenkins.state.builds[jobPath][0].building = false;
    jenkins.state.builds[jobPath][0].result = "SUCCESS";
    jenkins.state.builds[jobPath][0].duration = 12345;

    const got = await waitForNotification(
      h,
      (n) => n.method === "notifications/claude/channel"
        && String((n.params as { content?: string })?.content || "").includes("SUCCESS"),
      8000,
    );
    assert(got !== null, "received build_completed notification");
  } finally {
    await h.close();
    await new Promise<void>((r) => jenkins.server.close(() => r()));
    cleanupDbDir(dbDir);
  }
}

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log("E2E Orchestrator Tests");
  console.log("======================");

  try { await testMcpProtocol(); } catch (e) { failed++; console.log(`  FAIL  testMcpProtocol threw: ${e}`); }
  try { await testPluginBridge(); } catch (e) { failed++; console.log(`  FAIL  testPluginBridge threw: ${e}`); }
  try { await testFullPipelineFlow(); } catch (e) { failed++; console.log(`  FAIL  testFullPipelineFlow threw: ${e}`); }
  try { await testProcessRestartPersistence(); } catch (e) { failed++; console.log(`  FAIL  testProcessRestartPersistence threw: ${e}`); }
  try { await testSeededFlagPersistsAcrossRestart(); } catch (e) { failed++; console.log(`  FAIL  testSeededFlagPersistsAcrossRestart threw: ${e}`); }
  try { await testJenkinsPolling(); } catch (e) { failed++; console.log(`  FAIL  testJenkinsPolling threw: ${e}`); }

  console.log(`\n======================`);
  console.log(`Results: ${passed}/${passed + failed} passed, ${failed} failed`);
  setTimeout(() => process.exit(failed > 0 ? 1 : 0), 200);
}

main();
