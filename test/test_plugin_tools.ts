import { GitLabChannelPlugin } from "../channels/gitlab-plugin.js";
import { JenkinsChannelPlugin } from "../channels/jenkins-plugin.js";
import { SlackChannelPlugin } from "../channels/slack-plugin.js";
import type { WatchRegistration } from "../channel-plugin.js";
import { tmpdir } from "os";
import { join } from "path";
import { randomUUID } from "crypto";
import { unlinkSync } from "fs";

let passed = 0;
let failed = 0;

function assert(condition: boolean, name: string) {
  if (condition) {
    passed++;
    console.log(`  PASS  ${name}`);
  } else {
    failed++;
    console.log(`  FAIL  ${name}`);
  }
}

async function run() {
  console.log("Plugin Tool Tests\n=================\n");

  // ─── GitLab Plugin ──────────────────────────────────────────────────
  console.log("--- GitLab Plugin ---");

  const gitlabDbPath = join(tmpdir(), `gitlab-test-${randomUUID()}.db`);
  process.env.GITLAB_PERSONAL_ACCESS_TOKEN = "test-token";
  process.env.GITLAB_CHANNEL_DB_PATH = gitlabDbPath;
  process.env.GITLAB_CHANNEL_LOG_PATH = join(tmpdir(), `gitlab-test-${randomUUID()}.log`);
  process.env.GITLAB_PLUGIN_SKIP_VALIDATION = "true";

  const gitlabPlugin = new GitLabChannelPlugin();
  const gitlabWatches: WatchRegistration[] = [];
  await gitlabPlugin.init(async () => {});
  gitlabPlugin.setOnWatchRegistered!((w) => gitlabWatches.push(w));

  // 1. gitlab_watch_branch
  const branchResult = await gitlabPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/sdk.finance-backend",
    ref: "int",
  });
  assert(branchResult !== null, "gitlab_watch_branch returned result");
  assert(gitlabWatches.length === 1, "watch callback fired once");
  assert(gitlabWatches[0].watch_type === "pipeline-chain", "watch_type is pipeline-chain");
  assert(gitlabWatches[0].entity_type === "pipeline", "entity_type is pipeline");
  assert(gitlabWatches[0].entity_ref === "gitlab:banking/sdk.finance-backend:ref:int", "entity_ref formatted correctly");
  assert(gitlabWatches[0].correlation_key === "gitlab:banking/sdk.finance-backend:ref:int", "correlation_key matches");
  assert(gitlabWatches[0].expires_at !== undefined, "expires_at set");
  assert((gitlabWatches[0].expires_at! - Date.now()) > 1.9 * 60 * 60 * 1000, "expires_at ~2h from now (default)");

  // 2. gitlab_watch_branch with custom expiration
  gitlabWatches.length = 0;
  await gitlabPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/payment-app",
    ref: "main",
    expires_in_hours: 6,
  });
  assert(gitlabWatches.length === 1, "custom expiration: watch created");
  const expiresIn = (gitlabWatches[0].expires_at! - Date.now()) / (60 * 60 * 1000);
  assert(expiresIn > 5.9 && expiresIn < 6.1, `custom expires_in_hours=6 (got ${expiresIn.toFixed(2)}h)`);

  // 3. gitlab_watch_mr
  gitlabWatches.length = 0;
  const mrResult = await gitlabPlugin.handleToolCall("gitlab_watch_mr", {
    project: "banking/sdk.finance-backend",
    iid: 1234,
  });
  assert(mrResult !== null, "gitlab_watch_mr returned result");
  assert(gitlabWatches.length === 1, "mr watch callback fired");
  assert(gitlabWatches[0].watch_type === "merge-request", "mr watch_type correct");
  assert(gitlabWatches[0].entity_type === "merge_request", "mr entity_type correct");
  assert(gitlabWatches[0].entity_ref === "gitlab:banking/sdk.finance-backend:mr:1234", "mr entity_ref correct");

  // 4. gitlab_watch_current_branch — may succeed or fail depending on cwd
  gitlabWatches.length = 0;
  const currentResult = await gitlabPlugin.handleToolCall("gitlab_watch_current_branch", {});
  assert(currentResult !== null, "gitlab_watch_current_branch returned result");
  // If we're in a GitLab repo, it'll succeed. If not, it returns an error message.
  const currentText = currentResult!.content[0].text;
  const succeeded = currentText.includes("Watching pipeline");
  if (succeeded) {
    assert(gitlabWatches.length === 1, "current_branch: watch created");
    assert(gitlabWatches[0].entity_ref.startsWith("gitlab:"), "current_branch: entity_ref is gitlab-prefixed");
  } else {
    assert(currentText.startsWith("Error"), "current_branch: returns error when not in GitLab repo");
  }

  // 5. unknown tool
  const unknownResult = await gitlabPlugin.handleToolCall("gitlab_nonexistent", {});
  assert(unknownResult === null, "unknown gitlab tool returns null");

  // 6. Tools listed correctly
  assert(gitlabPlugin.tools.length === 3, `gitlab has 3 tools (got ${gitlabPlugin.tools.length})`);
  const toolNames = gitlabPlugin.tools.map((t) => t.name);
  assert(toolNames.includes("gitlab_watch_branch"), "gitlab_watch_branch listed");
  assert(toolNames.includes("gitlab_watch_mr"), "gitlab_watch_mr listed");
  assert(toolNames.includes("gitlab_watch_current_branch"), "gitlab_watch_current_branch listed");

  await gitlabPlugin.stop();
  try { unlinkSync(gitlabDbPath); } catch {}

  // ─── Project validation (404, auth, canonicalization, suggestions) ──
  console.log("\n--- GitLab Plugin: project validation ---");

  const validationDbPath = join(tmpdir(), `gitlab-validation-${randomUUID()}.db`);
  process.env.GITLAB_CHANNEL_DB_PATH = validationDbPath;
  process.env.GITLAB_CHANNEL_LOG_PATH = join(tmpdir(), `gitlab-validation-${randomUUID()}.log`);
  process.env.GITLAB_PLUGIN_SKIP_VALIDATION = "false";

  const port = 38000 + Math.floor(Math.random() * 1000);
  process.env.GITLAB_API_URL = `http://127.0.0.1:${port}/api/v4`;

  const projectsDb: Record<string, { id: number; path_with_namespace: string }> = {
    "banking%2Famaiz-3.0%2Fsdk.finance-backend": { id: 5950, path_with_namespace: "banking/amaiz-3.0/sdk.finance-backend" },
    "banking%2Frename-old": { id: 7777, path_with_namespace: "banking/rename-new" },
  };
  const branchesByProject: Record<number, string[]> = {
    5950: ["int", "main", "feature/x"],
    7777: ["main", "develop"],
  };
  const { createServer } = await import("http");
  let auth401 = false;
  const mockServer = createServer((req, res) => {
    if (auth401) { res.statusCode = 401; res.end('{"message":"401 Unauthorized"}'); return; }
    // /projects/:id/repository/branches/:name
    const branchOne = req.url?.match(/^\/api\/v4\/projects\/(\d+)\/repository\/branches\/([^?]+)/);
    if (branchOne) {
      const pid = parseInt(branchOne[1], 10);
      const name = decodeURIComponent(branchOne[2]);
      const branches = branchesByProject[pid] || [];
      if (branches.includes(name)) {
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ name, default: name === "main" }));
      } else {
        res.statusCode = 404; res.end('{"message":"404 Branch Not Found"}');
      }
      return;
    }
    // /projects/:id/repository/branches?search=...
    const branchSearch = req.url?.match(/^\/api\/v4\/projects\/(\d+)\/repository\/branches\?search=([^&]+)/);
    if (branchSearch) {
      const pid = parseInt(branchSearch[1], 10);
      const q = decodeURIComponent(branchSearch[2]).toLowerCase();
      const branches = (branchesByProject[pid] || []).filter((b) => b.toLowerCase().includes(q)).slice(0, 5);
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(branches.map((name) => ({ name }))));
      return;
    }
    // /projects/:path
    const proj = req.url?.match(/^\/api\/v4\/projects\/([^/?]+)$/);
    if (proj && projectsDb[proj[1]]) {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(projectsDb[proj[1]]));
      return;
    }
    res.statusCode = 404; res.end('{"message":"404 Project Not Found"}');
  });
  await new Promise<void>((r) => mockServer.listen(port, "127.0.0.1", r));

  const validatedPlugin = new GitLabChannelPlugin();
  const validatedWatches: WatchRegistration[] = [];
  await validatedPlugin.init(async () => {});
  validatedPlugin.setOnWatchRegistered!((w) => validatedWatches.push(w));

  // Seed cache so suggestions work
  validatedPlugin["stmts"].upsertProject.run(5950, "banking/amaiz-3.0/sdk.finance-backend", Date.now());
  validatedPlugin["stmts"].upsertProject.run(6106, "banking/amaiz-3.0/integrations", Date.now());

  // a) 404 → error with suggestion
  const r404 = await validatedPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/sdk.finance-backend", ref: "int",
  });
  const r404Text = r404!.content[0].text;
  assert(r404Text.startsWith("Error:"), "validation: 404 returns Error");
  assert(r404Text.includes("not found"), "validation: 404 message mentions 'not found'");
  assert(r404Text.includes("banking/amaiz-3.0/sdk.finance-backend"), "validation: 404 includes suggestion from cache");
  assert(validatedWatches.length === 0, "validation: 404 does not register a watch");

  // b) 200 → canonical path used; renamed project resolved through redirect
  validatedWatches.length = 0;
  const rRename = await validatedPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/rename-old", ref: "main",
  });
  assert(validatedWatches.length === 1, "validation: rename resolved to watch");
  assert(validatedWatches[0].entity_ref === "gitlab:banking/rename-new:ref:main", "validation: entity_ref uses canonical path, not user input");
  assert(rRename!.content[0].text.includes("rename-new"), "validation: success message shows canonical path");
  assert(rRename!.content[0].text.includes('resolved from "banking/rename-old"'), "validation: success message notes resolution");

  // c) Valid path → no resolution note
  validatedWatches.length = 0;
  const rValid = await validatedPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/amaiz-3.0/sdk.finance-backend", ref: "int",
  });
  assert(validatedWatches.length === 1, "validation: valid path registers watch");
  assert(validatedWatches[0].entity_ref === "gitlab:banking/amaiz-3.0/sdk.finance-backend:ref:int", "validation: valid path entity_ref correct");
  assert(!rValid!.content[0].text.includes("resolved from"), "validation: no resolution note on canonical input");

  // d) 401 → distinct error
  validatedWatches.length = 0;
  auth401 = true;
  const r401 = await validatedPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/amaiz-3.0/sdk.finance-backend", ref: "int",
  });
  const r401Text = r401!.content[0].text;
  assert(r401Text.startsWith("Error:"), "validation: 401 returns Error");
  assert(r401Text.includes("401"), "validation: 401 message includes status");
  assert(r401Text.includes("GITLAB_PERSONAL_ACCESS_TOKEN"), "validation: 401 message hints at token issue");
  assert(validatedWatches.length === 0, "validation: 401 does not register a watch");
  auth401 = false;

  // e) gitlab_watch_mr also validated
  validatedWatches.length = 0;
  const rMr404 = await validatedPlugin.handleToolCall("gitlab_watch_mr", {
    project: "banking/sdk.finance-backend", iid: 99,
  });
  assert(rMr404!.content[0].text.startsWith("Error:"), "validation: mr watch on bad path returns Error");
  assert(validatedWatches.length === 0, "validation: bad-path mr watch does not register");

  // f) Project exists but branch doesn't → 404 with branch suggestions
  validatedWatches.length = 0;
  const rBadBranch = await validatedPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/amaiz-3.0/sdk.finance-backend", ref: "intt",
  });
  const rBadBranchText = rBadBranch!.content[0].text;
  assert(rBadBranchText.startsWith("Error:"), "branch validation: missing branch returns Error");
  assert(rBadBranchText.includes('Branch "intt" not found'), "branch validation: error message mentions the branch");
  assert(rBadBranchText.includes("int"), "branch validation: suggests close-match branch from API search");
  assert(validatedWatches.length === 0, "branch validation: bad-branch watch does not register");

  // g) Project + branch both valid → no extra error
  validatedWatches.length = 0;
  const rGood = await validatedPlugin.handleToolCall("gitlab_watch_branch", {
    project: "banking/amaiz-3.0/sdk.finance-backend", ref: "main",
  });
  assert(validatedWatches.length === 1, "branch validation: valid project+branch registers watch");
  assert(rGood!.content[0].text.includes("Watching pipeline"), "branch validation: success message returned");

  await validatedPlugin.stop();
  await new Promise<void>((r) => mockServer.close(() => r()));
  try { unlinkSync(validationDbPath); } catch {}
  delete process.env.GITLAB_API_URL;
  process.env.GITLAB_PLUGIN_SKIP_VALIDATION = "true";

  // ─── Missing token → fail-fast at construction ──────────────────────
  console.log("\n--- GitLab Plugin: missing token ---");
  const tokenBackup = process.env.GITLAB_PERSONAL_ACCESS_TOKEN;
  delete process.env.GITLAB_PERSONAL_ACCESS_TOKEN;
  const missingTokenDb = join(tmpdir(), `gitlab-missing-${randomUUID()}.db`);
  process.env.GITLAB_CHANNEL_DB_PATH = missingTokenDb;
  let threw = false;
  try {
    const p = new GitLabChannelPlugin();
    await p.init(async () => {});
  } catch (err) {
    threw = err instanceof Error && err.message.includes("GITLAB_PERSONAL_ACCESS_TOKEN");
  }
  assert(threw, "missing token: throws clear error at startup");
  process.env.GITLAB_PERSONAL_ACCESS_TOKEN = tokenBackup!;
  try { unlinkSync(missingTokenDb); } catch {}

  // ─── Jenkins Plugin ─────────────────────────────────────────────────
  console.log("\n--- Jenkins Plugin ---");

  const jenkinsDbPath = join(tmpdir(), `jenkins-test-${randomUUID()}.db`);
  process.env.JENKINS_CHANNEL_URL = "http://fake-jenkins.test";
  process.env.JENKINS_CHANNEL_USERNAME = "test";
  process.env.JENKINS_CHANNEL_TOKEN = "test";
  process.env.JENKINS_CHANNEL_JOBS = "SDK/deploy_sdk_int,Processing/deploy_processing_int";
  process.env.JENKINS_CHANNEL_DB_PATH = jenkinsDbPath;
  process.env.JENKINS_CHANNEL_LOG_PATH = join(tmpdir(), `jenkins-test-${randomUUID()}.log`);

  const jenkinsPlugin = new JenkinsChannelPlugin();
  const jenkinsWatches: WatchRegistration[] = [];
  await jenkinsPlugin.init(async () => {});
  jenkinsPlugin.setOnWatchRegistered!((w) => jenkinsWatches.push(w));

  // 1. jenkins_watch_job (configured)
  const jobResult = await jenkinsPlugin.handleToolCall("jenkins_watch_job", {
    job_path: "SDK/deploy_sdk_int",
  });
  assert(jobResult !== null, "jenkins_watch_job returned result");
  assert(jenkinsWatches.length === 1, "jenkins watch callback fired");
  assert(jenkinsWatches[0].watch_type === "deploy-chain", "deploy-chain watch_type");
  assert(jenkinsWatches[0].entity_type === "build", "build entity_type");
  assert(jenkinsWatches[0].entity_ref === "jenkins:SDK/deploy_sdk_int", "entity_ref correct");
  assert(jenkinsWatches[0].correlation_key === "jenkins:SDK/deploy_sdk_int", "correlation_key correct");
  assert(!jobResult!.content[0].text.includes("note:"), "no warning for configured job");

  // 2. jenkins_watch_job (unconfigured — should still work but warn)
  jenkinsWatches.length = 0;
  const unconfiguredResult = await jenkinsPlugin.handleToolCall("jenkins_watch_job", {
    job_path: "Other/random_job",
  });
  assert(jenkinsWatches.length === 1, "unconfigured job: watch still created");
  assert(unconfiguredResult!.content[0].text.includes("note:"), "warning included for unconfigured job");

  // 3. unknown tool
  const jkUnknown = await jenkinsPlugin.handleToolCall("jenkins_nonexistent", {});
  assert(jkUnknown === null, "unknown jenkins tool returns null");

  // 4. Tool listed
  assert(jenkinsPlugin.tools.length === 1, "jenkins has 1 tool");
  assert(jenkinsPlugin.tools[0].name === "jenkins_watch_job", "jenkins_watch_job listed");

  await jenkinsPlugin.stop();
  try { unlinkSync(jenkinsDbPath); } catch {}

  // ─── Slack Plugin ───────────────────────────────────────────────────
  console.log("\n--- Slack Plugin ---");

  const slackDbPath = join(tmpdir(), `slack-test-${randomUUID()}.db`);
  process.env.SLACK_CHANNEL_DB_PATH = slackDbPath;
  process.env.SLACK_CHANNEL_LOG_PATH = join(tmpdir(), `slack-test-${randomUUID()}.log`);
  process.env.SLACK_MCP_AUTH_BIN = "/bin/true";
  process.env.SLACK_MCP_SERVER_BIN = "/bin/true";

  const slackPlugin = new SlackChannelPlugin();
  const slackWatches: WatchRegistration[] = [];
  await slackPlugin.init(async () => {});
  slackPlugin.setOnWatchRegistered!((w) => slackWatches.push(w));

  // 1. slack_watch_thread
  const threadResult = await slackPlugin.handleToolCall("slack_watch_thread", {
    channel_id: "C027LNCUH19",
    thread_ts: "1775119225.778049",
  });
  assert(threadResult !== null, "slack_watch_thread returned result");
  assert(slackWatches.length === 1, "slack watch callback fired");
  assert(slackWatches[0].watch_type === "slack-thread", "slack-thread watch_type");
  assert(slackWatches[0].entity_type === "slack_thread", "slack_thread entity_type");
  assert(slackWatches[0].entity_ref === "slack:C027LNCUH19:thread:1775119225.778049", "entity_ref correct");
  assert(slackWatches[0].correlation_key === "slack:C027LNCUH19:thread:1775119225.778049", "correlation_key correct");

  // 2. unknown tool
  const slackUnknown = await slackPlugin.handleToolCall("slack_nonexistent", {});
  assert(slackUnknown === null, "unknown slack tool returns null");

  // 3. Tool listed
  assert(slackPlugin.tools.length === 1, "slack has 1 tool");
  assert(slackPlugin.tools[0].name === "slack_watch_thread", "slack_watch_thread listed");

  // Don't call stop() on slack — it tries to close the MCP client which we never really connected
  try { unlinkSync(slackDbPath); } catch {}

  console.log(`\n=================`);
  console.log(`Results: ${passed}/${passed + failed} passed, ${failed} failed`);
  // Let pino's async sonic-boom drain before exit
  setTimeout(() => process.exit(failed > 0 ? 1 : 0), 100);
}

run().catch((err) => {
  console.error("Test runner error:", err);
  process.exit(1);
});
