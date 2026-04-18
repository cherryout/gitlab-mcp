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
