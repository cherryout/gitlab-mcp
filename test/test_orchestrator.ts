import { Orchestrator } from "../orchestrator.js";
import { tmpdir } from "os";
import { join } from "path";
import { randomUUID } from "crypto";
import { unlinkSync } from "fs";

const dbPath = join(tmpdir(), `orchestrator-test-${randomUUID()}.db`);
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
  console.log("Orchestrator Unit Tests\n=======================\n");

  const orchestrator = new Orchestrator({ dbPath });
  const delivered: Array<{ sessionId: string; content: string; meta: Record<string, string> }> = [];

  orchestrator.setNotifyCallback(async (sessionId, content, meta) => {
    delivered.push({ sessionId, content, meta });
  });

  // 1. Register session
  console.log("--- Session Management ---");
  const session = orchestrator.registerSession({ session_name: "test-bugfix", role: "bugfix", owner: "alex" });
  assert(!!session.session_id, "session created with ID");
  assert(session.role === "bugfix", "session role is bugfix");
  assert(session.status === "active", "session status is active");

  // 2. Get session state
  const state = orchestrator.getSessionState(session.session_id);
  assert(state.session.session_id === session.session_id, "getSessionState returns correct session");
  assert(state.runtime === null, "no runtime attached initially");
  assert(state.activeWatches.length === 0, "no watches initially");

  // 3. Attach runtime
  console.log("\n--- Runtime Attachment ---");
  const runtime = orchestrator.attachRuntime(session.session_id, undefined, "stdio");
  assert(!!runtime.runtime_id, "runtime created with ID");
  assert(runtime.attached === 1, "runtime is attached");
  assert(runtime.session_id === session.session_id, "runtime linked to session");

  // 4. Add watch
  console.log("\n--- Watch Management ---");
  const watch = orchestrator.addWatch({
    session_id: session.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:123:pipeline:456",
    correlation_key: "gitlab:123:ref:main",
    expires_at: Date.now() + 3600000,
  });
  assert(!!watch.watch_id, "watch created with ID");
  assert(watch.status === "active", "watch status is active");
  assert(watch.entity_ref === "gitlab:123:pipeline:456", "entity_ref correct");

  const watches = orchestrator.listWatches(session.session_id);
  assert(watches.length === 1, "1 active watch listed");

  // 5. Ingest event matching by entity_ref
  console.log("\n--- Event Ingestion (entity match) ---");
  const result1 = orchestrator.ingestEvent({
    content: "Pipeline #456 running -> failed",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:123:pipeline:456",
      correlation_key: "gitlab:123:ref:main",
      dedup_key: "gitlab:pipeline:456:failed",
      importance_hint: "high",
      title_hint: "Pipeline #456 failed",
    },
  });
  assert(!result1.deduplicated, "event not deduplicated");
  assert(result1.matchedWatches === 1, "matched 1 watch");
  assert(result1.deliveries.length === 1, "1 delivery decision");
  assert(result1.deliveries[0].mode === "live", "delivery mode is live (runtime attached)");
  assert(delivered.length === 1, "live notification sent");
  assert(delivered[0].meta.importance === "high", "importance forwarded correctly");

  // 6. Deduplication
  console.log("\n--- Deduplication ---");
  const result2 = orchestrator.ingestEvent({
    content: "Pipeline #456 running -> failed (dup)",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:123:pipeline:456",
      dedup_key: "gitlab:pipeline:456:failed",
    },
  });
  assert(result2.deduplicated, "duplicate event detected");
  assert(delivered.length === 1, "no extra notification for duplicate");

  // 7. Ingest event matching by correlation_key
  console.log("\n--- Event Ingestion (correlation match) ---");
  const result3 = orchestrator.ingestEvent({
    content: "Pipeline #789 on main: success",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:123:pipeline:789",
      correlation_key: "gitlab:123:ref:main",
      dedup_key: "gitlab:pipeline:789:success",
      importance_hint: "normal",
    },
  });
  assert(result3.matchedWatches === 1, "matched via correlation_key");
  assert(delivered.length === 2, "second live notification sent");

  // 8. Unmatched event
  console.log("\n--- Unmatched Event ---");
  const result4 = orchestrator.ingestEvent({
    content: "Build #99 on jenkins/job: SUCCESS",
    meta: { event_type: "build_completed", plugin: "jenkins" },
    orchestration: {
      source: "jenkins",
      event_kind: "build_completed",
      entity_type: "build",
      entity_ref: "jenkins:my-job:build:99",
      dedup_key: "jenkins:my-job:99:SUCCESS",
    },
  });
  assert(result4.matchedWatches === 0, "no watches matched");
  assert(delivered.length === 2, "no notification for unmatched");

  const unmatched = orchestrator.listUnmatchedEvents();
  assert(unmatched.length === 1, "1 unmatched event in list");
  assert(unmatched[0].source === "jenkins", "unmatched event source is jenkins");

  // 9. Detach runtime and test queued delivery
  console.log("\n--- Queued Delivery (runtime detached) ---");
  orchestrator.detachRuntime(runtime.runtime_id);

  const result5 = orchestrator.ingestEvent({
    content: "Pipeline #999 failed",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:123:pipeline:999",
      correlation_key: "gitlab:123:ref:main",
      dedup_key: "gitlab:pipeline:999:failed",
      importance_hint: "high",
    },
  });
  assert(result5.matchedWatches === 1, "watch still matches after detach");
  assert(result5.deliveries[0].mode === "queued", "delivery mode is queued");
  assert(delivered.length === 2, "no live notification when detached");

  const pending = orchestrator.listPendingDeliveries(session.session_id);
  assert(pending.length === 1, "1 pending delivery queued");

  // 10. Delivery summary
  console.log("\n--- Delivery Summary ---");
  const summary = orchestrator.getDeliverySummary(session.session_id);
  assert(summary.totalPending === 1, "summary shows 1 pending");
  assert(summary.bySource.gitlab === 1, "summary by source: 1 gitlab");
  assert(summary.byImportance.high === 1, "summary by importance: 1 high");

  // 11. Replay on resume
  console.log("\n--- Replay on Resume ---");
  const replayNotifications: Array<{ content: string; meta: Record<string, string> }> = [];
  const replayed = await orchestrator.replayOnResume(session.session_id, async (content, meta) => {
    replayNotifications.push({ content, meta });
  });
  assert(replayed === 1, "1 delivery replayed");
  assert(replayNotifications.length === 1, "1 replay notification sent");
  assert(replayNotifications[0].meta.replay === "true", "replay flag set");

  const pendingAfterReplay = orchestrator.listPendingDeliveries(session.session_id);
  assert(pendingAfterReplay.length === 0, "no pending after replay");

  // 12. Attention lifecycle
  console.log("\n--- Attention Lifecycle ---");
  const feed = orchestrator.listSessionFeed(session.session_id);
  assert(feed.length >= 1, "session feed has items");

  const firstAttention = feed[0];
  orchestrator.ackAttention(firstAttention.attention_id);
  const acked = orchestrator.listSessionFeed(session.session_id);
  const ackedItem = acked.find((a) => a.attention_id === firstAttention.attention_id);
  assert(ackedItem?.state === "acked", "attention item acked");

  orchestrator.resolveAttention(firstAttention.attention_id);
  const resolved = orchestrator.listSessionFeed(session.session_id);
  const resolvedItem = resolved.find((a) => a.attention_id === firstAttention.attention_id);
  assert(resolvedItem?.state === "resolved", "attention item resolved");

  // 13. Remove watch
  console.log("\n--- Watch Removal ---");
  orchestrator.removeWatch(watch.watch_id);
  const watchesAfter = orchestrator.listWatches(session.session_id);
  assert(watchesAfter.length === 0, "watch removed");

  // 14. Close session
  console.log("\n--- Session Close ---");
  orchestrator.closeSession(session.session_id);
  const closedState = orchestrator.getSessionState(session.session_id);
  assert(closedState.session.status === "archived", "session archived");

  // 15. Watch expiration
  console.log("\n--- Watch Expiration ---");
  const session2 = orchestrator.registerSession({ session_name: "test-expiry" });
  orchestrator.addWatch({
    session_id: session2.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:999:pipeline:1",
    expires_at: Date.now() - 1000,
  });
  const expired = orchestrator.expireStaleWatches();
  assert(expired === 1, "1 stale watch expired");
  const watchesAfterExpiry = orchestrator.listWatches(session2.session_id);
  assert(watchesAfterExpiry.length === 0, "expired watch not in active list");

  orchestrator.close();

  try { unlinkSync(dbPath); } catch {}

  console.log(`\n=======================`);
  console.log(`Results: ${passed}/${passed + failed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

run();
