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

  // Duplicate watch — should return existing, not create new
  const dupWatch = orchestrator.addWatch({
    session_id: session.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:123:pipeline:456",
  });
  assert(dupWatch.watch_id === watch.watch_id, "duplicate add_watch returns existing watch");
  const watchesAfterDup = orchestrator.listWatches(session.session_id);
  assert(watchesAfterDup.length === 1, "still only 1 watch after duplicate add");

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

  // 11. Replay on resume — replays queued items AND unacked delivered-live items.
  // Replay does NOT ack: stdio receipt is not proof Claude saw it. Items stay
  // unacked until the surfacing path (auto-surface or explicit ack_attention).
  console.log("\n--- Replay on Resume ---");
  const replayNotifications: Array<{ content: string; meta: Record<string, string> }> = [];
  const replayed = await orchestrator.replayOnResume(session.session_id, async (content, meta) => {
    replayNotifications.push({ content, meta });
  });
  assert(replayed === 3, `3 deliveries replayed (1 queued + 2 unacked-live), got ${replayed}`);
  assert(replayNotifications.length >= 1, "at least 1 replay notification sent");
  assert(replayNotifications.every((n) => n.meta.replay === "true"), "all replays flagged");

  const pendingAfterReplay = orchestrator.listPendingDeliveries(session.session_id);
  assert(pendingAfterReplay.length === 0, "no queued-state items after replay (promoted to delivered-live)");

  // Replay again — still re-emits because items remain unacked (no harness ack on stdio).
  const replayAgain = await orchestrator.replayOnResume(session.session_id, async () => {});
  assert(replayAgain === 3, `second replay still re-emits unacked items (got ${replayAgain})`);

  // After bulk-ack (the auto-surface path), replay becomes a no-op.
  const acksDone = orchestrator.ackDeliveriesBySession(session.session_id);
  assert(acksDone === 3, `bulk ack clears all 3 items (got ${acksDone})`);
  const replayAfterAck = await orchestrator.replayOnResume(session.session_id, async () => {});
  assert(replayAfterAck === 0, "replay is idempotent only AFTER explicit/auto ack");

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

  // ─── Phase 2 Tests ──────────────────────────────────────────────────

  console.log("\n--- Phase 2: findOrCreateSession (new) ---");
  const orch2 = new Orchestrator({ dbPath });
  const { session: s3, resumed: r3 } = orch2.findOrCreateSession("testuser", { role: "bugfix" });
  assert(!!s3.session_id, "new session created");
  assert(!r3, "not a resume");
  assert(s3.owner === "testuser", "owner is testuser");
  assert(s3.role === "bugfix", "role is bugfix");

  console.log("\n--- Phase 2: findOrCreateSession (resume) ---");
  orch2.markSessionResumable(s3.session_id);
  const stateAfterMark = orch2.getSessionState(s3.session_id);
  assert(stateAfterMark.session.status === "resumable", "session marked resumable");

  const { session: s4, resumed: r4 } = orch2.findOrCreateSession("testuser");
  assert(s4.session_id === s3.session_id, "same session resumed");
  assert(r4, "is a resume");
  assert(s4.status === "active", "status back to active");

  console.log("\n--- Phase 2: watches survive resume ---");
  const w2 = orch2.addWatch({
    session_id: s4.session_id,
    watch_type: "merge-request",
    entity_type: "merge_request",
    entity_ref: "gitlab:proj:mr:42",
    correlation_key: "gitlab:proj:mr:42",
  });
  orch2.attachRuntime(s4.session_id, undefined, "stdio");

  // Simulate shutdown: mark resumable
  orch2.markSessionResumable(s4.session_id);
  const watchesDuringDetach = orch2.listWatches(s4.session_id);
  assert(watchesDuringDetach.length === 1, "watch survives detach");

  // Simulate event while detached
  const res2 = orch2.ingestEvent({
    content: "New comment on MR !42",
    meta: { event_type: "mr_comment", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "mr_comment",
      entity_type: "merge_request",
      entity_ref: "gitlab:proj:mr:42",
      dedup_key: "gitlab:mr:42:comment:999",
      importance_hint: "normal",
      title_hint: "New comment on MR !42",
    },
  });
  assert(res2.matchedWatches === 1, "event matched watch while detached");
  assert(res2.deliveries[0].mode === "queued", "delivery queued while detached");

  // Resume and replay
  const { session: s5, resumed: r5 } = orch2.findOrCreateSession("testuser");
  assert(r5, "second resume");
  const pendingBeforeReplay = orch2.listPendingDeliveries(s5.session_id);
  assert(pendingBeforeReplay.length === 1, "1 pending delivery on resume");

  const replayed2: string[] = [];
  await orch2.replayOnResume(s5.session_id, async (content) => {
    replayed2.push(content);
  });
  assert(replayed2.length === 1, "1 item replayed on resume");
  assert(replayed2[0].includes("[replay") , "replayed content has replay marker");

  const pendingAfterReplay2 = orch2.listPendingDeliveries(s5.session_id);
  assert(pendingAfterReplay2.length === 0, "no pending after replay");

  console.log("\n--- Phase 2: stale runtime detection ---");
  const rt2 = orch2.attachRuntime(s5.session_id, undefined, "stdio");
  // Manually set heartbeat to the past to simulate stale
  orch2.heartbeat(rt2.runtime_id);
  // Use a large threshold so the runtime's heartbeat is definitely stale
  const detachedCount = orch2.detachStaleRuntimes(-1);
  assert(detachedCount >= 1, "stale runtime detached");
  const stateAfterStale = orch2.getSessionState(s5.session_id);
  assert(stateAfterStale.runtime === null, "no active runtime after stale detection");

  console.log("\n--- Phase 2: listSessions ---");
  const sessions = orch2.listSessions();
  assert(sessions.length >= 1, "listSessions returns sessions");
  assert(sessions.some((s) => s.session_id === s5.session_id), "our session in list");

  console.log("\n--- Phase 2: different owner gets new session ---");
  const { session: s6, resumed: r6 } = orch2.findOrCreateSession("otheruser", { role: "review" });
  assert(!r6, "different owner gets new session");
  assert(s6.session_id !== s5.session_id, "different session ID");
  assert(s6.owner === "otheruser", "correct owner");

  orch2.close();

  // ─── Phase 3+4 Tests ───────────────────────────────────────────────

  const orch3 = new Orchestrator({ dbPath });
  const delivered3: Array<{ sessionId: string; content: string; meta: Record<string, string> }> = [];
  orch3.setNotifyCallback(async (sessionId, content, meta) => {
    delivered3.push({ sessionId, content, meta });
  });

  const mainSession = orch3.registerSession({ session_name: "main", role: "main", owner: "test" });
  orch3.attachRuntime(mainSession.session_id);

  console.log("\n--- Phase 3: Watch auto-complete ---");
  orch3.attachRuntime(mainSession.session_id);
  const pipelineWatch = orch3.addWatch({
    session_id: mainSession.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:100:ref:main",
    correlation_key: "gitlab:100:ref:main",
  });
  assert(pipelineWatch.status === "active", "pipeline watch active");

  const completed = orch3.completeWatchesByEntity("pipeline", "gitlab:100:ref:main");
  assert(completed === 1, "1 watch auto-completed");
  const watchesAfterComplete = orch3.listWatches(mainSession.session_id);
  assert(watchesAfterComplete.length === 0, "no active watches after completion");

  console.log("\n--- Phase 4: Event retention cleanup ---");
  // Insert an old event that's not referenced by attention/delivery
  const oldEventResult = orch3.ingestEvent({
    content: "ancient event",
    meta: { event_type: "test", plugin: "test" },
    orchestration: { source: "test", event_kind: "old_event", dedup_key: `old-event-${Date.now()}` },
  });
  // Purge with 0 retention — everything not referenced gets purged
  const purged = orch3.purgeOldEvents(0);
  assert(purged >= 1, "old events purged");

  console.log("\n--- Phase 4: Richer replay digest ---");
  // Use a fresh session for clean digest test
  const digestSession = orch3.registerSession({ session_name: "digest-test", role: "bugfix", owner: "digest-user" });
  const digestWatch = orch3.addWatch({
    session_id: digestSession.session_id,
    watch_type: "merge-request",
    entity_type: "merge_request",
    entity_ref: "gitlab:proj:mr:55",
  });

  for (let i = 0; i < 3; i++) {
    orch3.ingestEvent({
      content: `Comment ${i} on MR !55`,
      meta: { event_type: "mr_comment", plugin: "gitlab" },
      orchestration: {
        source: "gitlab",
        event_kind: "mr_comment",
        entity_type: "merge_request",
        entity_ref: "gitlab:proj:mr:55",
        dedup_key: `mr55-comment-${i}`,
        importance_hint: "normal",
        title_hint: `Comment ${i} on MR !55`,
      },
    });
  }

  const digestPending = orch3.listPendingDeliveries(digestSession.session_id);
  assert(digestPending.length === 3, `3 items pending for digest (got ${digestPending.length})`);

  const digestNotifications: string[] = [];
  await orch3.replayOnResume(digestSession.session_id, async (content) => {
    digestNotifications.push(content);
  });
  assert(digestNotifications.length >= 1, "digest replay produced notifications");
  const digestText = digestNotifications.join("\n");
  assert(digestText.includes("gitlab"), "digest mentions source");
  assert(digestText.includes("[replay"), "digest has replay marker");

  orch3.close();

  // ─── Crash Recovery Test ────────────────────────────────────────────

  console.log("\n--- Crash Recovery: session stays active after hard kill ---");
  const orch4 = new Orchestrator({ dbPath });
  const crashSession = orch4.registerSession({ session_name: "crash-test", owner: "crashuser", role: "bugfix" });
  const crashRt = orch4.attachRuntime(crashSession.session_id);
  const crashWatch = orch4.addWatch({
    session_id: crashSession.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:crash:ref:main",
    correlation_key: "gitlab:crash:ref:main",
  });

  // Simulate hard kill: just close DB without markSessionResumable
  orch4.close();

  // New process starts — session is still "active" in DB, runtime still "attached"
  const orch5 = new Orchestrator({ dbPath });
  const { session: recovered, resumed: wasResumed } = orch5.findOrCreateSession("crashuser");
  assert(wasResumed, "crash recovery: session was resumed");
  assert(recovered.session_id === crashSession.session_id, "crash recovery: same session ID");
  assert(recovered.status === "active", "crash recovery: status is active");

  const recoveredWatches = orch5.listWatches(recovered.session_id);
  assert(recoveredWatches.length === 1, "crash recovery: watch survived");
  assert(recoveredWatches[0].entity_ref === "gitlab:crash:ref:main", "crash recovery: correct watch");

  // Events queued during "dead" period should work
  orch5.ingestEvent({
    content: "Pipeline failed while process was dead",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:crash:pipeline:999",
      correlation_key: "gitlab:crash:ref:main",
      dedup_key: "crash-recovery-event-1",
      importance_hint: "high",
    },
  });

  // After findOrCreateSession detached old runtimes, this should be queued
  const crashPending = orch5.listPendingDeliveries(recovered.session_id);
  assert(crashPending.length === 1, "crash recovery: event queued for recovered session");

  // Attach new runtime and replay
  orch5.attachRuntime(recovered.session_id);
  const crashReplayed: string[] = [];
  await orch5.replayOnResume(recovered.session_id, async (content) => {
    crashReplayed.push(content);
  });
  assert(crashReplayed.length === 1, "crash recovery: pending replayed on new runtime");

  orch5.close();

  // ─── Live-delivery ack semantics (regression: silent black hole) ────

  console.log("\n--- Live delivery: unacked items resurface on replay ---");
  const orch6 = new Orchestrator({ dbPath });
  const liveDelivered: string[] = [];
  orch6.setNotifyCallback(async (_sid, content) => {
    liveDelivered.push(content);
  });

  const liveSession = orch6.registerSession({ session_name: "live-ack-test", role: "main", owner: "alice" });
  orch6.attachRuntime(liveSession.session_id);
  orch6.addWatch({
    session_id: liveSession.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:42:ref:int",
  });

  orch6.ingestEvent({
    content: "Pipeline #1 on int: success",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:42:ref:int",
      dedup_key: "live-ack-test:1",
      importance_hint: "high",
      title_hint: "Pipeline #1 success",
    },
  });
  assert(liveDelivered.length === 1, "live delivery fired once");

  // Pretend the channel notification got dropped — Claude never acked it.
  // Replay-on-attach must resurface it.
  const livePending = orch6.listPendingDeliveries(liveSession.session_id);
  assert(livePending.length === 0, "queued-only pending list is empty (item was delivered-live, not queued)");

  const livePending2: string[] = [];
  const replayed3 = await orch6.replayOnResume(liveSession.session_id, async (content) => {
    livePending2.push(content);
  });
  assert(replayed3 === 1, `unacked delivered-live item resurfaces on replay (got ${replayed3})`);
  assert(livePending2.length === 1, "callback fired for the unacked-live item");

  // Replay is fire-and-forget on stdio — it does NOT ack. Surfacing API still
  // sees the item, so the next tool-call auto-surface will show it to Claude.
  const unackedStillThere = orch6.listUnackedDeliveredLive(liveSession.session_id);
  assert(unackedStillThere.length === 1, "replay does not ack — item still in unacked-live list");

  // Second replay re-emits again (also without acking).
  const replayed4 = await orch6.replayOnResume(liveSession.session_id, async () => {});
  assert(replayed4 === 1, "replay re-emits unacked items every time, until something acks");

  // Auto-surface (bulk ack via tool response) is the only path to ack.
  const acks = orch6.ackDeliveriesBySession(liveSession.session_id);
  assert(acks === 1, "bulk ack flips the row");
  const unackedAfter = orch6.listUnackedDeliveredLive(liveSession.session_id);
  assert(unackedAfter.length === 0, "no unacked-live items after explicit bulk ack");

  // After ack, replay finally is a no-op.
  const replayedAfterAck = await orch6.replayOnResume(liveSession.session_id, async () => {});
  assert(replayedAfterAck === 0, "replay is idempotent only AFTER ack");

  // New unacked event → surfacing API returns it; ackDeliveriesBySession clears it.
  orch6.ingestEvent({
    content: "Pipeline #2 on int: failed",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:42:ref:int",
      dedup_key: "live-ack-test:2",
      importance_hint: "high",
      title_hint: "Pipeline #2 failed",
    },
  });
  const unacked2 = orch6.listUnackedDeliveredLive(liveSession.session_id);
  assert(unacked2.length === 1, "new unacked-live item surfaces");
  const ackedCount = orch6.ackDeliveriesBySession(liveSession.session_id);
  assert(ackedCount === 1, "bulk ack flips one row");
  const unacked3 = orch6.listUnackedDeliveredLive(liveSession.session_id);
  assert(unacked3.length === 0, "no unacked-live items after bulk ack");

  // Per-attention ack path
  orch6.ingestEvent({
    content: "Pipeline #3 on int: running",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:42:ref:int",
      dedup_key: "live-ack-test:3",
      importance_hint: "normal",
      title_hint: "Pipeline #3 running",
    },
  });
  const feed3 = orch6.listSessionFeed(liveSession.session_id, { limit: 5 });
  const newest = feed3.find((a) => a.summary_hint === "Pipeline #3 running");
  assert(!!newest, "found new attention via feed");
  orch6.ackDeliveryByAttention(newest!.attention_id);
  const unacked4 = orch6.listUnackedDeliveredLive(liveSession.session_id);
  assert(unacked4.length === 0, "ack-by-attention clears the unacked entry");

  orch6.close();

  // ─── Audit log: cross-session forensics ─────────────────────────────

  console.log("\n--- Audit log: full lifecycle traces ---");
  const orch7 = new Orchestrator({ dbPath });
  const auditDelivered: string[] = [];
  orch7.setNotifyCallback(async (_sid, content) => { auditDelivered.push(content); });

  const auditSession = orch7.registerSession({ session_name: "audit-test", role: "main", owner: "carol" });
  orch7.attachRuntime(auditSession.session_id, undefined, "stdio");
  const auditWatch = orch7.addWatch({
    session_id: auditSession.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:audit:ref:int",
  });

  const auditIngest = orch7.ingestEvent({
    content: "Pipeline #1 success",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab",
      event_kind: "pipeline_status_changed",
      entity_type: "pipeline",
      entity_ref: "gitlab:audit:ref:int",
      dedup_key: "audit-test:1",
      importance_hint: "high",
      title_hint: "Pipeline #1",
    },
  });

  // Notify happens via fire-and-forget .then; flush microtasks before reading audit
  await new Promise((r) => setImmediate(r));

  // Session lifecycle traces
  const sessionTrail = orch7.getAuditTrail({ category: "session" });
  assert(sessionTrail.some((a) => a.action === "registered" && a.session_id === auditSession.session_id), "audit: session registered logged");

  // Runtime trace
  const runtimeTrail = orch7.getAuditTrail({ category: "runtime" });
  assert(runtimeTrail.some((a) => a.action === "attached" && a.session_id === auditSession.session_id), "audit: runtime attached logged");

  // Watch trace
  const watchTrail = orch7.getAuditTrail({ category: "watch" });
  assert(watchTrail.some((a) => a.action === "added" && a.watch_id === auditWatch.watch_id), "audit: watch added logged");

  // Event trace
  const eventTrail = orch7.getAuditTrail({ category: "event", eventId: auditIngest.eventId });
  assert(eventTrail.some((a) => a.action === "ingested"), "audit: event ingested logged");

  // Delivery trace — find the delivery decision
  const deliveryTrail = orch7.getAuditTrail({ category: "delivery", eventId: auditIngest.eventId });
  assert(deliveryTrail.some((a) => a.action === "decided_live" && a.outcome === "live"), "audit: live delivery decision logged");

  // Attention trace
  const attentionTrail = orch7.getAuditTrail({ category: "attention", eventId: auditIngest.eventId });
  assert(attentionTrail.some((a) => a.action === "created"), "audit: attention created logged");

  // Notify trace — should have an emit success event
  const notifyTrail = orch7.getAuditTrail({ category: "notify", sessionId: auditSession.session_id });
  assert(notifyTrail.some((a) => a.action === "emitted"), "audit: notify emitted logged");

  // Filter combinations work — session-scoped pulls session/runtime/watch/attention/delivery/notify rows
  const sessionScoped = orch7.getAuditTrail({ sessionId: auditSession.session_id, limit: 100 });
  assert(sessionScoped.length >= 6, `audit: session-scoped query returns multi-category trail (got ${sessionScoped.length})`);
  const distinctCats = new Set(sessionScoped.map((a) => a.category));
  assert(distinctCats.has("session") && distinctCats.has("runtime") && distinctCats.has("watch") && distinctCats.has("delivery"), "audit: session filter spans multiple categories");

  // Detail JSON is round-trippable
  const ingestEntry = eventTrail.find((a) => a.action === "ingested")!;
  const parsed = JSON.parse(ingestEntry.detail_json!);
  assert(parsed.source === "gitlab" && parsed.event_kind === "pipeline_status_changed", "audit: detail_json has structured payload");

  // Forensic flow: trace one delivery end-to-end
  const decision = deliveryTrail.find((a) => a.action === "decided_live")!;
  const fullDeliveryHistory = orch7.getAuditTrail({ deliveryId: decision.delivery_id! });
  assert(fullDeliveryHistory.length >= 1, "audit: filter by delivery_id returns history");

  // Replay produces a replay-category audit entry
  await orch7.replayOnResume(auditSession.session_id, async () => {});
  const replayTrail = orch7.getAuditTrail({ category: "replay", sessionId: auditSession.session_id });
  assert(replayTrail.length >= 1, "audit: replay category logged");

  // Ack via attention path
  const newAttention = orch7.listSessionFeed(auditSession.session_id)[0];
  orch7.ackDeliveryByAttention(newAttention.attention_id);
  const ackTrail = orch7.getAuditTrail({ category: "delivery", action: "acked_by_attention" });
  assert(ackTrail.length >= 1, "audit: ack_by_attention logged");

  // findDroppedNotifications: cross-session black-hole detector
  console.log("\n--- find_dropped_notifications: black-hole detector ---");
  const dropSession = orch7.registerSession({ session_name: "drop-test", role: "main", owner: "dora" });
  orch7.attachRuntime(dropSession.session_id);
  orch7.addWatch({
    session_id: dropSession.session_id,
    watch_type: "pipeline-chain",
    entity_type: "pipeline",
    entity_ref: "gitlab:drop:ref:int",
  });

  // Live event fires; we simulate Claude never seeing it (no ack)
  orch7.ingestEvent({
    content: "Pipeline dropped",
    meta: { event_type: "pipeline_status_changed", plugin: "gitlab" },
    orchestration: {
      source: "gitlab", event_kind: "pipeline_status_changed",
      entity_type: "pipeline", entity_ref: "gitlab:drop:ref:int",
      dedup_key: "drop-test:1", importance_hint: "high",
      title_hint: "Pipeline dropped — black hole",
    },
  });

  // grace_ms=0 lets us see it immediately
  const dropsImmediate = orch7.findDroppedNotifications({ graceMs: 0 });
  assert(dropsImmediate.some((d) => d.session_id === dropSession.session_id), "drop detector: cross-session sweep finds the unacked-live row");
  const ours = dropsImmediate.find((d) => d.session_id === dropSession.session_id)!;
  assert(ours.source === "gitlab", "drop detector: row enriched with event source");
  assert(ours.summary_hint === "Pipeline dropped — black hole", "drop detector: row enriched with attention summary");
  assert(ours.owner === "dora", "drop detector: row enriched with session owner");
  assert(typeof ours.age_ms === "number" && (ours.age_ms as number) >= 0, "drop detector: age_ms computed");

  // Filter by session_id
  const dropsScoped = orch7.findDroppedNotifications({ sessionId: dropSession.session_id, graceMs: 0 });
  assert(dropsScoped.every((d) => d.session_id === dropSession.session_id), "drop detector: session_id filter works");

  // Grace period excludes too-recent drops
  const dropsWithGrace = orch7.findDroppedNotifications({ graceMs: 60_000 });
  assert(!dropsWithGrace.some((d) => d.session_id === dropSession.session_id), "drop detector: grace period excludes recent drops");

  // Once acked, no longer reported as a drop
  orch7.ackDeliveriesBySession(dropSession.session_id);
  const dropsAfterAck = orch7.findDroppedNotifications({ graceMs: 0 });
  assert(!dropsAfterAck.some((d) => d.session_id === dropSession.session_id), "drop detector: acked rows excluded");

  // Purge respects retention
  const beforePurge = orch7.countAudit();
  assert(beforePurge > 0, "audit: rows present before purge");
  orch7.purgeOldAudit(0);
  const afterPurge = orch7.countAudit();
  assert(afterPurge === 0, `audit: purge with 0 retention clears all (got ${afterPurge})`);

  orch7.close();

  try { unlinkSync(dbPath); } catch {}

  console.log(`\n=======================`);
  console.log(`Results: ${passed}/${passed + failed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

run();
