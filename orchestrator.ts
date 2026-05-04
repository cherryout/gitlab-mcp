import { randomUUID } from "crypto";
import { pino } from "pino";
import {
  createOrchestratorDb,
  type OrchestratorDb,
  type OrchestratorStmts,
  type SessionRow,
  type RuntimeRow,
  type WatchRow,
  type AttentionRow,
  type EventRow,
  type PendingDeliveryRow,
  type AuditRow,
} from "./orchestrator-db.js";
import { decideDelivery, buildReplaySummary, replayPendingDeliveries, type DeliveryDecision, type ReplaySummary } from "./delivery.js";
import type { ChannelNotification } from "./channel-plugin.js";

export interface RegisterSessionParams {
  session_name?: string;
  owner?: string;
  role?: string;
  workspace?: string;
  project?: string;
  repository?: string;
  branch_hint?: string;
  metadata_json?: string;
}

export interface AddWatchParams {
  session_id: string;
  watch_type: string;
  entity_type: string;
  entity_ref: string;
  correlation_key?: string;
  delivery_policy?: string;
  fallback_policy?: string;
  expires_at?: number;
  metadata_json?: string;
}

export interface IngestResult {
  eventId: string;
  deduplicated: boolean;
  matchedWatches: number;
  deliveries: DeliveryDecision[];
}

export interface SessionState {
  session: SessionRow;
  runtime: RuntimeRow | null;
  activeWatches: WatchRow[];
  pendingDeliveries: number;
  recentAttention: AttentionRow[];
}

type NotifyCallback = (sessionId: string, content: string, meta: Record<string, string>) => Promise<void>;

const DEFAULT_DELIVERY_RETENTION_MS = 24 * 60 * 60 * 1000;
const DEFAULT_AUDIT_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;

export type AuditCategory =
  | "session"
  | "runtime"
  | "watch"
  | "event"
  | "delivery"
  | "attention"
  | "replay"
  | "tool_call"
  | "maintenance"
  | "notify";

export interface AuditEntry {
  category: AuditCategory;
  action: string;
  outcome?: "success" | "noop" | "error" | "deduplicated" | "queued" | "live" | string;
  sessionId?: string | null;
  runtimeId?: string | null;
  watchId?: string | null;
  eventId?: string | null;
  attentionId?: string | null;
  deliveryId?: string | null;
  detail?: Record<string, unknown>;
}

export interface AuditFilters {
  since?: number;
  until?: number;
  sessionId?: string;
  category?: string;
  action?: string;
  deliveryId?: string;
  attentionId?: string;
  eventId?: string;
  watchId?: string;
  limit?: number;
}

export class Orchestrator {
  private odb: OrchestratorDb;
  private stmts: OrchestratorStmts;
  private logger: pino.Logger;
  private notifyCallback: NotifyCallback | null = null;

  constructor(opts?: { dbPath?: string; logger?: pino.Logger }) {
    this.odb = createOrchestratorDb(opts?.dbPath);
    this.stmts = this.odb.stmts;
    this.logger = opts?.logger?.child({ component: "orchestrator" }) ||
      pino({ level: "silent" });
  }

  setNotifyCallback(fn: NotifyCallback): void {
    this.notifyCallback = fn;
  }

  close(): void {
    this.odb.close();
  }

  // ─── Audit Logging ──────────────────────────────────────────────────

  audit(entry: AuditEntry): void {
    try {
      const detailJson = entry.detail ? JSON.stringify(entry.detail) : null;
      this.stmts.insertAudit.run(
        randomUUID(),
        Date.now(),
        entry.sessionId ?? null,
        entry.runtimeId ?? null,
        entry.watchId ?? null,
        entry.eventId ?? null,
        entry.attentionId ?? null,
        entry.deliveryId ?? null,
        entry.category,
        entry.action,
        entry.outcome ?? null,
        detailJson,
      );
    } catch (err) {
      this.logger.error({ err, entry }, "audit insert failed");
    }
  }

  getAuditTrail(filters: AuditFilters = {}): AuditRow[] {
    const since = filters.since ?? 0;
    const until = filters.until ?? Date.now();
    const limit = Math.min(filters.limit ?? 200, 5000);

    const anyFilter =
      filters.sessionId || filters.category || filters.action ||
      filters.deliveryId || filters.attentionId || filters.eventId || filters.watchId;

    if (!anyFilter) {
      return this.stmts.listAudit.all(since, until, limit) as AuditRow[];
    }

    return this.stmts.listAuditFiltered.all(
      since, until,
      filters.sessionId ?? null, filters.sessionId ?? null,
      filters.category ?? null, filters.category ?? null,
      filters.action ?? null, filters.action ?? null,
      filters.deliveryId ?? null, filters.deliveryId ?? null,
      filters.attentionId ?? null, filters.attentionId ?? null,
      filters.eventId ?? null, filters.eventId ?? null,
      filters.watchId ?? null, filters.watchId ?? null,
      limit,
    ) as AuditRow[];
  }

  countAudit(): number {
    const row = this.stmts.countAudit.get() as { n: number };
    return row.n;
  }

  purgeOldAudit(retentionMs: number = DEFAULT_AUDIT_RETENTION_MS): number {
    const cutoff = Date.now() - retentionMs;
    const result = this.stmts.purgeOldAudit.run(cutoff);
    return result.changes as number;
  }

  // ─── Session Management ─────────────────────────────────────────────

  registerSession(params: RegisterSessionParams): SessionRow {
    const sessionId = randomUUID();
    const now = Date.now();
    this.stmts.insertSession.run(
      sessionId,
      params.session_name || null,
      params.owner || "default",
      params.role || "main",
      params.workspace || null,
      params.project || null,
      params.repository || null,
      params.branch_hint || null,
      now,
      now,
      params.metadata_json || null,
    );
    this.logger.info({ sessionId, role: params.role || "main" }, "session registered");
    this.audit({
      category: "session", action: "registered", outcome: "success",
      sessionId,
      detail: { owner: params.owner || "default", role: params.role || "main", session_name: params.session_name || null },
    });
    return this.stmts.getSession.get(sessionId) as SessionRow;
  }

  findOrCreateSession(owner: string, opts?: Omit<RegisterSessionParams, "owner">): { session: SessionRow; resumed: boolean } {
    const existing = this.stmts.findResumableSession.get(owner) as SessionRow | undefined;
    if (existing) {
      const now = Date.now();
      const liveRuntime = this.stmts.getActiveRuntime.get(existing.session_id) as RuntimeRow | undefined;
      const stealing = !!(liveRuntime && now - liveRuntime.last_heartbeat_at < 120_000);
      if (stealing) {
        this.logger.warn(
          { sessionId: existing.session_id, owner, stolenFromRuntime: liveRuntime!.runtime_id, heartbeatAge: now - liveRuntime!.last_heartbeat_at },
          "another runtime appears live; stealing attachment (multi-terminal not supported, use ORCHESTRATOR_SESSION_OWNER for parallel sessions)",
        );
        this.audit({
          category: "runtime", action: "stolen", outcome: "success",
          sessionId: existing.session_id, runtimeId: liveRuntime!.runtime_id,
          detail: { owner, heartbeat_age_ms: now - liveRuntime!.last_heartbeat_at },
        });
      }
      this.stmts.detachAllRuntimes.run(existing.session_id);
      this.stmts.updateSessionStatus.run("active", now, existing.session_id);
      this.stmts.updateSessionLastSeen.run(now, existing.session_id);
      const wasActive = existing.status === "active";
      const crashRecovery = wasActive && !liveRuntime;
      this.logger.info(
        { sessionId: existing.session_id, owner, previousStatus: existing.status, crashRecovery },
        crashRecovery ? "session recovered after crash" : "session resumed",
      );
      this.audit({
        category: "session", action: crashRecovery ? "crash_recovered" : "resumed", outcome: "success",
        sessionId: existing.session_id,
        detail: { owner, previous_status: existing.status, stealing },
      });
      return { session: this.stmts.getSession.get(existing.session_id) as SessionRow, resumed: true };
    }
    const session = this.registerSession({ ...opts, owner });
    return { session, resumed: false };
  }

  listSessions(): SessionRow[] {
    return this.stmts.listSessions.all() as SessionRow[];
  }

  closeSession(sessionId: string): void {
    const session = this.stmts.getSession.get(sessionId) as SessionRow | undefined;
    if (!session) throw new Error(`Session not found: ${sessionId}`);
    const now = Date.now();
    this.stmts.updateSessionStatus.run("archived", now, sessionId);
    this.logger.info({ sessionId }, "session closed");
    this.audit({ category: "session", action: "closed", outcome: "success", sessionId });
  }

  getSessionState(sessionId: string): SessionState {
    const session = this.stmts.getSession.get(sessionId) as SessionRow | undefined;
    if (!session) throw new Error(`Session not found: ${sessionId}`);
    const runtime = (this.stmts.getActiveRuntime.get(sessionId) as RuntimeRow | undefined) || null;
    const activeWatches = this.stmts.listWatchesBySession.all(sessionId) as WatchRow[];
    const pending = this.stmts.listPendingBySession.all(sessionId) as PendingDeliveryRow[];
    const recentAttention = this.stmts.listAttentionBySession.all(sessionId, 20) as AttentionRow[];
    return {
      session,
      runtime,
      activeWatches,
      pendingDeliveries: pending.length,
      recentAttention,
    };
  }

  // ─── Runtime Attachment ─────────────────────────────────────────────

  attachRuntime(sessionId: string, runtimeId?: string, channelName?: string): RuntimeRow {
    const session = this.stmts.getSession.get(sessionId) as SessionRow | undefined;
    if (!session) throw new Error(`Session not found: ${sessionId}`);

    const rid = runtimeId || randomUUID();
    const now = Date.now();
    this.stmts.insertRuntime.run(rid, sessionId, channelName || null, now, now, null);
    this.stmts.updateSessionLastSeen.run(now, sessionId);

    if (session.status === "resumable") {
      this.stmts.updateSessionStatus.run("active", now, sessionId);
    }

    this.logger.info({ sessionId, runtimeId: rid }, "runtime attached");
    this.audit({
      category: "runtime", action: "attached", outcome: "success",
      sessionId, runtimeId: rid,
      detail: { channel_name: channelName || null, prior_session_status: session.status },
    });
    return this.stmts.getActiveRuntime.get(sessionId) as RuntimeRow;
  }

  detachRuntime(runtimeId: string): void {
    this.stmts.detachRuntime.run(runtimeId);
    this.logger.info({ runtimeId }, "runtime detached");
    this.audit({ category: "runtime", action: "detached", outcome: "success", runtimeId });
  }

  markSessionResumable(sessionId: string): void {
    const now = Date.now();
    this.stmts.detachAllRuntimes.run(sessionId);
    this.stmts.updateSessionStatus.run("resumable", now, sessionId);
    this.logger.info({ sessionId }, "session marked resumable");
    this.audit({ category: "session", action: "marked_resumable", outcome: "success", sessionId });
  }

  detachStaleRuntimes(staleThresholdMs: number = 120_000): number {
    const cutoff = Date.now() - staleThresholdMs;
    const result = this.stmts.detachStaleRuntimes.run(cutoff);
    if (result.changes > 0) {
      this.logger.info({ detached: result.changes, staleThresholdMs }, "stale runtimes detached");
      this.audit({
        category: "maintenance", action: "stale_runtimes_detached", outcome: "success",
        detail: { detached_count: result.changes, threshold_ms: staleThresholdMs },
      });
    }
    return result.changes;
  }

  heartbeat(runtimeId: string): void {
    this.stmts.updateHeartbeat.run(Date.now(), runtimeId);
  }

  // ─── Watch Management ───────────────────────────────────────────────

  addWatch(params: AddWatchParams): WatchRow {
    const session = this.stmts.getSession.get(params.session_id) as SessionRow | undefined;
    if (!session) throw new Error(`Session not found: ${params.session_id}`);

    const existing = this.stmts.getActiveWatchInSession.get(
      params.session_id,
      params.entity_type,
      params.entity_ref,
    ) as WatchRow | undefined;
    if (existing) {
      this.logger.info({ watchId: existing.watch_id, entityRef: params.entity_ref }, "watch already exists, returning existing");
      this.audit({
        category: "watch", action: "add_dedup", outcome: "noop",
        sessionId: params.session_id, watchId: existing.watch_id,
        detail: { entity_type: params.entity_type, entity_ref: params.entity_ref },
      });
      return existing;
    }

    const watchId = randomUUID();
    const now = Date.now();
    this.stmts.insertWatch.run(
      watchId,
      params.session_id,
      params.watch_type,
      params.entity_type,
      params.entity_ref,
      params.correlation_key || null,
      params.delivery_policy || "live-or-queue",
      params.fallback_policy || null,
      null,
      params.expires_at || null,
      now,
      now,
      params.metadata_json || null,
    );
    this.logger.info({ watchId, sessionId: params.session_id, watchType: params.watch_type, entityRef: params.entity_ref }, "watch added");
    this.audit({
      category: "watch", action: "added", outcome: "success",
      sessionId: params.session_id, watchId,
      detail: {
        watch_type: params.watch_type,
        entity_type: params.entity_type,
        entity_ref: params.entity_ref,
        correlation_key: params.correlation_key || null,
        delivery_policy: params.delivery_policy || "live-or-queue",
        expires_at: params.expires_at || null,
      },
    });
    return this.stmts.getWatch.get(watchId) as WatchRow;
  }

  removeWatch(watchId: string): void {
    const now = Date.now();
    this.stmts.updateWatchStatus.run("cancelled", now, watchId);
    this.logger.info({ watchId }, "watch removed");
    this.audit({ category: "watch", action: "removed", outcome: "success", watchId });
  }

  listWatches(sessionId: string): WatchRow[] {
    return this.stmts.listWatchesBySession.all(sessionId) as WatchRow[];
  }

  // ─── Event Ingestion Pipeline ───────────────────────────────────────

  ingestEvent(notification: ChannelNotification): IngestResult {
    const orch = notification.orchestration;
    const meta = notification.meta;

    const source = orch?.source || meta.plugin || "unknown";
    const eventKind = orch?.event_kind || meta.event_type || "unknown";
    const dedupKey = orch?.dedup_key || null;
    const correlationKey = orch?.correlation_key || null;
    const entityType = orch?.entity_type || null;
    const entityRef = orch?.entity_ref || null;
    const importanceHint = orch?.importance_hint || "normal";
    const titleHint = orch?.title_hint || notification.content.slice(0, 120);

    if (dedupKey) {
      const existing = this.stmts.getEventByDedup.get(dedupKey) as { event_id: string } | undefined;
      if (existing) {
        this.logger.debug({ dedupKey, existingEventId: existing.event_id }, "event deduplicated");
        this.audit({
          category: "event", action: "deduplicated", outcome: "deduplicated",
          eventId: existing.event_id,
          detail: { source, event_kind: eventKind, dedup_key: dedupKey },
        });
        return { eventId: existing.event_id, deduplicated: true, matchedWatches: 0, deliveries: [] };
      }
    }

    const eventId = randomUUID();
    const now = Date.now();

    this.stmts.insertEvent.run(
      eventId,
      source,
      eventKind,
      orch?.source_ref || meta.web_url || meta.url || null,
      orch?.thread_ref || null,
      orch?.actor_ref || meta.author || null,
      titleHint,
      importanceHint,
      dedupKey,
      correlationKey,
      JSON.stringify(meta),
      notification.orchestration ? JSON.stringify(notification.orchestration) : null,
      now,
    );

    const matchedWatches = this.matchWatches(entityType, entityRef, correlationKey);
    const deliveries: DeliveryDecision[] = [];

    this.audit({
      category: "event", action: "ingested", outcome: "success",
      eventId,
      detail: {
        source, event_kind: eventKind, entity_type: entityType, entity_ref: entityRef,
        correlation_key: correlationKey, importance: importanceHint,
        matched_watches: matchedWatches.length,
      },
    });

    if (matchedWatches.length === 0) {
      this.audit({
        category: "event", action: "unmatched", outcome: "noop",
        eventId,
        detail: { source, event_kind: eventKind, entity_type: entityType, entity_ref: entityRef, correlation_key: correlationKey },
      });
    }

    for (const watch of matchedWatches) {
      const session = this.stmts.getSession.get(watch.session_id) as SessionRow | undefined;
      if (!session || session.status === "archived") {
        this.audit({
          category: "delivery", action: "skipped_archived_session", outcome: "noop",
          sessionId: watch.session_id, watchId: watch.watch_id, eventId,
        });
        continue;
      }

      const runtime = (this.stmts.getActiveRuntime.get(watch.session_id) as RuntimeRow | undefined) || null;

      const attentionId = randomUUID();
      this.stmts.insertAttention.run(
        attentionId,
        watch.session_id,
        eventId,
        watch.watch_type,
        importanceHint,
        0,
        "new",
        null,
        titleHint,
        null,
        now,
        now,
      );
      this.audit({
        category: "attention", action: "created", outcome: "success",
        sessionId: watch.session_id, attentionId, eventId, watchId: watch.watch_id,
        detail: { category_hint: watch.watch_type, importance: importanceHint, summary_hint: titleHint },
      });

      const attention = this.stmts.getAttention.get(attentionId) as AttentionRow;
      const decision = decideDelivery(session, runtime, attention, watch);

      const deliveryId = randomUUID();
      if (decision.mode === "live") {
        this.stmts.insertDelivery.run(deliveryId, watch.session_id, eventId, attentionId, "delivered-live", now, now, null, null, null);
        this.stmts.updateAttentionState.run("delivered", now, attentionId);
        this.audit({
          category: "delivery", action: "decided_live", outcome: "live",
          sessionId: watch.session_id, deliveryId, attentionId, eventId, watchId: watch.watch_id,
          runtimeId: runtime?.runtime_id ?? null,
          detail: { delivery_policy: watch.delivery_policy, attached: true },
        });
        this.deliverLive(watch.session_id, notification.content, {
          orchestrated: "true",
          session_id: watch.session_id,
          event_id: eventId,
          attention_id: attentionId,
          source,
          event_kind: eventKind,
          importance: importanceHint,
        });
      } else {
        this.stmts.insertDelivery.run(deliveryId, watch.session_id, eventId, attentionId, "queued", now, null, null, null, null);
        this.stmts.updateAttentionState.run("queued", now, attentionId);
        this.audit({
          category: "delivery", action: "decided_queued", outcome: "queued",
          sessionId: watch.session_id, deliveryId, attentionId, eventId, watchId: watch.watch_id,
          detail: { delivery_policy: watch.delivery_policy, attached: false, reason: runtime ? "live-only-policy-detached" : "no-runtime" },
        });
      }

      deliveries.push(decision);
    }

    this.logger.info({ eventId, source, eventKind, matchedWatches: matchedWatches.length, deliveries: deliveries.length }, "event ingested");
    return { eventId, deduplicated: false, matchedWatches: matchedWatches.length, deliveries };
  }

  private matchWatches(
    entityType: string | null,
    entityRef: string | null,
    correlationKey: string | null,
  ): WatchRow[] {
    const seen = new Set<string>();
    const result: WatchRow[] = [];

    if (entityType && entityRef) {
      const byEntity = this.stmts.getActiveWatchesByEntity.all(entityType, entityRef) as WatchRow[];
      for (const w of byEntity) {
        if (!seen.has(w.watch_id)) {
          seen.add(w.watch_id);
          result.push(w);
        }
      }
    }

    if (correlationKey) {
      const byCorrelation = this.stmts.getActiveWatchesByCorrelation.all(correlationKey) as WatchRow[];
      for (const w of byCorrelation) {
        if (!seen.has(w.watch_id)) {
          seen.add(w.watch_id);
          result.push(w);
        }
      }
    }

    return result;
  }

  private deliverLive(sessionId: string, content: string, meta: Record<string, string>): void {
    if (!this.notifyCallback) {
      this.audit({
        category: "notify", action: "no_callback", outcome: "error",
        sessionId, eventId: meta.event_id ?? null, attentionId: meta.attention_id ?? null,
      });
      return;
    }
    this.notifyCallback(sessionId, content, meta).then(() => {
      this.audit({
        category: "notify", action: "emitted", outcome: "success",
        sessionId, eventId: meta.event_id ?? null, attentionId: meta.attention_id ?? null,
        detail: { source: meta.source, event_kind: meta.event_kind, importance: meta.importance },
      });
    }).catch((err) => {
      this.logger.error({ err, sessionId }, "live delivery failed");
      this.audit({
        category: "notify", action: "emit_failed", outcome: "error",
        sessionId, eventId: meta.event_id ?? null, attentionId: meta.attention_id ?? null,
        detail: { error: err instanceof Error ? err.message : String(err) },
      });
    });
  }

  // ─── Query Methods ──────────────────────────────────────────────────

  listSessionFeed(sessionId: string, opts?: { limit?: number; since?: number }): AttentionRow[] {
    const limit = opts?.limit || 50;
    if (opts?.since) {
      return this.stmts.listAttentionBySessionSince.all(sessionId, opts.since, limit) as AttentionRow[];
    }
    return this.stmts.listAttentionBySession.all(sessionId, limit) as AttentionRow[];
  }

  listPendingDeliveries(sessionId: string): PendingDeliveryRow[] {
    return this.stmts.listPendingBySession.all(sessionId) as PendingDeliveryRow[];
  }

  listUnmatchedEvents(opts?: { limit?: number; since?: number }): EventRow[] {
    const limit = opts?.limit || 50;
    const since = opts?.since || (Date.now() - 24 * 60 * 60 * 1000);
    return this.stmts.listUnmatchedEvents.all(since, limit) as EventRow[];
  }

  getDeliverySummary(sessionId: string): ReplaySummary {
    const pending = this.stmts.listPendingBySession.all(sessionId) as PendingDeliveryRow[];
    if (pending.length === 0) {
      return { totalPending: 0, byImportance: {}, bySource: {}, items: [] };
    }

    const attentionIds = [...new Set(pending.map((p) => p.attention_id).filter(Boolean))];
    const eventIds = [...new Set(pending.map((p) => p.event_id))];

    const attentionItems = attentionIds.map((id) => this.stmts.getAttention.get(id) as AttentionRow).filter(Boolean);
    const events = eventIds.map((id) => this.stmts.getEvent.get(id) as EventRow).filter(Boolean);

    return buildReplaySummary(pending, attentionItems, events);
  }

  // ─── Attention Lifecycle ────────────────────────────────────────────

  getAttention(attentionId: string): AttentionRow | null {
    return (this.stmts.getAttention.get(attentionId) as AttentionRow | undefined) || null;
  }

  ackAttention(attentionId: string): void {
    this.stmts.updateAttentionState.run("acked", Date.now(), attentionId);
    this.audit({ category: "attention", action: "acked", outcome: "success", attentionId });
  }

  snoozeAttention(attentionId: string, until: number): void {
    const attention = this.stmts.getAttention.get(attentionId) as AttentionRow | undefined;
    if (!attention) throw new Error(`Attention item not found: ${attentionId}`);
    const now = Date.now();
    this.stmts.updateAttentionState.run("snoozed", now, attentionId);
    this.audit({
      category: "attention", action: "snoozed", outcome: "success",
      attentionId, sessionId: attention.session_id,
      detail: { until_ms: until },
    });
  }

  resolveAttention(attentionId: string): void {
    this.stmts.updateAttentionState.run("resolved", Date.now(), attentionId);
    this.audit({ category: "attention", action: "resolved", outcome: "success", attentionId });
  }

  // ─── Replay ─────────────────────────────────────────────────────────

  async replayOnResume(
    sessionId: string,
    notifyFn: (content: string, meta: Record<string, string>) => Promise<void>,
  ): Promise<number> {
    const startedAt = Date.now();
    const count = await replayPendingDeliveries(sessionId, this.stmts, notifyFn);
    this.audit({
      category: "replay", action: count > 0 ? "completed" : "noop",
      outcome: count > 0 ? "success" : "noop",
      sessionId,
      detail: { items_replayed: count, duration_ms: Date.now() - startedAt },
    });
    return count;
  }

  listUnackedDeliveredLive(sessionId: string, withinMs: number = 60 * 60 * 1000): PendingDeliveryRow[] {
    const cutoff = Date.now() - withinMs;
    return this.stmts.listUnackedDeliveredLiveBySession.all(sessionId, cutoff) as PendingDeliveryRow[];
  }

  findDroppedNotifications(opts?: { sessionId?: string; graceMs?: number; limit?: number }): Array<Record<string, unknown>> {
    const graceMs = opts?.graceMs ?? 60_000;
    const cutoff = Date.now() - graceMs;
    const limit = Math.min(opts?.limit ?? 100, 1000);
    const sessionId = opts?.sessionId ?? null;
    const rows = this.stmts.findDroppedNotifications.all(cutoff, sessionId, sessionId, limit) as Array<Record<string, unknown>>;
    const now = Date.now();
    return rows.map((r) => ({
      ...r,
      age_ms: typeof r.delivered_at === "number" ? now - (r.delivered_at as number) : null,
    }));
  }

  ackDeliveriesBySession(sessionId: string): number {
    const result = this.stmts.ackDeliveriesBySession.run(Date.now(), sessionId);
    const changes = result.changes as number;
    if (changes > 0) {
      this.audit({
        category: "delivery", action: "bulk_acked", outcome: "success",
        sessionId, detail: { count: changes, source: "auto-surface" },
      });
    }
    return changes;
  }

  ackDelivery(deliveryId: string): void {
    this.stmts.markDeliveryAcked.run(Date.now(), deliveryId);
    this.audit({ category: "delivery", action: "acked", outcome: "success", deliveryId });
  }

  ackDeliveryByAttention(attentionId: string): void {
    this.stmts.ackDeliveryByAttention.run(Date.now(), attentionId);
    this.audit({ category: "delivery", action: "acked_by_attention", outcome: "success", attentionId });
  }

  // ─── Maintenance ────────────────────────────────────────────────────

  expireStaleWatches(): number {
    const now = Date.now();
    const result = this.stmts.expireStaleWatches.run(now, now);
    if (result.changes > 0) {
      this.logger.info({ expired: result.changes }, "stale watches expired");
      this.audit({
        category: "maintenance", action: "watches_expired", outcome: "success",
        detail: { count: result.changes },
      });
    }
    return result.changes;
  }

  expireStaleDeliveries(retentionMs?: number): number {
    const now = Date.now();
    const cutoff = now - (retentionMs || DEFAULT_DELIVERY_RETENTION_MS);
    const result = this.stmts.expireStaleDeliveries.run(now, cutoff);
    if (result.changes > 0) {
      this.logger.info({ expired: result.changes }, "stale deliveries expired");
      this.audit({
        category: "maintenance", action: "deliveries_expired", outcome: "success",
        detail: { count: result.changes, retention_ms: retentionMs || DEFAULT_DELIVERY_RETENTION_MS },
      });
    }
    return result.changes;
  }

  purgeOldEvents(retentionMs: number = 7 * 24 * 60 * 60 * 1000): number {
    const cutoff = Date.now() - retentionMs;
    const result = this.stmts.purgeOldEvents.run(cutoff);
    if (result.changes > 0) {
      this.logger.info({ purged: result.changes }, "old events purged");
      this.audit({
        category: "maintenance", action: "events_purged", outcome: "success",
        detail: { count: result.changes, retention_ms: retentionMs },
      });
    }
    return result.changes;
  }

  completeWatchesByEntity(entityType: string, entityRef: string): number {
    const now = Date.now();
    const result = this.stmts.completeWatchesByEntity.run(now, entityType, entityRef);
    if (result.changes > 0) {
      this.logger.info({ entityType, entityRef, completed: result.changes }, "watches auto-completed");
      this.audit({
        category: "watch", action: "auto_completed", outcome: "success",
        detail: { entity_type: entityType, entity_ref: entityRef, count: result.changes },
      });
    }
    return result.changes;
  }

}
