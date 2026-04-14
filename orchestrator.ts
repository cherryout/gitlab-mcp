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
    return this.stmts.getSession.get(sessionId) as SessionRow;
  }

  closeSession(sessionId: string): void {
    const session = this.stmts.getSession.get(sessionId) as SessionRow | undefined;
    if (!session) throw new Error(`Session not found: ${sessionId}`);
    const now = Date.now();
    this.stmts.updateSessionStatus.run("archived", now, sessionId);
    this.logger.info({ sessionId }, "session closed");
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
    return this.stmts.getActiveRuntime.get(sessionId) as RuntimeRow;
  }

  detachRuntime(runtimeId: string): void {
    this.stmts.detachRuntime.run(runtimeId);
    this.logger.info({ runtimeId }, "runtime detached");
  }

  heartbeat(runtimeId: string): void {
    this.stmts.updateHeartbeat.run(Date.now(), runtimeId);
  }

  // ─── Watch Management ───────────────────────────────────────────────

  addWatch(params: AddWatchParams): WatchRow {
    const session = this.stmts.getSession.get(params.session_id) as SessionRow | undefined;
    if (!session) throw new Error(`Session not found: ${params.session_id}`);

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
    return this.stmts.getWatch.get(watchId) as WatchRow;
  }

  removeWatch(watchId: string): void {
    const now = Date.now();
    this.stmts.updateWatchStatus.run("cancelled", now, watchId);
    this.logger.info({ watchId }, "watch removed");
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

    for (const watch of matchedWatches) {
      const session = this.stmts.getSession.get(watch.session_id) as SessionRow | undefined;
      if (!session || session.status === "archived") continue;

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

      const attention = this.stmts.getAttention.get(attentionId) as AttentionRow;
      const decision = decideDelivery(session, runtime, attention, watch);

      const deliveryId = randomUUID();
      if (decision.mode === "live") {
        this.stmts.insertDelivery.run(deliveryId, watch.session_id, eventId, attentionId, "delivered-live", now, now, null, null, null);
        this.stmts.updateAttentionState.run("delivered", now, attentionId);
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
    if (this.notifyCallback) {
      this.notifyCallback(sessionId, content, meta).catch((err) => {
        this.logger.error({ err, sessionId }, "live delivery failed");
      });
    }
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

  ackAttention(attentionId: string): void {
    this.stmts.updateAttentionState.run("acked", Date.now(), attentionId);
  }

  snoozeAttention(attentionId: string, until: number): void {
    const attention = this.stmts.getAttention.get(attentionId) as AttentionRow | undefined;
    if (!attention) throw new Error(`Attention item not found: ${attentionId}`);
    const now = Date.now();
    this.stmts.updateAttentionState.run("snoozed", now, attentionId);
  }

  resolveAttention(attentionId: string): void {
    this.stmts.updateAttentionState.run("resolved", Date.now(), attentionId);
  }

  // ─── Replay ─────────────────────────────────────────────────────────

  async replayOnResume(
    sessionId: string,
    notifyFn: (content: string, meta: Record<string, string>) => Promise<void>,
  ): Promise<number> {
    return replayPendingDeliveries(sessionId, this.stmts, notifyFn);
  }

  // ─── Maintenance ────────────────────────────────────────────────────

  expireStaleWatches(): number {
    const now = Date.now();
    const result = this.stmts.expireStaleWatches.run(now, now);
    if (result.changes > 0) {
      this.logger.info({ expired: result.changes }, "stale watches expired");
    }
    return result.changes;
  }

  expireStaleDeliveries(retentionMs?: number): number {
    const now = Date.now();
    const cutoff = now - (retentionMs || DEFAULT_DELIVERY_RETENTION_MS);
    const result = this.stmts.expireStaleDeliveries.run(now, cutoff);
    if (result.changes > 0) {
      this.logger.info({ expired: result.changes }, "stale deliveries expired");
    }
    return result.changes;
  }
}
