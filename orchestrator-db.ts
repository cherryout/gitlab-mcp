import Database from "better-sqlite3";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";

const DEFAULT_DB_DIR = join(homedir(), ".cache", "orchestrator");
const DEFAULT_DB_PATH = join(DEFAULT_DB_DIR, "orchestrator.db");

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    session_name TEXT,
    owner TEXT NOT NULL DEFAULT 'default',
    role TEXT NOT NULL DEFAULT 'main',
    workspace TEXT,
    project TEXT,
    repository TEXT,
    branch_hint TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    metadata_json TEXT
  );

  CREATE TABLE IF NOT EXISTS runtime_attachments (
    runtime_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    channel_name TEXT,
    attached INTEGER NOT NULL DEFAULT 1,
    started_at INTEGER NOT NULL,
    last_heartbeat_at INTEGER NOT NULL,
    runtime_metadata_json TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_ra_session ON runtime_attachments(session_id);

  CREATE TABLE IF NOT EXISTS watches (
    watch_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    watch_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_ref TEXT NOT NULL,
    correlation_key TEXT,
    delivery_policy TEXT NOT NULL DEFAULT 'live-or-queue',
    fallback_policy TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    grace_until INTEGER,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    metadata_json TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_watches_session ON watches(session_id);
  CREATE INDEX IF NOT EXISTS idx_watches_entity ON watches(entity_type, entity_ref);
  CREATE INDEX IF NOT EXISTS idx_watches_correlation ON watches(correlation_key);
  CREATE INDEX IF NOT EXISTS idx_watches_status ON watches(status);

  CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    source_ref TEXT,
    thread_ref TEXT,
    actor_ref TEXT,
    title_hint TEXT,
    importance_hint TEXT DEFAULT 'normal',
    dedup_key TEXT,
    correlation_key TEXT,
    raw_payload_ref TEXT,
    normalized_json TEXT,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_events_dedup ON events(dedup_key);
  CREATE INDEX IF NOT EXISTS idx_events_correlation ON events(correlation_key);
  CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);

  CREATE TABLE IF NOT EXISTS attention_items (
    attention_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    event_id TEXT NOT NULL REFERENCES events(event_id),
    category TEXT,
    importance TEXT DEFAULT 'normal',
    requires_action INTEGER DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'new',
    delivery_mode TEXT,
    summary_hint TEXT,
    reminder_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_ai_session ON attention_items(session_id);
  CREATE INDEX IF NOT EXISTS idx_ai_state ON attention_items(state);
  CREATE INDEX IF NOT EXISTS idx_ai_event ON attention_items(event_id);

  CREATE TABLE IF NOT EXISTS pending_deliveries (
    delivery_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    event_id TEXT NOT NULL REFERENCES events(event_id),
    attention_id TEXT REFERENCES attention_items(attention_id),
    delivery_state TEXT NOT NULL DEFAULT 'queued',
    queued_at INTEGER NOT NULL,
    delivered_at INTEGER,
    replayed_at INTEGER,
    expired_at INTEGER,
    acked_at INTEGER,
    metadata_json TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_pd_session ON pending_deliveries(session_id);
  CREATE INDEX IF NOT EXISTS idx_pd_state ON pending_deliveries(delivery_state);

  CREATE TABLE IF NOT EXISTS orchestrator_audit_log (
    audit_id TEXT PRIMARY KEY,
    ts INTEGER NOT NULL,
    session_id TEXT,
    runtime_id TEXT,
    watch_id TEXT,
    event_id TEXT,
    attention_id TEXT,
    delivery_id TEXT,
    category TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT,
    detail_json TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_audit_ts ON orchestrator_audit_log(ts);
  CREATE INDEX IF NOT EXISTS idx_audit_session ON orchestrator_audit_log(session_id, ts);
  CREATE INDEX IF NOT EXISTS idx_audit_category ON orchestrator_audit_log(category, ts);
  CREATE INDEX IF NOT EXISTS idx_audit_delivery ON orchestrator_audit_log(delivery_id);
  CREATE INDEX IF NOT EXISTS idx_audit_attention ON orchestrator_audit_log(attention_id);
  CREATE INDEX IF NOT EXISTS idx_audit_event ON orchestrator_audit_log(event_id);
  CREATE INDEX IF NOT EXISTS idx_audit_watch ON orchestrator_audit_log(watch_id);

  CREATE TABLE IF NOT EXISTS source_checkpoints (
    source TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    last_cursor TEXT,
    last_seen_timestamp INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (source, scope_key)
  );
`;

export interface SessionRow {
  session_id: string;
  session_name: string | null;
  owner: string;
  role: string;
  workspace: string | null;
  project: string | null;
  repository: string | null;
  branch_hint: string | null;
  status: string;
  created_at: number;
  last_seen_at: number;
  metadata_json: string | null;
}

export interface RuntimeRow {
  runtime_id: string;
  session_id: string;
  channel_name: string | null;
  attached: number;
  started_at: number;
  last_heartbeat_at: number;
  runtime_metadata_json: string | null;
}

export interface WatchRow {
  watch_id: string;
  session_id: string;
  watch_type: string;
  entity_type: string;
  entity_ref: string;
  correlation_key: string | null;
  delivery_policy: string;
  fallback_policy: string | null;
  status: string;
  grace_until: number | null;
  expires_at: number | null;
  created_at: number;
  updated_at: number;
  metadata_json: string | null;
}

export interface EventRow {
  event_id: string;
  source: string;
  event_kind: string;
  source_ref: string | null;
  thread_ref: string | null;
  actor_ref: string | null;
  title_hint: string | null;
  importance_hint: string | null;
  dedup_key: string | null;
  correlation_key: string | null;
  raw_payload_ref: string | null;
  normalized_json: string | null;
  created_at: number;
}

export interface AttentionRow {
  attention_id: string;
  session_id: string;
  event_id: string;
  category: string | null;
  importance: string;
  requires_action: number;
  state: string;
  delivery_mode: string | null;
  summary_hint: string | null;
  reminder_at: number | null;
  created_at: number;
  updated_at: number;
}

export interface PendingDeliveryRow {
  delivery_id: string;
  session_id: string;
  event_id: string;
  attention_id: string | null;
  delivery_state: string;
  queued_at: number;
  delivered_at: number | null;
  replayed_at: number | null;
  expired_at: number | null;
  acked_at: number | null;
  metadata_json: string | null;
}

export interface CheckpointRow {
  source: string;
  scope_key: string;
  last_cursor: string | null;
  last_seen_timestamp: number | null;
  updated_at: number;
}

export interface AuditRow {
  audit_id: string;
  ts: number;
  session_id: string | null;
  runtime_id: string | null;
  watch_id: string | null;
  event_id: string | null;
  attention_id: string | null;
  delivery_id: string | null;
  category: string;
  action: string;
  outcome: string | null;
  detail_json: string | null;
}

export interface OrchestratorStmts {
  getSession: Database.Statement;
  insertSession: Database.Statement;
  updateSessionStatus: Database.Statement;
  updateSessionLastSeen: Database.Statement;
  listSessions: Database.Statement;
  findResumableSession: Database.Statement;

  insertRuntime: Database.Statement;
  getActiveRuntime: Database.Statement;
  detachRuntime: Database.Statement;
  detachAllRuntimes: Database.Statement;
  detachStaleRuntimes: Database.Statement;
  updateHeartbeat: Database.Statement;

  insertWatch: Database.Statement;
  getWatch: Database.Statement;
  listWatchesBySession: Database.Statement;
  updateWatchStatus: Database.Statement;
  getActiveWatchesByEntity: Database.Statement;
  getActiveWatchInSession: Database.Statement;
  getActiveWatchesByCorrelation: Database.Statement;
  expireStaleWatches: Database.Statement;

  insertEvent: Database.Statement;
  getEventByDedup: Database.Statement;
  getEvent: Database.Statement;
  listRecentEvents: Database.Statement;
  listUnmatchedEvents: Database.Statement;

  insertAttention: Database.Statement;
  updateAttentionState: Database.Statement;
  listAttentionBySession: Database.Statement;
  listAttentionBySessionSince: Database.Statement;
  getAttention: Database.Statement;

  insertDelivery: Database.Statement;
  updateDeliveryState: Database.Statement;
  markDeliveryDelivered: Database.Statement;
  markDeliveryReplayed: Database.Statement;
  markDeliveryAcked: Database.Statement;
  ackDeliveryByAttention: Database.Statement;
  ackDeliveriesBySession: Database.Statement;
  listPendingBySession: Database.Statement;
  findDroppedNotifications: Database.Statement;
  listForReplayBySession: Database.Statement;
  listUnackedDeliveredLiveBySession: Database.Statement;
  expireStaleDeliveries: Database.Statement;
  purgeOldEvents: Database.Statement;
  completeWatchesByEntity: Database.Statement;

  upsertCheckpoint: Database.Statement;
  getCheckpoint: Database.Statement;

  insertAudit: Database.Statement;
  listAudit: Database.Statement;
  listAuditFiltered: Database.Statement;
  purgeOldAudit: Database.Statement;
  countAudit: Database.Statement;
}

export interface OrchestratorDb {
  db: Database.Database;
  stmts: OrchestratorStmts;
  close(): void;
}

export function createOrchestratorDb(dbPath?: string): OrchestratorDb {
  const path = dbPath || DEFAULT_DB_PATH;
  mkdirSync(DEFAULT_DB_DIR, { recursive: true });

  const db = new Database(path);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA);

  const cols = db.prepare("PRAGMA table_info(pending_deliveries)").all() as Array<{ name: string }>;
  if (!cols.some((c) => c.name === "acked_at")) {
    db.exec("ALTER TABLE pending_deliveries ADD COLUMN acked_at INTEGER");
    db.exec("CREATE INDEX IF NOT EXISTS idx_pd_acked ON pending_deliveries(session_id, acked_at)");
  }

  const stmts: OrchestratorStmts = {
    getSession: db.prepare("SELECT * FROM sessions WHERE session_id = ?"),
    insertSession: db.prepare(`
      INSERT INTO sessions (session_id, session_name, owner, role, workspace, project, repository, branch_hint, status, created_at, last_seen_at, metadata_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
    `),
    updateSessionStatus: db.prepare("UPDATE sessions SET status = ?, last_seen_at = ? WHERE session_id = ?"),
    updateSessionLastSeen: db.prepare("UPDATE sessions SET last_seen_at = ? WHERE session_id = ?"),
    listSessions: db.prepare("SELECT * FROM sessions WHERE status IN ('active', 'resumable') ORDER BY last_seen_at DESC"),
    findResumableSession: db.prepare("SELECT * FROM sessions WHERE owner = ? AND status IN ('resumable', 'active') ORDER BY last_seen_at DESC LIMIT 1"),
    detachAllRuntimes: db.prepare("UPDATE runtime_attachments SET attached = 0 WHERE session_id = ? AND attached = 1"),
    detachStaleRuntimes: db.prepare("UPDATE runtime_attachments SET attached = 0 WHERE attached = 1 AND last_heartbeat_at < ?"),

    insertRuntime: db.prepare(`
      INSERT INTO runtime_attachments (runtime_id, session_id, channel_name, attached, started_at, last_heartbeat_at, runtime_metadata_json)
      VALUES (?, ?, ?, 1, ?, ?, ?)
    `),
    getActiveRuntime: db.prepare("SELECT * FROM runtime_attachments WHERE session_id = ? AND attached = 1 ORDER BY started_at DESC LIMIT 1"),
    detachRuntime: db.prepare("UPDATE runtime_attachments SET attached = 0 WHERE runtime_id = ?"),
    updateHeartbeat: db.prepare("UPDATE runtime_attachments SET last_heartbeat_at = ? WHERE runtime_id = ?"),

    insertWatch: db.prepare(`
      INSERT INTO watches (watch_id, session_id, watch_type, entity_type, entity_ref, correlation_key, delivery_policy, fallback_policy, status, grace_until, expires_at, created_at, updated_at, metadata_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
    `),
    getWatch: db.prepare("SELECT * FROM watches WHERE watch_id = ?"),
    listWatchesBySession: db.prepare("SELECT * FROM watches WHERE session_id = ? AND status = 'active' ORDER BY created_at DESC"),
    updateWatchStatus: db.prepare("UPDATE watches SET status = ?, updated_at = ? WHERE watch_id = ?"),
    getActiveWatchesByEntity: db.prepare("SELECT * FROM watches WHERE entity_type = ? AND entity_ref = ? AND status = 'active'"),
    getActiveWatchInSession: db.prepare("SELECT * FROM watches WHERE session_id = ? AND entity_type = ? AND entity_ref = ? AND status = 'active' LIMIT 1"),
    getActiveWatchesByCorrelation: db.prepare("SELECT * FROM watches WHERE correlation_key = ? AND status = 'active'"),
    expireStaleWatches: db.prepare("UPDATE watches SET status = 'expired', updated_at = ? WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at < ?"),

    insertEvent: db.prepare(`
      INSERT INTO events (event_id, source, event_kind, source_ref, thread_ref, actor_ref, title_hint, importance_hint, dedup_key, correlation_key, raw_payload_ref, normalized_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
    getEventByDedup: db.prepare("SELECT event_id FROM events WHERE dedup_key = ?"),
    getEvent: db.prepare("SELECT * FROM events WHERE event_id = ?"),
    listRecentEvents: db.prepare("SELECT * FROM events ORDER BY created_at DESC LIMIT ?"),
    listUnmatchedEvents: db.prepare(`
      SELECT e.* FROM events e
      LEFT JOIN attention_items ai ON ai.event_id = e.event_id
      WHERE ai.attention_id IS NULL AND e.created_at > ?
      ORDER BY e.created_at DESC LIMIT ?
    `),

    insertAttention: db.prepare(`
      INSERT INTO attention_items (attention_id, session_id, event_id, category, importance, requires_action, state, delivery_mode, summary_hint, reminder_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
    updateAttentionState: db.prepare("UPDATE attention_items SET state = ?, updated_at = ? WHERE attention_id = ?"),
    listAttentionBySession: db.prepare("SELECT * FROM attention_items WHERE session_id = ? ORDER BY created_at DESC LIMIT ?"),
    listAttentionBySessionSince: db.prepare("SELECT * FROM attention_items WHERE session_id = ? AND created_at > ? ORDER BY created_at DESC LIMIT ?"),
    getAttention: db.prepare("SELECT * FROM attention_items WHERE attention_id = ?"),

    insertDelivery: db.prepare(`
      INSERT INTO pending_deliveries (delivery_id, session_id, event_id, attention_id, delivery_state, queued_at, delivered_at, replayed_at, expired_at, metadata_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
    updateDeliveryState: db.prepare("UPDATE pending_deliveries SET delivery_state = ? WHERE delivery_id = ?"),
    markDeliveryDelivered: db.prepare("UPDATE pending_deliveries SET delivery_state = 'delivered-live', delivered_at = ? WHERE delivery_id = ?"),
    markDeliveryReplayed: db.prepare("UPDATE pending_deliveries SET delivery_state = 'replayed-on-resume', replayed_at = ?, acked_at = ? WHERE delivery_id = ?"),
    markDeliveryAcked: db.prepare("UPDATE pending_deliveries SET acked_at = ? WHERE delivery_id = ? AND acked_at IS NULL"),
    ackDeliveryByAttention: db.prepare("UPDATE pending_deliveries SET acked_at = ? WHERE attention_id = ? AND acked_at IS NULL"),
    ackDeliveriesBySession: db.prepare("UPDATE pending_deliveries SET acked_at = ? WHERE session_id = ? AND delivery_state = 'delivered-live' AND acked_at IS NULL"),
    listPendingBySession: db.prepare("SELECT * FROM pending_deliveries WHERE session_id = ? AND delivery_state = 'queued' ORDER BY queued_at ASC"),
    findDroppedNotifications: db.prepare(`
      SELECT
        pd.delivery_id, pd.session_id, pd.event_id, pd.attention_id,
        pd.delivered_at, pd.queued_at,
        ai.summary_hint, ai.importance, ai.category as attention_category,
        e.source, e.event_kind, e.title_hint as event_title,
        s.session_name, s.owner, s.role
      FROM pending_deliveries pd
      LEFT JOIN attention_items ai ON ai.attention_id = pd.attention_id
      LEFT JOIN events e ON e.event_id = pd.event_id
      LEFT JOIN sessions s ON s.session_id = pd.session_id
      WHERE pd.delivery_state = 'delivered-live'
        AND pd.acked_at IS NULL
        AND pd.delivered_at IS NOT NULL
        AND pd.delivered_at <= ?
        AND (? IS NULL OR pd.session_id = ?)
      ORDER BY pd.delivered_at DESC
      LIMIT ?
    `),
    listForReplayBySession: db.prepare(`
      SELECT * FROM pending_deliveries
      WHERE session_id = ?
        AND ((delivery_state = 'queued')
             OR (delivery_state = 'delivered-live' AND acked_at IS NULL))
      ORDER BY queued_at ASC
    `),
    listUnackedDeliveredLiveBySession: db.prepare(`
      SELECT * FROM pending_deliveries
      WHERE session_id = ?
        AND delivery_state = 'delivered-live'
        AND acked_at IS NULL
        AND delivered_at >= ?
      ORDER BY delivered_at ASC
    `),
    expireStaleDeliveries: db.prepare("UPDATE pending_deliveries SET delivery_state = 'expired', expired_at = ? WHERE delivery_state = 'queued' AND queued_at < ?"),
    purgeOldEvents: db.prepare("DELETE FROM events WHERE created_at < ? AND event_id NOT IN (SELECT event_id FROM attention_items) AND event_id NOT IN (SELECT event_id FROM pending_deliveries WHERE delivery_state = 'queued')"),
    completeWatchesByEntity: db.prepare("UPDATE watches SET status = 'completed', updated_at = ? WHERE entity_type = ? AND entity_ref = ? AND status = 'active'"),

    upsertCheckpoint: db.prepare(`
      INSERT INTO source_checkpoints (source, scope_key, last_cursor, last_seen_timestamp, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(source, scope_key) DO UPDATE SET last_cursor=excluded.last_cursor, last_seen_timestamp=excluded.last_seen_timestamp, updated_at=excluded.updated_at
    `),
    getCheckpoint: db.prepare("SELECT * FROM source_checkpoints WHERE source = ? AND scope_key = ?"),

    insertAudit: db.prepare(`
      INSERT INTO orchestrator_audit_log
        (audit_id, ts, session_id, runtime_id, watch_id, event_id, attention_id, delivery_id, category, action, outcome, detail_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),
    listAudit: db.prepare(`
      SELECT * FROM orchestrator_audit_log
      WHERE ts >= ? AND ts <= ?
      ORDER BY ts DESC
      LIMIT ?
    `),
    listAuditFiltered: db.prepare(`
      SELECT * FROM orchestrator_audit_log
      WHERE ts >= ? AND ts <= ?
        AND (? IS NULL OR session_id = ?)
        AND (? IS NULL OR category = ?)
        AND (? IS NULL OR action = ?)
        AND (? IS NULL OR delivery_id = ?)
        AND (? IS NULL OR attention_id = ?)
        AND (? IS NULL OR event_id = ?)
        AND (? IS NULL OR watch_id = ?)
      ORDER BY ts DESC
      LIMIT ?
    `),
    purgeOldAudit: db.prepare("DELETE FROM orchestrator_audit_log WHERE ts <= ?"),
    countAudit: db.prepare("SELECT COUNT(*) as n FROM orchestrator_audit_log"),
  };

  return {
    db,
    stmts,
    close() {
      db.close();
    },
  };
}
