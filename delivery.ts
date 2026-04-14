import type {
  SessionRow,
  RuntimeRow,
  WatchRow,
  AttentionRow,
  EventRow,
  PendingDeliveryRow,
  OrchestratorStmts,
} from "./orchestrator-db.js";

export interface DeliveryDecision {
  mode: "live" | "queued";
  sessionId: string;
  attentionId: string;
  eventId: string;
}

export interface ReplaySummary {
  totalPending: number;
  byImportance: Record<string, number>;
  bySource: Record<string, number>;
  items: Array<{
    attentionId: string;
    eventId: string;
    summaryHint: string | null;
    importance: string;
  }>;
}

export function decideDelivery(
  _session: SessionRow,
  runtime: RuntimeRow | null,
  attention: AttentionRow,
  watch: WatchRow,
): DeliveryDecision {
  const isAttached = runtime !== null && runtime.attached === 1;

  if (watch.delivery_policy === "live-only" && !isAttached) {
    return {
      mode: "queued",
      sessionId: attention.session_id,
      attentionId: attention.attention_id,
      eventId: attention.event_id,
    };
  }

  return {
    mode: isAttached ? "live" : "queued",
    sessionId: attention.session_id,
    attentionId: attention.attention_id,
    eventId: attention.event_id,
  };
}

export function buildReplaySummary(
  pendingDeliveries: PendingDeliveryRow[],
  attentionItems: AttentionRow[],
  events: EventRow[],
): ReplaySummary {
  const attentionMap = new Map(attentionItems.map((a) => [a.attention_id, a]));
  const eventMap = new Map(events.map((e) => [e.event_id, e]));

  const byImportance: Record<string, number> = {};
  const bySource: Record<string, number> = {};
  const items: ReplaySummary["items"] = [];

  for (const delivery of pendingDeliveries) {
    const attention = attentionMap.get(delivery.attention_id || "");
    const event = eventMap.get(delivery.event_id);

    const importance = attention?.importance || "normal";
    byImportance[importance] = (byImportance[importance] || 0) + 1;

    if (event) {
      bySource[event.source] = (bySource[event.source] || 0) + 1;
    }

    items.push({
      attentionId: delivery.attention_id || "",
      eventId: delivery.event_id,
      summaryHint: attention?.summary_hint || event?.title_hint || null,
      importance,
    });
  }

  return {
    totalPending: pendingDeliveries.length,
    byImportance,
    bySource,
    items,
  };
}

export async function replayPendingDeliveries(
  sessionId: string,
  stmts: OrchestratorStmts,
  notifyFn: (content: string, meta: Record<string, string>) => Promise<void>,
): Promise<number> {
  const pending = stmts.listPendingBySession.all(sessionId) as PendingDeliveryRow[];
  if (pending.length === 0) return 0;

  const attentionIds = [...new Set(pending.map((p) => p.attention_id).filter(Boolean))];
  const eventIds = [...new Set(pending.map((p) => p.event_id))];

  const attentionItems = attentionIds.map((id) => stmts.getAttention.get(id) as AttentionRow).filter(Boolean);
  const events = eventIds.map((id) => stmts.getEvent.get(id) as EventRow).filter(Boolean);

  const summary = buildReplaySummary(pending, attentionItems, events);
  const now = Date.now();

  const high = summary.items.filter((i) => i.importance === "high" || i.importance === "critical");
  const rest = summary.items.filter((i) => i.importance !== "high" && i.importance !== "critical");

  for (const item of high) {
    const event = events.find((e) => e.event_id === item.eventId);
    await notifyFn(
      `[replay] ${item.summaryHint || event?.event_kind || "event"}`,
      {
        orchestrated: "true",
        replay: "true",
        session_id: sessionId,
        event_id: item.eventId,
        importance: item.importance,
        source: event?.source || "",
      },
    );
  }

  if (rest.length > 0) {
    const bySourceKind: Record<string, string[]> = {};
    for (const item of rest) {
      const event = events.find((e) => e.event_id === item.eventId);
      const key = event?.source || "unknown";
      if (!bySourceKind[key]) bySourceKind[key] = [];
      if (item.summaryHint) bySourceKind[key].push(item.summaryHint);
    }

    const sections = Object.entries(bySourceKind).map(([source, hints]) => {
      const unique = [...new Set(hints)].slice(0, 3);
      const detail = unique.length > 0 ? `: ${unique.join("; ")}` : "";
      const more = hints.length > 3 ? ` (+${hints.length - 3} more)` : "";
      return `  ${source}: ${hints.length} items${detail}${more}`;
    });

    await notifyFn(
      `[replay digest] ${rest.length} queued items:\n${sections.join("\n")}`,
      {
        orchestrated: "true",
        replay: "true",
        replay_type: "digest",
        session_id: sessionId,
        total: String(summary.totalPending),
      },
    );
  }

  for (const delivery of pending) {
    stmts.markDeliveryReplayed.run(now, delivery.delivery_id);
  }

  return pending.length;
}
