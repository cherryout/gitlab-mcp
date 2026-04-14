# Agentic Session Orchestration for Cloud Code

## Final Architecture Specification

## 1. Purpose

This document defines the final architecture for an **agentic, session-centric, event-driven workflow system** built around Cloud Code sessions, MCP tools, channels, and a lightweight orchestration layer.

The goal is to preserve the current strengths of the existing setup:

- Cloud Code sessions as the main working environment
- MCP-based access to Slack, Telegram, GitLab, Jenkins, and Target Process
- Skill-driven workflows for inbox triage, monitoring, investigation, and replies
- Human approval before important actions
- Future UI shell support for mobile, tablet, and browser-based control

At the same time, the solution adds what is currently missing:

- reactive event awareness
- durable watch tracking
- session-aware routing
- delivery buffering when a live runtime is not attached
- controlled orchestration around sessions without replacing them

This specification intentionally does **not** aim to build a heavy generalized enterprise workflow platform. It defines the **minimum complete architecture** needed to make the existing skill-based system reactive, resilient, and manageable.

---

## 2. Core Decision

The system will use the following model:

> **Cloud Code sessions remain the primary operational surface.**
>
> **An external orchestrator becomes the control plane for events, watches, routing, and delivery.**
>
> **Source-specific MCP tools remain responsible for native domain access and actions.**

This is a hybrid model:

- **Sessions are not replaced** by a centralized case engine.
- **Orchestration is added around sessions**, not instead of them.
- **Events flow through a shared pipeline**, not directly from external systems into sessions.
- **Sessions declare watches and interests**, and the orchestrator routes relevant events back into those sessions.

---

## 3. Main Principles

### 3.1 Session-first, not session-only
Cloud Code sessions remain the center of user work. They are where tasks are investigated, messages are reviewed, actions are approved, and operational context is consumed.

However, sessions must not be the only place where event state lives.

### 3.2 Session is a resumable operation log
A Cloud Code session is not a stateless worker. It is a persisted conversation and operation history that can be resumed or continued later.

This means:

- closing a live runtime does **not** destroy the session identity
- watches should be tied to the **session identity**, not only to a live terminal process
- events can continue to accumulate for a session while no live runtime is attached

### 3.3 Runtime attachment is separate from session identity
A session can exist without an active live runtime.

Therefore, the system must distinguish between:

- **session identity/history**
- **live runtime attachment**
- **event delivery endpoint**

### 3.4 Unify attention, not domain semantics
The system should **not** attempt to fully normalize all source data into one universal business model.

Instead, it should unify only what is required for:

- wake-up logic
- deduplication
- routing
- watch ownership
- delivery tracking
- backlog and replay

Native source semantics remain in source-specific MCP tools.

### 3.5 Events go through the orchestrator
External systems must not notify sessions directly.

Instead:

- GitLab, Jenkins, Slack, Telegram, and Target Process send events into the orchestrator
- the orchestrator normalizes, deduplicates, correlates, and routes them
- sessions receive only relevant events through the orchestration channel

### 3.6 Sessions subscribe to watches, not raw systems
A session should not directly subscribe to GitLab, Jenkins, Slack, or Telegram transports.

Instead, a session declares:

- what it cares about
- what entity it is watching
- under what delivery policy

Then the orchestrator decides which incoming events belong to that watch.

### 3.7 Source MCPs stay source-native
Slack MCP, GitLab MCP, Jenkins MCP, Telegram MCP, and Target Process MCP should remain source-native.

They are responsible for:

- fetching source details
- hydrating context
- performing source-specific actions
- replying or mutating state inside that system

They should not become thin adapters over a universal database.

---

## 4. System Scope

This architecture covers the following sources:

- **Slack**: DMs, mentions, watched threads, operational alerts
- **Telegram**: watched chats, request intake, follow-ups
- **GitLab**: merge requests, comments, pipelines, assignments, reviews
- **Jenkins**: deploy results, job failures, post-build operational events
- **Target Process**: assigned items, task changes, status changes

The system is local-first and can initially run without Kafka or large infrastructure.

---

## 5. High-Level Architecture

```text
External Sources
  ├─ Slack Events / Pollers
  ├─ Telegram Updates / Pollers
  ├─ GitLab Webhooks / Pollers
  ├─ Jenkins Webhooks / Pollers
  └─ Target Process Pollers
           |
           v
   Source Adapters / Connectors
           |
           v
   Local Orchestrator Pipeline
     - normalize event envelope
     - deduplicate
     - correlate
     - match watches
     - create attention items
     - queue pending deliveries
     - decide delivery policy
           |
           +------------------------------+
           |                              |
           v                              v
   Orchestration Storage             Orchestration Channel
   - sessions                        - session notifications
   - runtime attachments             - replay notices
   - watches                         - live event delivery
   - events
   - attention items
   - pending deliveries
           |
           v
   Cloud Code Sessions
     - interactive work
     - invoke MCP skills
     - approve actions
     - inspect context
     - add/remove watches
```

---

## 6. Key Architectural Statement

The system should be designed around the following statement:

> **Session says “watch this”. Orchestrator says “this event belongs to you”.**

This is the central contract.

---

## 7. Session Model

### 7.1 Session identity
A session is a persisted work object representing a resumable operation log and conversation history.

A session record should include:

- `session_id`
- `session_name` (optional, human-friendly)
- `owner`
- `role`
- `workspace`
- `project`
- `repository`
- `branch_hint` (optional)
- `status` (`active`, `resumable`, `archived`)
- `created_at`
- `last_seen_at`
- `metadata_json`

### 7.2 Session roles
Suggested roles:

- `main`
- `inbox`
- `bugfix`
- `review`
- `ops`
- `release-watch`
- `background-investigation`
- `system-managed`

Roles do not replace watches, but they help with fallback routing.

### 7.3 Runtime attachment
A session may or may not have a currently attached live runtime.

Runtime attachment should be modeled separately from the session.

Fields:

- `runtime_id`
- `session_id`
- `channel_name`
- `attached`
- `started_at`
- `last_heartbeat_at`
- `runtime_metadata_json`

This separation is critical because session history can continue to exist even when the live process is closed.

---

## 8. Watch Model

### 8.1 Why watches exist
Watches connect a session to an entity, correlation space, or operational flow.

They answer the question:

> “Which future events should be routed back into this session?”

### 8.2 Watch ownership
A watch belongs to a **session identity**, not only to a live runtime.

This is required because:

- sessions can be resumed later
- events can arrive while no live runtime is attached
- event history must still be attributable to the same work context

### 8.3 Watch fields
A watch should include:

- `watch_id`
- `session_id`
- `watch_type`
- `entity_type`
- `entity_ref`
- `correlation_key`
- `delivery_policy`
- `fallback_policy`
- `status`
- `grace_until`
- `expires_at`
- `created_at`
- `updated_at`
- `metadata_json`

### 8.4 Watch types
Suggested watch types:

- `pipeline-chain`
- `deploy-chain`
- `merge-request`
- `mr-comments`
- `slack-thread`
- `telegram-thread`
- `targetprocess-item`
- `branch`
- `task-followup`
- `release-monitor`

### 8.5 Watch statuses
Suggested statuses:

- `active`
- `suspended`
- `completed`
- `expired`
- `cancelled`

A watch should generally remain active even if no runtime is attached to the session.

---

## 9. Event Model

### 9.1 Event purpose
The event model is not meant to replace native source data. It exists to support orchestration.

### 9.2 Canonical event envelope
Each event should have a minimal shared envelope:

- `event_id`
- `source`
- `event_kind`
- `timestamp`
- `source_ref`
- `thread_ref`
- `actor_ref`
- `title_hint`
- `importance_hint`
- `dedup_key`
- `correlation_key`
- `raw_payload_ref`
- `normalized_json`

### 9.3 Raw payload storage
Source-native payload should also be stored separately or referenced.

Examples:

- GitLab webhook payload
- Jenkins build metadata payload
- Slack event payload
- Telegram message update payload
- Target Process snapshot

The event envelope is for orchestration. The raw payload is for traceability and debugging.

---

## 10. Attention Item Model

### 10.1 Why attention items exist
An event is not yet a user-facing alert. The system needs an intermediate object representing:

- something that deserves attention
- whether it has been delivered
- whether it requires action
- what its lifecycle is

### 10.2 Fields
Suggested fields:

- `attention_id`
- `session_id`
- `event_id`
- `category`
- `importance`
- `requires_action`
- `state`
- `delivery_mode`
- `summary_hint`
- `reminder_at`
- `created_at`
- `updated_at`

### 10.3 Attention states
Suggested states:

- `new`
- `queued`
- `delivered`
- `acked`
- `snoozed`
- `resolved`
- `expired`

### 10.4 Important principle
The system should **unify attention**, not full domain semantics.

That means the attention layer becomes the common operational surface for routing and user awareness.

---

## 11. Pending Delivery Model

When a session has no active runtime attached, relevant events should not be lost. They should be queued for that session.

Suggested fields:

- `delivery_id`
- `session_id`
- `event_id`
- `attention_id`
- `delivery_state`
- `queued_at`
- `delivered_at`
- `replayed_at`
- `expired_at`
- `metadata_json`

Suggested delivery states:

- `queued`
- `delivered-live`
- `replayed-on-resume`
- `summarized-on-resume`
- `expired`
- `rerouted`

---

## 12. Source Checkpoints

Source checkpointing remains source-specific and should not be removed.

Suggested fields:

- `source`
- `scope_key`
- `last_cursor`
- `last_seen_timestamp`
- `updated_at`

These checkpoints support:

- differential polling
- deduplication
- recovery after restart
- stable incremental ingestion

---

## 13. Orchestration MCP

The orchestrator should be exposed to sessions through a dedicated MCP interface.

### 13.1 Purpose
The orchestration MCP is the shared control-plane MCP.

It does **not** replace source-specific MCPs.

### 13.2 Suggested operations
- `register_session`
- `close_session`
- `attach_runtime`
- `detach_runtime`
- `add_watch`
- `remove_watch`
- `list_watches`
- `list_session_feed`
- `list_pending_deliveries`
- `ack_attention`
- `snooze_attention`
- `resolve_attention`
- `list_unmatched_events`
- `get_delivery_summary`
- `get_session_state`

### 13.3 Responsibility boundary
The orchestration MCP answers:

> “What is important to this session right now?”

Source MCPs answer:

> “How do I fetch details or act inside this source system?”

---

## 14. Source MCPs

Source MCPs remain source-specific.

### 14.1 Slack MCP
Responsible for:

- fetch DM threads
- fetch mentions
- fetch thread context
- send replies
- inspect referenced messages

### 14.2 GitLab MCP
Responsible for:

- fetch MR details
- fetch comments
- fetch pipelines
- inspect job logs
- approve/review/comment actions

### 14.3 Jenkins MCP
Responsible for:

- inspect deploy status
- inspect build logs
- fetch downstream job chain
- correlate deploy details

### 14.4 Telegram MCP
Responsible for:

- fetch chat history
- fetch follow-up messages
- reply to messages
- inspect topic/thread context

### 14.5 Target Process MCP
Responsible for:

- fetch task details
- inspect changes
- fetch comments and assignments
- mutate task state if needed

---

## 15. Channels Strategy

### 15.1 Main decision
Use **one orchestration channel** for session delivery.

Do **not** subscribe sessions directly to raw GitLab/Jenkins/Slack/Telegram channels.

### 15.2 Why
This preserves:

- unified routing
- deduplication
- fallback delivery
- consistent session semantics
- future extensibility

### 15.3 Channel purpose
The orchestration channel is a **delivery transport**, not the source of truth.

It carries:

- live event notifications
- replay notices
- backlog summaries
- route-to-session messages

### 15.4 Wrapper role
A shell/UI wrapper should launch sessions with the orchestration channel configured and register the session with the orchestrator.

The wrapper is the **session supervisor**, not the main event pipeline.

---

## 16. Shell/UI Wrapper

### 16.1 Purpose
The shell/UI wrapper manages session lifecycle and user interaction surfaces.

### 16.2 Responsibilities
- start session with correct channel configuration
- register or restore session identity
- attach runtime metadata
- present feed, watches, and backlog
- support mobile/tablet/browser management
- help resume prior sessions
- show linked background sessions if introduced later

### 16.3 Non-responsibilities
The wrapper should **not** become the main event router.

It should also **not** be the primary transport for raw injected notifications if native channels are available.

---

## 17. Session Lifecycle

### 17.1 Opening a session
1. Wrapper starts or resumes session `S`
2. Orchestrator registers session `S`
3. Orchestrator marks runtime as attached
4. Session begins interactive work

### 17.2 Adding watches
A session may add watches explicitly or through skills.

Example:

- after push, add a pipeline-chain watch
- if MR exists, add an MR watch
- if deploy is expected, add deploy-chain watch

### 17.3 Runtime detached
If the live process is closed:

- session `S` remains valid and resumable
- runtime attachment is marked detached
- watches remain associated with session `S`
- future relevant events are queued as pending deliveries for session `S`

### 17.4 Resume
When session `S` is resumed:

- runtime is reattached
- pending deliveries are replayed or summarized
- watches continue normally

---

## 18. Delivery Semantics

### 18.1 Live delivery
If a session has an attached live runtime and a matching watch:

- event becomes attention item
- attention item is delivered via orchestration channel

### 18.2 Queued delivery
If a session has no attached live runtime:

- event is still matched against the session’s watches
- event becomes attention item
- delivery is queued for that same session

### 18.3 Replay on resume
When the session resumes:

- queued deliveries may be replayed individually
- or summarized into a compact digest
- or both depending on policy

### 18.4 Fallback routing
If an event is critical and the session is not resumed within policy limits, the orchestrator may:

- route to a main session
- route to an ops session
- raise a mobile/UI alert
- mark as urgent backlog item

This is fallback, not the primary flow.

---

## 19. Critical Clarification: Session vs Runtime

This is one of the most important implementation statements.

### Incorrect model
- session closed means session gone
- watch orphaned because session died

### Correct model
- session remains
- runtime attachment disappears
- watch remains associated with the session identity
- delivery changes from live channel delivery to queued delivery

This distinction must be reflected in both the data model and the orchestrator logic.

---

## 20. Core User Flows

### 20.1 Bugfix push flow
1. User works in a bugfix session
2. User pushes code
3. Skill registers pipeline-chain watch
4. Optionally registers deploy-chain watch
5. GitLab event arrives
6. Orchestrator matches event to session watch
7. If runtime attached: deliver live
8. If runtime detached: queue for same session
9. On resume: replay or summarize

### 20.2 Merge request review flow
1. Review session is opened or resumed
2. MR watch is registered
3. New comments, approvals, and pipeline changes arrive
4. Orchestrator routes them to the review session
5. Session invokes GitLab MCP to inspect full details

### 20.3 Slack thread flow
1. Session starts working on a Slack request
2. Slack thread watch is registered
3. New messages in that thread arrive
4. Session receives them live or via resume backlog
5. Session can respond through Slack MCP

### 20.4 Target Process assignment flow
1. Poller detects assignment or update
2. Event enters orchestrator
3. Rules decide whether to route to an inbox session or backlog
4. Session hydrates via TP MCP if action is needed

### 20.5 Deploy monitoring flow
1. Deploy watch registered after pipeline or manual request
2. Jenkins events arrive
3. Orchestrator correlates to the tracked chain
4. Session receives updates only for the watched deploy chain

---

## 21. Edge Case: Session Closed After Watch Registration

### Problem
User opens a session, adds a watch, then accidentally closes the live session before the observed event chain completes.

### Required behavior
- the watch must not disappear
- the session must remain the owner of that watch
- events must continue to be matched to the same session
- live delivery must degrade into queued delivery
- on resume, the user must be able to continue in the same session history

### Example
1. User pushes code
2. Session adds pipeline watch
3. User closes runtime
4. Pipeline fails
5. Orchestrator creates attention item for the same session
6. Delivery is queued
7. User resumes session later
8. Session receives pipeline failure summary and can continue the investigation in the same op-log context

---

## 22. Optional Background Worker Sessions

### 22.1 Why background workers may be needed
If a watched flow becomes critical while the user session has no attached runtime, the orchestrator may optionally start a system-managed background worker.

### 22.2 Important rule
A background worker is **not** the same as the original user session.

It is a separate linked execution unit that may:

- inspect logs
- gather evidence
- prepare a summary
- suggest next actions

### 22.3 Linkage
The background worker should be linked to:

- original session id
- triggering watch id
- triggering event id
- owner

### 22.4 Background session classification
Suggested role:

- `system-managed`
- `background-investigation`

### 22.5 Principle
Background workers are optional fallback acceleration. They do not replace the resumable session model.

---

## 23. Routing Rules

### 23.1 Preferred route
The preferred route is always:

- match event to watches
- keep it attached to the same session identity
- deliver live if attached
- queue if detached

### 23.2 Fallback route
Fallback routing is allowed only when policy requires it, such as:

- critical production failure
- session has not resumed within allowed time
- immediate reaction required

### 23.3 Fallback targets
Suggested fallback targets:

- `main`
- `ops`
- `owner-default`
- `backlog-only`
- `ui-alert`

---

## 24. Replay Policy

When a session resumes, pending deliveries may be processed in one of three ways:

### 24.1 Full replay
Good for:

- a small number of important events
- review-oriented work
- debugging precise event order

### 24.2 Digest summary
Good for:

- many low-level status events
- noisy deployment chains
- multiple comments or messages

### 24.3 Hybrid replay
Recommended default:

- replay important events individually
- summarize low-value status noise

Example digest:

- 1 pipeline failed
- 1 Jenkins deploy failed
- 2 new MR comments
- 1 Slack mention

---

## 25. Source-Agnostic vs Source-Specific Logic

### 25.1 Source-agnostic logic belongs in orchestrator
- session registration
- runtime attachment tracking
- watch ownership
- event deduplication
- routing
- attention lifecycle
- pending delivery queue
- replay policy

### 25.2 Source-specific logic belongs in source MCPs
- Slack message hydration
- GitLab pipeline and MR inspection
- Jenkins build investigation
- Telegram follow-up reading
- TP task context retrieval

This boundary should remain explicit.

---

## 26. What Must Not Be Built Right Now

The following are intentionally out of scope for the first implementation:

- Kafka-based distributed event infrastructure
- full BPMN or enterprise workflow engine
- universal semantic inbox for all messages and entities
- complete cross-source knowledge graph
- full case management platform with mandatory global canonicalization

These may become future extensions, but they are not required to realize the current value.

---

## 27. Minimal Persistent Storage

A minimal first implementation can be built on SQLite.

Recommended tables:

### `sessions`
- session_id
- session_name
- owner
- role
- workspace
- project
- repository
- branch_hint
- status
- created_at
- last_seen_at
- metadata_json

### `runtime_attachments`
- runtime_id
- session_id
- channel_name
- attached
- started_at
- last_heartbeat_at
- runtime_metadata_json

### `source_checkpoints`
- source
- scope_key
- last_cursor
- last_seen_timestamp
- updated_at

### `events`
- event_id
- source
- event_kind
- source_ref
- thread_ref
- actor_ref
- title_hint
- importance_hint
- dedup_key
- correlation_key
- raw_payload_ref
- normalized_json
- created_at

### `watches`
- watch_id
- session_id
- watch_type
- entity_type
- entity_ref
- correlation_key
- delivery_policy
- fallback_policy
- status
- grace_until
- expires_at
- created_at
- updated_at
- metadata_json

### `attention_items`
- attention_id
- session_id
- event_id
- category
- importance
- requires_action
- state
- delivery_mode
- summary_hint
- reminder_at
- created_at
- updated_at

### `pending_deliveries`
- delivery_id
- session_id
- event_id
- attention_id
- delivery_state
- queued_at
- delivered_at
- replayed_at
- expired_at
- metadata_json

### `background_workers` (optional)
- worker_id
- linked_session_id
- trigger_watch_id
- trigger_event_id
- status
- created_at
- updated_at
- metadata_json

---

## 28. Matching and Correlation

### 28.1 Matching principle
Matching should be based primarily on entity and correlation keys, not only on event type.

### 28.2 Examples
- pipeline chain matched by commit SHA / branch / project
- deploy chain matched by environment + upstream pipeline + commit
- MR watch matched by MR id
- Slack thread watch matched by thread timestamp or thread id
- Telegram thread matched by chat id + topic/thread id
- TP item matched by item id

### 28.3 Why this matters
Watching “pipeline_failed” is weaker than watching “this branch / this commit / this pipeline chain”.

The watch should represent user interest in an entity or flow, not merely a raw event name.

---

## 29. Delivery Policies

Recommended delivery policies:

- `live-only`
- `live-or-queue`
- `queue-and-digest`
- `critical-fallback`

Recommended default:

- `live-or-queue`

Recommended critical default:

- `live-or-queue` plus `critical-fallback`

---

## 30. Expiration and Retention Policies

### 30.1 Watch expiration
Short-lived watches may expire automatically.

Examples:

- pipeline watch: 2 hours
- deploy watch: 2–6 hours
- MR watch: several days or explicit completion
- Slack thread watch: explicit close or inactivity timeout

### 30.2 Pending delivery retention
Pending deliveries should be retained long enough to survive normal resume patterns.

Suggested initial retention:

- 24 hours for routine items
- longer for critical incidents or active review sessions

---

## 31. Human Approval and Control

This system is designed to remain human-controlled.

The session remains the place where:

- actions are reviewed
- messages are approved
- replies are checked
- next steps are chosen

Reactive awareness does not remove the human approval layer.

---

## 32. Why This Architecture Fits the Existing Setup

This solution fits the current system because:

- it keeps the current MCP skill investments intact
- it preserves the session-centric workflow already working well
- it adds reactive awareness without forcing a total redesign
- it supports mobile/tablet/browser session shells naturally
- it does not require a heavy infrastructure stack
- it keeps source logic where it already belongs

---

## 33. Implementation Roadmap

### Phase 1 — Minimal orchestration core
- create SQLite schema
- implement session registration and runtime attachment
- implement event ingestion and source checkpoints
- implement watch registration
- implement matching and pending delivery queue
- expose minimal orchestration MCP

### Phase 2 — Session delivery
- launch sessions through wrapper with orchestration channel
- implement live delivery into session
- implement queued delivery and replay on resume
- implement session feed view in shell/UI

### Phase 3 — Source integration polish
- connect GitLab/Jenkins/Slack/Telegram/TP adapters
- define correlation keys per source
- refine delivery policies per watch type

### Phase 4 — Optional improvements
- digest generation on resume
- critical fallback routing
- optional background worker sessions
- richer mobile UI controls

---

## 34. Final Statements

The final architectural statements are:

1. **Cloud Code sessions remain the primary operational surface.**
2. **A session is a persisted operation log and resumable work context.**
3. **Runtime attachment is separate from session identity.**
4. **Watches belong to session identity, not only to a live runtime.**
5. **External events flow through a shared orchestrator pipeline.**
6. **Sessions subscribe to watches, not directly to raw external systems.**
7. **The orchestrator routes relevant events back to sessions.**
8. **If no runtime is attached, deliveries are queued for the same session.**
9. **Resume restores live delivery for the same session context.**
10. **Source MCPs remain source-native and are not replaced by a universal schema.**
11. **The system should unify attention and delivery control, not all source semantics.**
12. **A shell/UI wrapper supervises session lifecycle; channels deliver routed events.**
13. **Background worker sessions are optional fallbacks, not replacements for resumable user sessions.**
14. **This architecture is intentionally MVP-friendly and avoids premature platform overengineering.**

---

## 35. Concise Final Summary

This solution implements a **session-centric, event-driven orchestration model** for Cloud Code.

It preserves the existing agent-and-skill workflow, while adding a lightweight orchestration layer that:

- accepts events from external systems
- tracks session-owned watches
- routes relevant events to the correct session
- buffers deliveries when live runtime is detached
- allows the same session to continue later through resume

The design deliberately avoids overbuilding a generalized workflow engine and instead focuses on the minimum reliable architecture needed to make the current system reactive, durable, and manageable.
