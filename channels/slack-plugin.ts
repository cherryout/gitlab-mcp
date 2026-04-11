import type {
  ChannelPlugin,
  EventTypeDef,
  NotifyFn,
  ToolDef,
  ToolCallResult,
} from "../channel-plugin.js";
import Database from "better-sqlite3";
import { existsSync, mkdirSync, readFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { pino } from "pino";
import { spawn, ChildProcess } from "child_process";

// ─── Child MCP Client ─────────────────────────────────────────────────

class SlackMCPClient {
  private process: ChildProcess | null = null;
  private reqId = 0;
  private pending = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();
  private buffer = "";

  constructor(
    private command: string,
    private args: string[],
    private env: Record<string, string>,
    private logger: pino.Logger,
  ) {}

  async connect(): Promise<void> {
    this.process = spawn(this.command, this.args, {
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, ...this.env },
    });

    this.process.stdout!.on("data", (chunk: Buffer) => {
      this.buffer += chunk.toString();
      const lines = this.buffer.split("\n");
      this.buffer = lines.pop()!;
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const msg = JSON.parse(line);
          if (msg.id && this.pending.has(msg.id)) {
            this.pending.get(msg.id)!.resolve(msg);
            this.pending.delete(msg.id);
          }
        } catch {
          // skip non-JSON lines
        }
      }
    });

    this.process.stderr!.on("data", (chunk: Buffer) => {
      this.logger.debug({ stderr: chunk.toString().trim() }, "slack-mcp stderr");
    });

    this.process.on("exit", (code) => {
      this.logger.warn({ code }, "slack-mcp process exited");
      for (const [, p] of this.pending) {
        p.reject(new Error(`Process exited with code ${code}`));
      }
      this.pending.clear();
    });

    await this.request("initialize", {
      protocolVersion: "2024-11-05",
      capabilities: {},
      clientInfo: { name: "slack-channel-plugin", version: "1.0" },
    });
    await this.notify("notifications/initialized");
  }

  async close() {
    if (this.process) {
      this.process.kill();
      this.process = null;
    }
  }

  private async request(method: string, params?: Record<string, unknown>): Promise<unknown> {
    this.reqId++;
    const id = this.reqId;
    const msg: Record<string, unknown> = { jsonrpc: "2.0", id, method };
    if (params) msg.params = params;

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      const body = JSON.stringify(msg) + "\n";
      this.process!.stdin!.write(body);
      setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`Request ${method} timed out`));
        }
      }, 30000);
    });
  }

  private async notify(method: string, params?: Record<string, unknown>) {
    const msg: Record<string, unknown> = { jsonrpc: "2.0", method };
    if (params) msg.params = params;
    this.process!.stdin!.write(JSON.stringify(msg) + "\n");
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<string> {
    const resp = await this.request("tools/call", { name, arguments: args }) as {
      result?: { content?: Array<{ type: string; text: string }> };
      error?: { message: string };
    };
    if (resp.error) throw new Error(resp.error.message);
    const content = resp.result?.content || [];
    return content
      .filter((c) => c.type === "text")
      .map((c) => c.text)
      .join("\n");
  }
}

// ─── Types ────────────────────────────────────────────────────────────

interface Stmts {
  getMessage: Database.Statement;
  upsertMessage: Database.Statement;
  getChannel: Database.Statement;
  upsertChannel: Database.Statement;
  getChannels: Database.Statement;
  getLatestTs: Database.Statement;
}

// ─── Plugin ───────────────────────────────────────────────────────────

export class SlackChannelPlugin implements ChannelPlugin {
  readonly name = "slack";

  readonly eventTypes: EventTypeDef[] = [
    { name: "message_received", description: "New message in a watched channel or DM" },
    { name: "mention", description: "Someone mentioned you (@)" },
    { name: "thread_reply", description: "Reply in a thread you participate in" },
  ];

  readonly tools: ToolDef[] = [];

  private notify!: NotifyFn;
  private db!: Database.Database;
  private stmts!: Stmts;
  private logger!: pino.Logger;
  private client!: SlackMCPClient;
  private pollTimer?: ReturnType<typeof setTimeout>;
  private seeded = false;

  private pollIntervalMs!: number;
  private myUsername!: string;
  private aclChannels!: string[];
  private aclUsers!: string[];
  private mentionsOnlyChannels!: Set<string>;

  async init(notify: NotifyFn) {
    this.notify = notify;

    this.pollIntervalMs = parseInt(process.env.SLACK_CHANNEL_POLL_INTERVAL || "30000", 10);
    this.myUsername = process.env.SLACK_CHANNEL_MY_USERNAME || "oleksandr.denysenko";
    const aclPath = process.env.SLACK_MCP_ACL_FILE || join(homedir(), ".config", "slack-mcp", "acl.json");
    const acl = this.loadAcl(aclPath);
    this.aclChannels = acl.channels;
    this.aclUsers = acl.users;
    this.mentionsOnlyChannels = acl.mentionsOnly;

    const slackCommand = process.env.SLACK_MCP_AUTH_BIN || "/opt/homebrew/bin/slack-mcp-auth";
    const slackWorkspace = process.env.SLACK_CHANNEL_WORKSPACE || "iqoption";
    const slackServerBin = process.env.SLACK_MCP_SERVER_BIN || "/opt/homebrew/bin/slack-mcp-server";

    const dbDir = join(homedir(), ".cache", "slack-channel");
    mkdirSync(dbDir, { recursive: true });
    const dbPath = process.env.SLACK_CHANNEL_DB_PATH || join(dbDir, "state.db");
    const logPath = process.env.SLACK_CHANNEL_LOG_PATH || join(dbDir, "channel.log");
    const logLevel = process.env.SLACK_CHANNEL_LOG_LEVEL || "info";

    this.logger = pino(
      { level: logLevel, name: "slack" },
      pino.destination({ dest: logPath, sync: false }),
    );

    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        channel_id TEXT NOT NULL,
        channel_name TEXT NOT NULL,
        user_name TEXT NOT NULL,
        text TEXT NOT NULL,
        ts TEXT NOT NULL,
        thread_ts TEXT,
        seen_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        discovered_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_id, ts);
    `);

    this.stmts = {
      getMessage: this.db.prepare("SELECT * FROM messages WHERE id = ?"),
      upsertMessage: this.db.prepare(`
        INSERT INTO messages (id, channel_id, channel_name, user_name, text, ts, thread_ts, seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
      `),
      getChannel: this.db.prepare("SELECT * FROM channels WHERE id = ?"),
      upsertChannel: this.db.prepare(`
        INSERT INTO channels (id, name, type, discovered_at) VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET name=excluded.name
      `),
      getChannels: this.db.prepare("SELECT * FROM channels"),
      getLatestTs: this.db.prepare("SELECT MAX(ts) as latest_ts FROM messages WHERE channel_id = ?"),
    };

    this.client = new SlackMCPClient(
      slackCommand,
      ["mcp", slackWorkspace],
      { SLACK_MCP_SERVER_BIN: slackServerBin, SLACK_MCP_ATTACHMENT_TOOL: "true" },
      this.logger,
    );

    this.logger.info({
      pollInterval: this.pollIntervalMs,
      workspace: slackWorkspace,
      myUsername: this.myUsername,
      aclChannels: this.aclChannels,
      aclUsers: this.aclUsers,
      mentionsOnly: [...this.mentionsOnlyChannels],
      dbPath,
    }, "slack plugin initialized");
  }

  private loadAcl(path: string): { channels: string[]; users: string[]; mentionsOnly: Set<string> } {
    if (!existsSync(path)) return { channels: [], users: [], mentionsOnly: new Set() };
    try {
      const raw = JSON.parse(readFileSync(path, "utf-8"));
      const readList: string[] = raw.channels?.read || [];
      const channels = readList.filter((s: string) => s.startsWith("#")).map((s: string) => s.slice(1));
      const users = readList.filter((s: string) => s.startsWith("@")).map((s: string) => s.slice(1));
      const mentionsOnly = new Set<string>(
        (raw.channels?.mentions_only || []).map((s: string) => s.startsWith("#") ? s.slice(1) : s),
      );
      return { channels, users, mentionsOnly };
    } catch {
      return { channels: [], users: [], mentionsOnly: new Set() };
    }
  }

  async start() {
    this.logger.info("connecting to slack MCP server");
    await this.client.connect();
    this.logger.info("connected, discovering channels");
    await this.discoverChannels();
    this.logger.info("starting poll loop");
    this.schedulePoll();
  }

  async stop() {
    if (this.pollTimer) clearTimeout(this.pollTimer);
    await this.client.close();
    this.db.close();
    this.logger.info("stopped");
  }

  async handleToolCall(_name: string, _args: Record<string, unknown>): Promise<ToolCallResult | null> {
    return null;
  }

  // ─── Channel Discovery ──────────────────────────────────────────────

  private async discoverChannels() {
    try {
      const result = await this.client.callTool("channels_list", {
        channel_types: "public_channel,private_channel,im,mpim",
        limit: "500",
      });

      const now = Date.now();
      const lines = result.split("\n").filter((l) => l.trim());

      for (const line of lines) {
        const idMatch = line.match(/(?:^|\s)(C[A-Z0-9]+|D[A-Z0-9]+|G[A-Z0-9]+)/);
        const nameMatch = line.match(/#([\w-]+)/);
        if (!idMatch) continue;

        const channelId = idMatch[1];
        const channelName = nameMatch ? nameMatch[1] : channelId;
        const type = channelId.startsWith("D") ? "im" : channelId.startsWith("G") ? "mpim" : "channel";

        this.stmts.upsertChannel.run(channelId, channelName, type, now);
      }

      const channels = this.stmts.getChannels.all() as Array<{ id: string; name: string }>;
      this.logger.info({ count: channels.length }, "channels discovered");
    } catch (err) {
      this.logger.error({ err }, "channel discovery failed");
    }
  }

  // ─── Polling ────────────────────────────────────────────────────────

  private schedulePoll() {
    const delay = this.seeded ? this.pollIntervalMs : 0;
    this.pollTimer = setTimeout(async () => {
      await this.pollAllChannels();
      this.schedulePoll();
    }, delay);
  }

  private async pollAllChannels() {
    const channels = this.stmts.getChannels.all() as Array<{
      id: string; name: string; type: string;
    }>;

    if (channels.length === 0) return;

    const isFirstRun = !this.seeded;
    this.logger.debug({ channels: channels.length, isFirstRun }, "polling channels");

    const allowedNames = new Set([...this.aclChannels, ...this.aclUsers]);

    for (const channel of channels) {
      if (allowedNames.size > 0 && !allowedNames.has(channel.name)) {
        continue;
      }

      try {
        await this.pollChannel(channel, isFirstRun);
      } catch (err) {
        this.logger.error({ err, channel: channel.name }, "poll channel error");
      }
    }

    if (isFirstRun) {
      this.logger.info("channels seeded (first run, no events emitted)");
    }
    this.seeded = true;
  }

  private async pollChannel(
    channel: { id: string; name: string; type: string },
    isFirstRun: boolean,
  ) {
    const result = await this.client.callTool("conversations_history", {
      channel_id: channel.id,
      limit: isFirstRun ? "5" : "10",
    });

    const now = Date.now();
    const messages = this.parseMessages(result, channel);

    for (const msg of messages) {
      const existing = this.stmts.getMessage.get(msg.id) as { id: string } | undefined;
      if (existing) continue;

      this.stmts.upsertMessage.run(
        msg.id, channel.id, channel.name, msg.user,
        msg.text, msg.ts, msg.threadTs || null, now,
      );

      if (isFirstRun) continue;

      const eventType = this.classifyMessage(msg, channel);
      if (!eventType) continue;

      this.logger.info({
        eventType, channel: channel.name, user: msg.user, msgId: msg.id,
      }, "new message event");

      await this.notify({
        content: `[#${channel.name}] ${msg.user}: ${msg.text}`,
        meta: {
          event_type: eventType,
          channel_id: channel.id,
          channel_name: channel.name,
          user: msg.user,
          message_id: msg.id,
          ts: msg.ts,
          ...(msg.threadTs ? { thread_ts: msg.threadTs } : {}),
        },
      });
    }
  }

  private classifyMessage(
    msg: { user: string; text: string; ts: string; threadTs?: string },
    channel: { name: string; type: string },
  ): string | null {
    const isMention = this.isMentionedIn(msg.text);

    if (this.mentionsOnlyChannels.has(channel.name) && !isMention) return null;

    if (isMention) return "mention";
    if (msg.threadTs && msg.threadTs !== msg.ts) return "thread_reply";

    return "message_received";
  }

  private isMentionedIn(text: string): boolean {
    const lowerText = text.toLowerCase();
    const patterns = [
      this.myUsername.toLowerCase(),
      "oleksandr",
      "odenysenko",
    ];
    return patterns.some((p) => lowerText.includes(p));
  }

  private parseMessages(
    raw: string,
    channel: { id: string; name: string },
  ): Array<{ id: string; user: string; text: string; ts: string; threadTs?: string }> {
    const messages: Array<{ id: string; user: string; text: string; ts: string; threadTs?: string }> = [];

    const lines = raw.split("\n");
    let currentUser = "";
    let currentTs = "";
    let currentThreadTs = "";
    let currentText: string[] = [];

    for (const line of lines) {
      const headerMatch = line.match(/^(?:MsgID:\s*\S+\s+)?(\S+)\s+\((\d+\.\d+)\)(?:\s+thread:(\d+\.\d+))?/);
      if (headerMatch) {
        if (currentTs && currentUser) {
          messages.push({
            id: `${channel.id}:${currentTs}`,
            user: currentUser,
            text: currentText.join("\n").trim(),
            ts: currentTs,
            threadTs: currentThreadTs || undefined,
          });
        }
        currentUser = headerMatch[1];
        currentTs = headerMatch[2];
        currentThreadTs = headerMatch[3] || "";
        currentText = [];
        continue;
      }

      const simpleMatch = line.match(/^MsgID:\s*(\S+)/);
      if (simpleMatch && !headerMatch) {
        if (currentTs && currentUser) {
          messages.push({
            id: `${channel.id}:${currentTs}`,
            user: currentUser,
            text: currentText.join("\n").trim(),
            ts: currentTs,
            threadTs: currentThreadTs || undefined,
          });
        }
        currentUser = "";
        currentTs = simpleMatch[1];
        currentThreadTs = "";
        currentText = [];
        continue;
      }

      if (currentTs) {
        currentText.push(line);
      }
    }

    if (currentTs && currentUser) {
      messages.push({
        id: `${channel.id}:${currentTs}`,
        user: currentUser,
        text: currentText.join("\n").trim(),
        ts: currentTs,
        threadTs: currentThreadTs || undefined,
      });
    }

    return messages;
  }
}
