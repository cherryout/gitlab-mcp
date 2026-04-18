import type {
  ChannelPlugin,
  EventTypeDef,
  NotifyFn,
  OnWatchRegisteredFn,
  ToolDef,
  ToolCallResult,
} from "../channel-plugin.js";
import Database from "better-sqlite3";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import nodeFetch from "node-fetch";
import { pino } from "pino";

interface JenkinsBuild {
  number: number;
  result: string | null;
  building: boolean;
  timestamp: number;
  duration: number;
  url: string;
  displayName?: string;
}

interface JenkinsJob {
  name: string;
  url: string;
  jobs?: JenkinsJob[];
  builds?: JenkinsBuild[];
  lastBuild?: JenkinsBuild | null;
}

interface Stmts {
  getBuild: Database.Statement;
  upsertBuild: Database.Statement;
  getJob: Database.Statement;
  upsertJob: Database.Statement;
  getJobs: Database.Statement;
}

export class JenkinsChannelPlugin implements ChannelPlugin {
  readonly name = "jenkins";

  readonly eventTypes: EventTypeDef[] = [
    { name: "build_started", description: "New build started on a watched job" },
    { name: "build_completed", description: "Build finished (SUCCESS, FAILURE, UNSTABLE, ABORTED)" },
  ];

  readonly tools: ToolDef[] = [
    {
      name: "jenkins_watch_job",
      description: "Watch a Jenkins job. Routes build_started and build_completed events for this job to the session. The next completed build is typically your deploy. Use mcp__jenkins-int__search_jobs or get_all_jobs to find job paths.",
      inputSchema: {
        type: "object",
        properties: {
          job_path: { type: "string", description: "Job path (e.g. 'SDK/deploy_sdk_int', 'Processing/deploy_processing_int')" },
          expires_in_hours: { type: "number", description: "Expiration in hours (default: 2)" },
        },
        required: ["job_path"],
      },
    },
  ];

  private notify!: NotifyFn;
  private onWatchRegistered?: OnWatchRegisteredFn;
  private db!: Database.Database;
  private stmts!: Stmts;
  private logger!: pino.Logger;
  private pollTimer?: ReturnType<typeof setTimeout>;
  private seeded = false;

  private baseUrl!: string;
  private authHeader!: string;
  private watchJobs!: string[];
  private pollIntervalMs!: number;

  async init(notify: NotifyFn) {
    this.notify = notify;

    this.baseUrl = (process.env.JENKINS_CHANNEL_URL || "").replace(/\/+$/, "");
    const username = process.env.JENKINS_CHANNEL_USERNAME || "";
    const token = process.env.JENKINS_CHANNEL_TOKEN || "";
    this.authHeader = "Basic " + Buffer.from(`${username}:${token}`).toString("base64");
    this.watchJobs = (process.env.JENKINS_CHANNEL_JOBS || "")
      .split(",").map((s) => s.trim()).filter(Boolean);
    this.pollIntervalMs = parseInt(process.env.JENKINS_CHANNEL_POLL_INTERVAL || "30000", 10);

    const dbDir = join(homedir(), ".cache", "jenkins-channel");
    mkdirSync(dbDir, { recursive: true });
    const dbPath = process.env.JENKINS_CHANNEL_DB_PATH || join(dbDir, "state.db");
    const logPath = process.env.JENKINS_CHANNEL_LOG_PATH || join(dbDir, "channel.log");
    const logLevel = process.env.JENKINS_CHANNEL_LOG_LEVEL || "info";

    this.logger = pino(
      { level: logLevel, name: "jenkins" },
      pino.destination({ dest: logPath, sync: false }),
    );

    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS builds (
        job_path TEXT NOT NULL,
        build_number INTEGER NOT NULL,
        building INTEGER NOT NULL,
        result TEXT,
        url TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        duration INTEGER NOT NULL,
        seen_at INTEGER NOT NULL,
        PRIMARY KEY (job_path, build_number)
      );
      CREATE TABLE IF NOT EXISTS jobs (
        path TEXT PRIMARY KEY,
        url TEXT NOT NULL,
        discovered_at INTEGER NOT NULL
      );
    `);

    this.stmts = {
      getBuild: this.db.prepare("SELECT * FROM builds WHERE job_path = ? AND build_number = ?"),
      upsertBuild: this.db.prepare(`
        INSERT INTO builds (job_path, build_number, building, result, url, timestamp, duration, seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_path, build_number) DO UPDATE SET
          building=excluded.building, result=excluded.result,
          duration=excluded.duration, seen_at=excluded.seen_at
      `),
      getJob: this.db.prepare("SELECT * FROM jobs WHERE path = ?"),
      upsertJob: this.db.prepare(`
        INSERT INTO jobs (path, url, discovered_at) VALUES (?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET url=excluded.url
      `),
      getJobs: this.db.prepare("SELECT * FROM jobs"),
    };

    this.logger.info({
      baseUrl: this.baseUrl,
      username,
      watchJobs: this.watchJobs,
      pollInterval: this.pollIntervalMs,
      dbPath,
    }, "jenkins plugin initialized");
  }

  setOnWatchRegistered(fn: OnWatchRegisteredFn): void {
    this.onWatchRegistered = fn;
  }

  async start() {
    this.logger.info("starting poll loop");
    this.schedulePoll();
  }

  async stop() {
    if (this.pollTimer) clearTimeout(this.pollTimer);
    this.db.close();
    this.logger.info("stopped");
  }

  async handleToolCall(name: string, args: Record<string, unknown>): Promise<ToolCallResult | null> {
    if (name === "jenkins_watch_job") {
      const jobPath = args.job_path as string;
      const expiresInHours = (args.expires_in_hours as number) ?? 2;
      if (!this.onWatchRegistered) {
        return { content: [{ type: "text", text: "Error: orchestrator callback not configured" }] };
      }
      if (!this.watchJobs.includes(jobPath)) {
        this.logger.warn({ jobPath, configured: this.watchJobs }, "watching unconfigured job (plugin won't poll it)");
      }
      this.onWatchRegistered({
        watch_type: "deploy-chain",
        entity_type: "build",
        entity_ref: `jenkins:${jobPath}`,
        correlation_key: `jenkins:${jobPath}`,
        expires_at: Date.now() + expiresInHours * 60 * 60 * 1000,
      });
      this.logger.info({ jobPath, expiresInHours }, "jenkins job watch registered");
      const warning = this.watchJobs.includes(jobPath)
        ? ""
        : " (note: this job is not in JENKINS_CHANNEL_JOBS — plugin won't emit events for it until configured)";
      return { content: [{ type: "text", text: `Watching Jenkins job "${jobPath}". Next build_completed event routes here.${warning}` }] };
    }
    return null;
  }

  // ─── Jenkins API ────────────────────────────────────────────────────

  private async jenkinsGet(path: string): Promise<unknown> {
    const url = `${this.baseUrl}${path}`;
    const res = await nodeFetch(url, {
      headers: {
        Authorization: this.authHeader,
        Accept: "application/json",
      },
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Jenkins API ${res.status}: ${body.slice(0, 200)}`);
    }
    return res.json();
  }

  // ─── Polling ────────────────────────────────────────────────────────

  private schedulePoll() {
    const delay = this.seeded ? this.pollIntervalMs : 0;
    this.pollTimer = setTimeout(async () => {
      await this.pollJobs();
      this.schedulePoll();
    }, delay);
  }

  private async pollJobs() {
    const isFirstRun = !this.seeded;
    this.logger.debug({ isFirstRun, jobs: this.watchJobs.length }, "polling jobs");

    for (const jobPath of this.watchJobs) {
      try {
        await this.pollJob(jobPath, isFirstRun);
      } catch (err) {
        this.logger.error({ err, job: jobPath }, "poll job error");
      }
    }

    if (isFirstRun) {
      this.logger.info({ jobs: this.watchJobs.length }, "jobs seeded (first run)");
    }
    this.seeded = true;
  }

  private async pollJob(jobPath: string, isFirstRun: boolean) {
    const apiPath = this.jobApiPath(jobPath);
    const data = await this.jenkinsGet(
      `${apiPath}/api/json?tree=name,url,builds[number,result,building,timestamp,duration,url,displayName]{0,5}`,
    ) as JenkinsJob;

    const now = Date.now();
    this.stmts.upsertJob.run(jobPath, data.url || "", now);

    const builds = data.builds || [];
    this.logger.debug({ job: jobPath, builds: builds.length }, "builds fetched");

    for (const build of builds) {
      const existing = this.stmts.getBuild.get(jobPath, build.number) as {
        building: number; result: string | null;
      } | undefined;

      const wasBuilding = existing?.building === 1;
      const oldResult = existing?.result || null;

      this.stmts.upsertBuild.run(
        jobPath, build.number, build.building ? 1 : 0,
        build.result || null, build.url, build.timestamp,
        build.duration, now,
      );

      if (isFirstRun) continue;

      if (!existing && build.building) {
        this.logger.info({ job: jobPath, build: build.number }, "build_started event");
        await this.notify({
          content: `Build #${build.number} started on "${jobPath}"`,
          meta: {
            event_type: "build_started",
            job: jobPath,
            build_number: String(build.number),
            url: build.url,
          },
          orchestration: {
            source: "jenkins",
            event_kind: "build_started",
            entity_type: "build",
            entity_ref: `jenkins:${jobPath}:build:${build.number}`,
            correlation_key: `jenkins:${jobPath}`,
            dedup_key: `jenkins:${jobPath}:${build.number}:started`,
            importance_hint: "low",
            source_ref: build.url,
            title_hint: `Build #${build.number} started on "${jobPath}"`,
          },
        });
      }

      if (build.result && build.result !== oldResult) {
        const emoji = build.result === "SUCCESS" ? "✅"
          : build.result === "FAILURE" ? "❌"
          : build.result === "UNSTABLE" ? "⚠️"
          : "⛔";

        this.logger.info({
          job: jobPath, build: build.number, result: build.result,
          wasBuilding,
        }, "build_completed event");

        await this.notify({
          content: `${emoji} Build #${build.number} on "${jobPath}": ${build.result} (${this.formatDuration(build.duration)})`,
          meta: {
            event_type: "build_completed",
            job: jobPath,
            build_number: String(build.number),
            result: build.result,
            duration: String(build.duration),
            url: build.url,
          },
          orchestration: {
            source: "jenkins",
            event_kind: "build_completed",
            entity_type: "build",
            entity_ref: `jenkins:${jobPath}:build:${build.number}`,
            correlation_key: `jenkins:${jobPath}`,
            dedup_key: `jenkins:${jobPath}:${build.number}:${build.result}`,
            importance_hint: build.result === "FAILURE" ? "high" : "normal",
            source_ref: build.url,
            title_hint: `Build #${build.number} on "${jobPath}": ${build.result}`,
          },
        });
      }
    }
  }

  private jobApiPath(jobPath: string): string {
    return "/" + jobPath.split("/").map((p) => `job/${encodeURIComponent(p)}`).join("/");
  }

  private formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`;
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const remaining = seconds % 60;
    return remaining > 0 ? `${minutes}m${remaining}s` : `${minutes}m`;
  }
}
