import Database from "better-sqlite3";
import path from "path";
import { createLogger } from "../utils/logger";

const logger = createLogger({ component: "job-store" });

export type JobStatus = "pending" | "active" | "completed" | "failed" | "dead-letter";
export type JobPriority = "low" | "normal" | "high" | "critical";

export const PRIORITY_WEIGHTS: Record<JobPriority, number> = {
  critical: 4,
  high: 3,
  normal: 2,
  low: 1,
};

export interface Job<T = any> {
  id: string;
  taskId: string;
  type: string;
  payload: T;
  status: JobStatus;
  priority: JobPriority;
  progress: number;
  attempts: number;
  maxAttempts: number;
  lastError?: string | null;
  nextRunAt: string;
  createdAt: string;
  updatedAt: string;
  completedAt?: string | null;
  failedAt?: string | null;
}

export interface JobStore {
  insert(job: Job): void;
  findById(id: string): Job | undefined;
  findByTaskId(taskId: string): Job | undefined;
  /**
   * Read-only inspection of the next runnable job. This does **not** claim it:
   * two callers can be handed the same row. Use {@link JobStore.claimNextPendingJob}
   * to take ownership of a job.
   */
  getNextPendingJob(nowIso?: string): Job | undefined;
  /**
   * Atomically claim the next runnable job and mark it `active`.
   *
   * The claim is a single conditional `UPDATE ... RETURNING *` executed inside
   * a `BEGIN IMMEDIATE` transaction, so two workers — or two processes sharing
   * the same SQLite file — can never be handed the same row. Returns
   * `undefined` when nothing is runnable.
   */
  claimNextPendingJob(nowIso?: string): Job | undefined;
  updateStatus(
    id: string,
    status: JobStatus,
    updates?: {
      lastError?: string | null;
      attempts?: number;
      nextRunAt?: string;
      progress?: number;
      completedAt?: string | null;
      failedAt?: string | null;
      expectedStatus?: JobStatus | JobStatus[];
    }
  ): boolean;
  updateProgress(id: string, progress: number): void;
  list(filter?: {
    status?: JobStatus;
    taskId?: string;
    page?: number;
    pageSize?: number;
  }): { jobs: Job[]; total: number };
  getStats(): {
    pending: number;
    active: number;
    completed: number;
    failed: number;
    deadLetter: number;
    total: number;
  };
  getDeadLetterJobs(page?: number, pageSize?: number): { jobs: Job[]; total: number };
  retryDeadLetterJob(id: string): boolean;
  recoverIncompleteJobs(): number;
  delete(id: string): boolean;
  clear(): void;
}

let _jobDb: Database.Database | null = null;

/**
 * How many times `claimNextPendingJob` retries when SQLite reports the database
 * is busy. `busy_timeout` (set in {@link getJobDb}) already makes SQLite wait
 * for the write lock; these retries cover the case where a second process
 * exhausts that window. After the last attempt the caller's poll loop takes
 * over, which is why the claim gives up instead of throwing.
 */
const CLAIM_BUSY_ATTEMPTS = 3;

/**
 * `SQLITE_BUSY` / `SQLITE_BUSY_*` (e.g. `SQLITE_BUSY_SNAPSHOT`) as reported by
 * better-sqlite3.
 */
function isBusyError(err: unknown): boolean {
  const code = (err as { code?: unknown } | null | undefined)?.code;
  return typeof code === "string" && code.startsWith("SQLITE_BUSY");
}

export function getJobDb(dbPath?: string): Database.Database {
  if (!_jobDb) {
    const filePath = dbPath ?? path.join(process.cwd(), "jobs.db");
    _jobDb = new Database(filePath);
    _jobDb.pragma("busy_timeout = 5000");
    _jobDb.pragma("journal_mode = WAL");
    try {
      (_jobDb as any).on("error", (err: Error) => {
        logger.error({ err }, "job database error");
      });
    } catch {
      // Node EventEmitter support
    }
    initJobSchema(_jobDb);
  }
  return _jobDb;
}

export async function closeJobDb(): Promise<void> {
  const db = _jobDb;
  if (!db) return;
  db.close();
  _jobDb = null;
}

export function initJobSchema(db: Database.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id           TEXT PRIMARY KEY,
      taskId       TEXT NOT NULL,
      type         TEXT NOT NULL,
      payloadJson  TEXT NOT NULL,
      status       TEXT NOT NULL DEFAULT 'pending',
      priority     TEXT NOT NULL DEFAULT 'normal',
      priorityNum  INTEGER NOT NULL DEFAULT 2,
      progress     INTEGER NOT NULL DEFAULT 0,
      attempts     INTEGER NOT NULL DEFAULT 0,
      maxAttempts  INTEGER NOT NULL DEFAULT 3,
      lastError    TEXT,
      nextRunAt    TEXT NOT NULL,
      createdAt    TEXT NOT NULL,
      updatedAt    TEXT NOT NULL,
      completedAt  TEXT,
      failedAt     TEXT
    );
    CREATE TABLE IF NOT EXISTS job_history (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      jobId      TEXT NOT NULL,
      status     TEXT NOT NULL,
      createdAt  TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_jobs_status_nextRun ON jobs (status, nextRunAt);
    CREATE INDEX IF NOT EXISTS idx_jobs_taskId ON jobs (taskId);
    CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs (priorityNum DESC, createdAt ASC);
  `);
}

function mapRowToJob(row: any): Job {
  let payload: any = {};
  try {
    payload = JSON.parse(row.payloadJson);
  } catch {
    payload = row.payloadJson;
  }

  return {
    id: row.id,
    taskId: row.taskId,
    type: row.type,
    payload,
    status: row.status as JobStatus,
    priority: row.priority as JobPriority,
    progress: Number(row.progress),
    attempts: Number(row.attempts),
    maxAttempts: Number(row.maxAttempts),
    lastError: row.lastError,
    nextRunAt: row.nextRunAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    completedAt: row.completedAt,
    failedAt: row.failedAt,
  };
}

export function createJobStore(db: Database.Database): JobStore {
  initJobSchema(db);

  // ---------------------------------------------------------------------------
  // Atomic claim (#647)
  //
  // The claim used to be a SELECT (`getNextPendingJob`) followed by a separate
  // UPDATE (`updateStatus`). Between those two statements another worker or
  // process could read the same row, and the handler would then run twice —
  // which is a money bug for payment-settling jobs.
  //
  // The statement below both chooses the next runnable row and flips it to
  // 'active' in one go. The trailing condition repeats the runnable predicate
  // so that a row which stopped being claimable in between is not claimed;
  // `RETURNING *` yields the row that was actually updated, and no row when the
  // predicate matched nothing. Running it via `.immediate()` opens the
  // transaction with BEGIN IMMEDIATE and takes the write lock up front, so
  // concurrent claimers serialise instead of racing.
  // ---------------------------------------------------------------------------
  const claimNextStmt = db.prepare(`
    UPDATE jobs
    SET status = 'active',
        updatedAt = @now
    WHERE id = (
      SELECT id FROM jobs
      WHERE (status = 'pending' OR (status = 'failed' AND attempts < maxAttempts))
        AND nextRunAt <= @now
      ORDER BY priorityNum DESC, createdAt ASC
      LIMIT 1
    )
      AND (status = 'pending' OR (status = 'failed' AND attempts < maxAttempts))
    RETURNING *
  `);
  const claimNextTx = db.transaction((nowIso: string) => {
    const row = claimNextStmt.get({ now: nowIso }) as any;
    return row ?? undefined;
  });

  return {
    insert(job: Job): void {
      const priorityNum = PRIORITY_WEIGHTS[job.priority] ?? 2;
      const stmt = db.prepare(`
        INSERT INTO jobs (
          id, taskId, type, payloadJson, status, priority, priorityNum,
          progress, attempts, maxAttempts, lastError, nextRunAt,
          createdAt, updatedAt, completedAt, failedAt
        ) VALUES (
          @id, @taskId, @type, @payloadJson, @status, @priority, @priorityNum,
          @progress, @attempts, @maxAttempts, @lastError, @nextRunAt,
          @createdAt, @updatedAt, @completedAt, @failedAt
        )
      `);

      stmt.run({
        id: job.id,
        taskId: job.taskId,
        type: job.type,
        payloadJson: JSON.stringify(job.payload),
        status: job.status,
        priority: job.priority,
        priorityNum,
        progress: job.progress ?? 0,
        attempts: job.attempts ?? 0,
        maxAttempts: job.maxAttempts ?? 3,
        lastError: job.lastError ?? null,
        nextRunAt: job.nextRunAt ?? new Date().toISOString(),
        createdAt: job.createdAt ?? new Date().toISOString(),
        updatedAt: job.updatedAt ?? new Date().toISOString(),
        completedAt: job.completedAt ?? null,
        failedAt: job.failedAt ?? null,
      });
    },

    findById(id: string): Job | undefined {
      const row = db.prepare("SELECT * FROM jobs WHERE id = ?").get(id);
      if (!row) return undefined;
      return mapRowToJob(row);
    },

    findByTaskId(taskId: string): Job | undefined {
      const row = db
        .prepare("SELECT * FROM jobs WHERE taskId = ? ORDER BY createdAt DESC LIMIT 1")
        .get(taskId);
      if (!row) return undefined;
      return mapRowToJob(row);
    },

    getNextPendingJob(nowIso?: string): Job | undefined {
      const now = nowIso ?? new Date().toISOString();
      // Pick next runnable job: status = 'pending' or (status = 'failed' AND attempts < maxAttempts) with nextRunAt <= now
      // Order by priority (highest first), then FIFO (oldest createdAt first)
      const row = db
        .prepare(`
          SELECT * FROM jobs
          WHERE (status = 'pending' OR (status = 'failed' AND attempts < maxAttempts))
            AND nextRunAt <= ?
          ORDER BY priorityNum DESC, createdAt ASC
          LIMIT 1
        `)
        .get(now);

      if (!row) return undefined;
      return mapRowToJob(row);
    },

    claimNextPendingJob(nowIso?: string): Job | undefined {
      const now = nowIso ?? new Date().toISOString();

      for (let attempt = 0; attempt < CLAIM_BUSY_ATTEMPTS; attempt++) {
        try {
          const row = claimNextTx.immediate(now);
          return row ? mapRowToJob(row) : undefined;
        } catch (err) {
          if (!isBusyError(err)) throw err;
          logger.warn(
            { attempt, err },
            "job claim hit a busy database; retrying the claim transaction"
          );
        }
      }

      // Hand the claim back to the caller's poll loop instead of throwing into
      // it: a busy database means "try again shortly", not "this cycle failed".
      logger.warn("job claim gave up on a busy database; will retry on the next poll");
      return undefined;
    },

    updateStatus(
      id: string,
      status: JobStatus,
      updates?: {
        lastError?: string | null;
        attempts?: number;
        nextRunAt?: string;
        progress?: number;
        completedAt?: string | null;
        failedAt?: string | null;
        expectedStatus?: JobStatus | JobStatus[];
      }
    ): boolean {
      const now = new Date().toISOString();
      const setClauses: string[] = ["status = ?", "updatedAt = ?"];
      const params: any[] = [status, now];

      if (updates) {
        if (updates.attempts !== undefined) {
          setClauses.push("attempts = ?");
          params.push(updates.attempts);
        }
        if (updates.progress !== undefined) {
          setClauses.push("progress = ?");
          params.push(updates.progress);
        }
        if (updates.lastError !== undefined) {
          setClauses.push("lastError = ?");
          params.push(updates.lastError);
        }
        if (updates.nextRunAt !== undefined) {
          setClauses.push("nextRunAt = ?");
          params.push(updates.nextRunAt);
        }
        if (updates.completedAt !== undefined) {
          setClauses.push("completedAt = ?");
          params.push(updates.completedAt);
        }
        if (updates.failedAt !== undefined) {
          setClauses.push("failedAt = ?");
          params.push(updates.failedAt);
        }
      }

      let whereClause = "WHERE id = ?";
      params.push(id);

      if (updates?.expectedStatus) {
        const expected = Array.isArray(updates.expectedStatus)
          ? updates.expectedStatus
          : [updates.expectedStatus];
        if (expected.length > 0) {
          const placeholders = expected.map(() => "?").join(", ");
          whereClause += ` AND status IN (${placeholders})`;
          params.push(...expected);
        }
      }

      const query = `UPDATE jobs SET ${setClauses.join(", ")} ${whereClause}`;

      const updateTx = db.transaction(() => {
        const info = db.prepare(query).run(...params);
        if (info.changes > 0) {
          db.prepare(`
            INSERT INTO job_history (jobId, status, createdAt)
            VALUES (?, ?, ?)
          `).run(id, status, now);
        }
        return info.changes > 0;
      });

      return updateTx();
    },

    updateProgress(id: string, progress: number): void {
      const clamped = Math.max(0, Math.min(100, Math.round(progress)));
      const now = new Date().toISOString();
      db.prepare("UPDATE jobs SET progress = ?, updatedAt = ? WHERE id = ?").run(clamped, now, id);
    },

    list(filter: {
      status?: JobStatus;
      taskId?: string;
      page?: number;
      pageSize?: number;
    } = {}): { jobs: Job[]; total: number } {
      const page = filter.page ?? 1;
      const pageSize = filter.pageSize ?? 50;
      const offset = (page - 1) * pageSize;

      const conditions: string[] = [];
      const params: any[] = [];

      if (filter.status) {
        conditions.push("status = ?");
        params.push(filter.status);
      }
      if (filter.taskId) {
        conditions.push("taskId = ?");
        params.push(filter.taskId);
      }

      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const rows = db
        .prepare(
          `SELECT * FROM jobs ${whereClause} ORDER BY priorityNum DESC, createdAt DESC LIMIT ? OFFSET ?`
        )
        .all(...params, pageSize, offset);

      const countResult = db
        .prepare(`SELECT COUNT(*) as count FROM jobs ${whereClause}`)
        .get(...params) as { count: number };

      return {
        jobs: rows.map(mapRowToJob),
        total: countResult?.count ?? 0,
      };
    },

    getStats() {
      const rows = db
        .prepare("SELECT status, COUNT(*) as count FROM jobs GROUP BY status")
        .all() as Array<{ status: string; count: number }>;

      const counts: Record<string, number> = {
        pending: 0,
        active: 0,
        completed: 0,
        failed: 0,
        "dead-letter": 0,
      };

      let total = 0;
      for (const row of rows) {
        counts[row.status] = Number(row.count);
        total += Number(row.count);
      }

      return {
        pending: counts["pending"] || 0,
        active: counts["active"] || 0,
        completed: counts["completed"] || 0,
        failed: counts["failed"] || 0,
        deadLetter: counts["dead-letter"] || 0,
        total,
      };
    },

    getDeadLetterJobs(page = 1, pageSize = 50): { jobs: Job[]; total: number } {
      return this.list({ status: "dead-letter", page, pageSize });
    },

    retryDeadLetterJob(id: string): boolean {
      const now = new Date().toISOString();
      const info = db
        .prepare(`
          UPDATE jobs
          SET status = 'pending',
              attempts = 0,
              lastError = NULL,
              nextRunAt = ?,
              failedAt = NULL,
              updatedAt = ?
          WHERE id = ? AND status = 'dead-letter'
        `)
        .run(now, now, id);

      return info.changes > 0;
    },

    recoverIncompleteJobs(): number {
      const now = new Date().toISOString();
      // On server restart, active jobs were interrupted — reset them to pending
      const info = db
        .prepare(`
          UPDATE jobs
          SET status = 'pending',
              nextRunAt = ?,
              updatedAt = ?
          WHERE status = 'active'
        `)
        .run(now, now);

      if (info.changes > 0) {
        logger.info({ count: info.changes }, "recovered incomplete active jobs to pending");
      }
      return info.changes;
    },

    delete(id: string): boolean {
      const info = db.prepare("DELETE FROM jobs WHERE id = ?").run(id);
      return info.changes > 0;
    },

    clear(): void {
      db.prepare("DELETE FROM jobs").run();
    },
  };
}
