import Database from "better-sqlite3";
import pino from "pino";
import { mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

import { isInMemoryPath, openDatabase, resolveDatabasePath } from "../index";
import {
  MigrationLoadError,
  loadMigrations,
  parseMigrationFile,
  resolveMigrationsDir,
} from "./loader";
import {
  MigrationChecksumError,
  MigrationFailedError,
  MigrationRunner,
  type MigrationResult,
} from "./runner";
import { parseCliArgs, runMigrateCli } from "./cli";

const silentLogger = pino({ enabled: false });

function makeRunner(migrations: string[], db = new Database(":memory:")) {
  const dir = mkdtempSync(join(tmpdir(), "ainet-migrations-"));
  for (const [index, sql] of migrations.entries()) {
    const n = String(index + 1).padStart(4, "0");
    writeFileSync(join(dir, `${n}_m${index + 1}.sql`), sql);
  }
  const runner = new MigrationRunner(db, loadMigrations(dir), { logger: silentLogger });
  return { runner, db, dir };
}

function tableNames(db: Database.Database): string[] {
  return (
    db
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
}

function appliedIds(db: Database.Database): string[] {
  return (
    db.prepare("SELECT id FROM schema_migrations ORDER BY id ASC").all() as Array<{ id: string }>
  ).map((row) => row.id);
}

describe("loadMigrations", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "ainet-load-"));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("throws when the directory does not exist", () => {
    expect(() => loadMigrations(join(dir, "nope"))).toThrow(MigrationLoadError);
  });

  it("rejects filenames that are not <version>_<name>.sql", () => {
    expect(() => parseMigrationFile("init.sql", "SELECT 1;")).toThrow(MigrationLoadError);
    expect(() => parseMigrationFile("_init.sql", "SELECT 1;")).toThrow(MigrationLoadError);
  });

  it("rejects a migration with an empty up section", () => {
    expect(() => parseMigrationFile("0001_empty.sql", "\n   \n")).toThrow(/empty up section/);
    expect(() =>
      parseMigrationFile("0001_header.sql", "-- 0001_header: notes only\n\n-- TODO: write the DDL\n"),
    ).toThrow(/empty up section/);
  });

  it("rejects duplicate versions across differently named files", () => {
    writeFileSync(join(dir, "0001_a.sql"), "CREATE TABLE IF NOT EXISTS a (id TEXT);");
    writeFileSync(join(dir, "0001_b.sql"), "CREATE TABLE IF NOT EXISTS b (id TEXT);");
    expect(() => loadMigrations(dir)).toThrow(/Duplicate migration version 0001/);
  });

  it("orders migrations numerically rather than lexicographically", () => {
    writeFileSync(join(dir, "0010_ten.sql"), "CREATE TABLE IF NOT EXISTS ten (id TEXT);");
    writeFileSync(join(dir, "0002_two.sql"), "CREATE TABLE IF NOT EXISTS two (id TEXT);");
    writeFileSync(join(dir, "0009_nine.sql"), "CREATE TABLE IF NOT EXISTS nine (id TEXT);");
    expect(loadMigrations(dir).map((m) => m.id)).toEqual(["0002", "0009", "0010"]);
  });

  it("ignores non-SQL files in the directory", () => {
    writeFileSync(join(dir, "0001_a.sql"), "CREATE TABLE IF NOT EXISTS a (id TEXT);");
    writeFileSync(join(dir, "README.md"), "not a migration");
    expect(loadMigrations(dir).map((m) => m.filename)).toEqual(["0001_a.sql"]);
  });
});

describe("parseMigrationFile", () => {
  const contents = [
    "-- 0001_init: create widgets",
    "CREATE TABLE IF NOT EXISTS widgets (id TEXT PRIMARY KEY);",
    "",
    "-- migrate:down",
    "DROP TABLE IF EXISTS widgets;",
    "",
  ].join("\n");

  it("splits the up and down sections and derives id/name/checksum", () => {
    const migration = parseMigrationFile("0001_init.sql", contents);

    expect(migration.id).toBe("0001");
    expect(migration.name).toBe("init");
    expect(migration.version).toBe(1);
    expect(migration.upSql).toBe(
      "-- 0001_init: create widgets\nCREATE TABLE IF NOT EXISTS widgets (id TEXT PRIMARY KEY);",
    );
    expect(migration.downSql).toBe("DROP TABLE IF EXISTS widgets;");
    expect(migration.checksum).toMatch(/^[0-9a-f]{64}$/);
  });

  it("treats a file without a down marker as forward-only", () => {
    const migration = parseMigrationFile("0002_more.sql", "CREATE TABLE IF NOT EXISTS more (id TEXT);");
    expect(migration.downSql).toBeNull();
  });

  it("produces a stable checksum across line endings", () => {
    const lf = parseMigrationFile("0001_init.sql", contents);
    const crlf = parseMigrationFile("0001_init.sql", contents.replace(/\n/g, "\r\n"));
    expect(crlf.checksum).toBe(lf.checksum);
  });
});

describe("MigrationRunner.up", () => {
  it("applies every pending migration in version order and records it", () => {
    const { runner, db } = makeRunner([
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);",
      "CREATE TABLE IF NOT EXISTS b (id TEXT PRIMARY KEY);",
      "CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY);",
    ]);

    const results = runner.up();

    expect(results.map((r) => [r.id, r.status])).toEqual([
      ["0001", "applied"],
      ["0002", "applied"],
      ["0003", "applied"],
    ]);
    expect(appliedIds(db)).toEqual(["0001", "0002", "0003"]);
    expect(tableNames(db)).toEqual(expect.arrayContaining(["a", "b", "c", "schema_migrations"]));
  });

  it("records id, filename, sql and appliedAt for each migration", () => {
    const { runner, db } = makeRunner(["CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);"]);
    runner.up();

    const row = db
      .prepare("SELECT id, filename, sql, applied_at FROM schema_migrations")
      .get() as { id: string; filename: string; sql: string; applied_at: string };

    expect(row.id).toBe("0001");
    expect(row.filename).toBe("0001_m1.sql");
    expect(row.sql).toContain("CREATE TABLE IF NOT EXISTS a");
    expect(Number.isNaN(Date.parse(row.applied_at))).toBe(false);
  });

  it("is a no-op when run twice", () => {
    const { runner, db } = makeRunner([
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);",
      "CREATE TABLE IF NOT EXISTS b (id TEXT PRIMARY KEY);",
    ]);

    expect(runner.up()).toHaveLength(2);
    const second = runner.up();

    expect(second).toEqual([]);
    expect(runner.currentVersion()).toBe("0002");
    expect(appliedIds(db)).toEqual(["0001", "0002"]);
  });

  it("preserves existing rows when re-run", () => {
    const { runner, db } = makeRunner([
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY, note TEXT);",
    ]);
    runner.up();
    db.prepare("INSERT INTO a (id, note) VALUES ('keep', 'me')").run();

    runner.up();

    expect(db.prepare("SELECT note FROM a WHERE id = 'keep'").get()).toEqual({ note: "me" });
  });

  it("adopts a legacy database that already has the tables", () => {
    const db = new Database(":memory:");
    db.exec("CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY, note TEXT);");
    db.prepare("INSERT INTO a (id, note) VALUES ('legacy', 'row')").run();

    const { runner } = makeRunner(
      [
        // Legacy inline DDL: same statements, already present.
        "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY, note TEXT);",
        "CREATE INDEX IF NOT EXISTS idx_a_note ON a (note);",
      ],
      db,
    );
    const results = runner.up();

    expect(results.map((r) => r.status)).toEqual(["applied", "applied"]);
    expect(db.prepare("SELECT note FROM a WHERE id = 'legacy'").get()).toEqual({ note: "row" });
  });

  it("rolls back the failing migration, records nothing and skips the rest", () => {
    const { runner, db } = makeRunner([
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);",
      "CREATE TABLE IF NOT EXISTS broken (id TEXT PRIMARY KEY); INSERT INTO missing_table VALUES (1);",
      "CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY);",
    ]);

    let thrown: unknown;
    let results: MigrationResult[] = [];
    try {
      results = runner.up();
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(MigrationFailedError);
    const failure = thrown as MigrationFailedError;
    expect(failure.failed.filename).toBe("0002_m2.sql");
    expect(failure.failed.error).toMatch(/missing_table/);
    // The failing migration's own DDL was rolled back, and the migration after
    // it was never attempted.
    expect(tableNames(db)).toEqual(expect.arrayContaining(["a", "schema_migrations"]));
    expect(tableNames(db)).not.toContain("broken");
    expect(tableNames(db)).not.toContain("c");
    expect(appliedIds(db)).toEqual(["0001"]);

    // The run is resumable: the successful migration is reported, the failure
    // is marked, and the untouched migration is not in the list at all.
    expect(results).toHaveLength(0);
    expect(failure.results.map((r) => [r.id, r.status])).toEqual([
      ["0001", "applied"],
      ["0002", "skipped"],
    ]);
  });

  it("can report a failure instead of throwing", () => {
    const { runner } = makeRunner(["THIS IS NOT SQL;"]);

    const results = runner.up({ throwOnError: false });

    expect(results.map((r) => r.status)).toEqual(["skipped"]);
    expect(results[0].error).toBeTruthy();
  });

  it("recovers once the broken migration is fixed", () => {
    const { runner, db, dir } = makeRunner([
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);",
      "THIS IS NOT SQL;",
      "CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY);",
    ]);
    expect(() => runner.up()).toThrow(MigrationFailedError);
    expect(appliedIds(db)).toEqual(["0001"]);

    // The failed migration was never recorded, so editing its file is safe.
    writeFileSync(join(dir, "0002_m2.sql"), "CREATE TABLE IF NOT EXISTS b (id TEXT PRIMARY KEY);");
    const reloaded = new MigrationRunner(db, loadMigrations(dir), { logger: silentLogger });

    expect(reloaded.up().map((r) => [r.id, r.status])).toEqual([
      ["0002", "applied"],
      ["0003", "applied"],
    ]);
    expect(appliedIds(db)).toEqual(["0001", "0002", "0003"]);
  });

  it("does not write anything during a dry run", () => {
    const { runner, db } = makeRunner(["CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);"]);

    const results = runner.up({ dryRun: true });

    expect(results[0].status).toBe("planned");
    expect(tableNames(db)).toEqual(["schema_migrations"]);
    expect(appliedIds(db)).toEqual([]);
  });

  it("refuses to run when an applied migration file was edited", () => {
    const { runner, db, dir } = makeRunner(["CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);"]);
    runner.up();

    writeFileSync(
      join(dir, "0001_m1.sql"),
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY, extra TEXT);",
    );
    // A fresh process reloads from disk, so the drift is visible.
    const reloaded = new MigrationRunner(db, loadMigrations(dir), { logger: silentLogger });

    expect(() => reloaded.up()).toThrow(MigrationChecksumError);
    expect(() => reloaded.up()).toThrow(/was modified after it was applied/);
  });

  it("refuses to run when an applied migration file is missing", () => {
    const { runner, db, dir } = makeRunner(["CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);"]);
    runner.up();

    rmSync(join(dir, "0001_m1.sql"));
    // A fresh process reloads from disk, so the file really is unknown.
    const reloaded = new MigrationRunner(db, loadMigrations(dir), { logger: silentLogger });

    expect(() => reloaded.up()).toThrow(/is recorded as applied but its file is missing/);
  });

  it("lists pending and applied migrations for a fresh database", () => {
    const { runner } = makeRunner([
      "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);",
      "CREATE TABLE IF NOT EXISTS b (id TEXT PRIMARY KEY);",
    ]);

    expect(runner.listApplied()).toEqual([]);
    expect(runner.currentVersion()).toBeNull();
    expect(runner.listPending().map((m) => m.id)).toEqual(["0001", "0002"]);

    runner.up();

    expect(runner.listPending()).toEqual([]);
    expect(runner.currentVersion()).toBe("0002");
    expect(runner.listApplied()).toHaveLength(2);
  });
});

describe("MigrationRunner.down", () => {
  const migrations = [
    "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);\n-- migrate:down\nDROP TABLE IF EXISTS a;",
    "CREATE TABLE IF NOT EXISTS b (id TEXT PRIMARY KEY);\n-- migrate:down\nDROP TABLE IF EXISTS b;",
  ];

  it("is refused unless down migrations are explicitly enabled", () => {
    const { runner } = makeRunner(migrations);
    runner.up();

    expect(() => runner.down()).toThrow(/--include-down-migrations/);
  });

  it("reverts newest first and clears the bookkeeping rows", () => {
    const db = new Database(":memory:");
    const dir = mkdtempSync(join(tmpdir(), "ainet-migrations-"));
    for (const [index, sql] of migrations.entries()) {
      const n = String(index + 1).padStart(4, "0");
      writeFileSync(join(dir, `${n}_m${index + 1}.sql`), sql);
    }
    const runner = new MigrationRunner(db, loadMigrations(dir), {
      logger: silentLogger,
      includeDownMigrations: true,
    });

    runner.up();
    expect(appliedIds(db)).toEqual(["0001", "0002"]);

    const results = runner.down();

    expect(results.map((r) => [r.id, r.status])).toEqual([
      ["0002", "reverted"],
      ["0001", "reverted"],
    ]);
    expect(tableNames(db)).toEqual(["schema_migrations"]);
    expect(appliedIds(db)).toEqual([]);
  });

  it("stops at --to and leaves earlier migrations applied", () => {
    const db = new Database(":memory:");
    const dir = mkdtempSync(join(tmpdir(), "ainet-migrations-"));
    for (const [index, sql] of migrations.entries()) {
      const n = String(index + 1).padStart(4, "0");
      writeFileSync(join(dir, `${n}_m${index + 1}.sql`), sql);
    }
    const runner = new MigrationRunner(db, loadMigrations(dir), {
      logger: silentLogger,
      includeDownMigrations: true,
    });
    runner.up();

    runner.down({ to: "0001" });

    expect(appliedIds(db)).toEqual(["0001"]);
    expect(tableNames(db)).toEqual(expect.arrayContaining(["a", "schema_migrations"]));
    expect(tableNames(db)).not.toContain("b");

    // Forward-only still works after a revert.
    expect(runner.up().map((r) => r.status)).toEqual(["applied"]);
    expect(appliedIds(db)).toEqual(["0001", "0002"]);
  });

  it("refuses to revert a migration that has no down section", () => {
    const dir = mkdtempSync(join(tmpdir(), "ainet-migrations-"));
    writeFileSync(join(dir, "0001_a.sql"), "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);");
    const runner = new MigrationRunner(new Database(":memory:"), loadMigrations(dir), {
      logger: silentLogger,
      includeDownMigrations: true,
    });

    runner.up();
    expect(() => runner.down()).toThrow(/no down section/);
  });
});

describe("shipped migration files", () => {
  const migrations = loadMigrations(resolveMigrationsDir());

  it("are discovered in order", () => {
    expect(migrations.map((m) => m.filename)).toEqual(["0001_init.sql", "0002_indexes.sql"]);
  });

  it("apply cleanly to a fresh empty database", () => {
    const db = new Database(":memory:");
    const runner = new MigrationRunner(db, migrations, { logger: silentLogger });

    const results = runner.up();

    expect(results.every((r) => r.status === "applied")).toBe(true);
    expect(tableNames(db)).toEqual(
      expect.arrayContaining([
        "agents",
        "jobs",
        "payments",
        "quality_scores",
        "schema_migrations",
        "task_events",
        "tasks",
      ]),
    );
    expect(runner.currentVersion()).toBe("0002");
  });

  it("capture the tasks, agents and payments columns the application reads", () => {
    const db = new Database(":memory:");
    new MigrationRunner(db, migrations, { logger: silentLogger }).up();

    const columns = (table: string) =>
      (db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>).map((c) => c.name);

    expect(columns("tasks")).toEqual([
      "id",
      "prompt",
      "walletPublicKey",
      "status",
      "dagJson",
      "createdAt",
      "updatedAt",
    ]);
    expect(columns("agents")).toEqual([
      "id",
      "capabilities",
      "pricingXLM",
      "endpoint",
      "stellarPublicKey",
      "reputationScore",
      "lastSeenAt",
      "status",
    ]);
    expect(columns("payments")).toEqual(
      expect.arrayContaining(["taskId", "nodeId", "balanceId", "status", "amountStroops", "txHash"]),
    );
  });

  it("support the existing payments write path and the stats aggregates", () => {
    const db = new Database(":memory:");
    new MigrationRunner(db, migrations, { logger: silentLogger }).up();

    db.prepare(
      "INSERT INTO payments (taskId, nodeId, balanceId, status, amountStroops, txHash) VALUES (?, ?, ?, ?, ?, ?)",
    ).run("t1", "n1", "b1", "released", "15000000", null);

    const row = db.prepare("SELECT amount, createdAt FROM payments").get() as {
      amount: number;
      createdAt: string;
    };
    expect(row.amount).toBe(15_000_000);
    expect(Number.isNaN(Date.parse(row.createdAt))).toBe(false);
    expect(
      db.prepare("SELECT COALESCE(SUM(amount), 0) AS total FROM payments WHERE status = 'released'").get(),
    ).toEqual({ total: 15_000_000 });
  });

  it("are idempotent when applied twice", () => {
    const db = new Database(":memory:");
    const runner = new MigrationRunner(db, migrations, { logger: silentLogger });

    runner.up();
    const before = tableNames(db);
    expect(runner.up()).toEqual([]);

    expect(tableNames(db)).toEqual(before);
    expect(appliedIds(db)).toEqual(["0001", "0002"]);
  });

  it("revert cleanly with down migrations enabled", () => {
    const db = new Database(":memory:");
    const runner = new MigrationRunner(db, migrations, {
      logger: silentLogger,
      includeDownMigrations: true,
    });
    runner.up();

    expect(runner.down().every((r) => r.status === "reverted")).toBe(true);
    // sqlite_sequence is SQLite's internal AUTOINCREMENT bookkeeping and is
    // left behind by the dropped AUTOINCREMENT tables.
    expect(tableNames(db)).toEqual(["schema_migrations", "sqlite_sequence"]);
  });
});

describe("resolveDatabasePath", () => {
  const original = { ...process.env };

  afterEach(() => {
    process.env = { ...original };
  });

  it("prefers the explicit override, then DB_PATH, then DATABASE_URL", () => {
    delete process.env.DATABASE_URL;
    process.env.DB_PATH = "./from-db-path.db";
    expect(resolveDatabasePath("./explicit.db")).toBe(join(process.cwd(), "explicit.db"));
    expect(resolveDatabasePath()).toBe(join(process.cwd(), "from-db-path.db"));

    delete process.env.DB_PATH;
    process.env.DATABASE_URL = "./from-database-url.db";
    expect(resolveDatabasePath()).toBe(join(process.cwd(), "from-database-url.db"));
  });

  it("falls back to ./data/ai-net.db", () => {
    delete process.env.DB_PATH;
    delete process.env.DATABASE_URL;
    expect(resolveDatabasePath()).toBe(join(process.cwd(), "data", "ai-net.db"));
  });

  it("strips a file: prefix and leaves memory URIs alone", () => {
    expect(resolveDatabasePath("file:./relative.db")).toBe(join(process.cwd(), "relative.db"));
    expect(resolveDatabasePath(":memory:")).toBe(":memory:");
    expect(resolveDatabasePath("file::memory:?cache=shared")).toBe("file::memory:?cache=shared");
    expect(isInMemoryPath("file::memory:?cache=shared")).toBe(true);
    expect(isInMemoryPath("./data/ai-net.db")).toBe(false);
  });

  it("rejects non-SQLite URLs instead of creating a bogus file", () => {
    expect(() => resolveDatabasePath("postgresql://user:pw@localhost:5432/ainet")).toThrow(
      /Unsupported database URL/,
    );
    expect(() => resolveDatabasePath("   ")).toThrow(/Database path is empty/);
  });
});

describe("openDatabase", () => {
  it("creates the parent directory and the database file", () => {
    const dir = mkdtempSync(join(tmpdir(), "ainet-db-"));
    const file = join(dir, "nested", "deeper", "ainet.db");

    const db = openDatabase(file);
    db.close();

    expect(existsSync(file)).toBe(true);
    rmSync(dir, { recursive: true, force: true });
  });

  it("applies the pragmas the application relies on", () => {
    const db = openDatabase(":memory:");
    expect((db.pragma("busy_timeout", { simple: true }) as number)).toBe(5000);
    db.close();
  });
});

describe("parseCliArgs", () => {
  it("defaults to an upward run with no extra flags", () => {
    expect(parseCliArgs([])).toEqual({
      status: false,
      dryRun: false,
      down: false,
      includeDownMigrations: false,
      help: false,
    });
  });

  it("parses the documented flags", () => {
    expect(parseCliArgs(["--include-down-migrations", "--down", "--to=0001"])).toMatchObject({
      down: true,
      includeDownMigrations: true,
      to: "0001",
    });
    expect(parseCliArgs(["--steps", "2"])).toMatchObject({ steps: 2 });
    expect(parseCliArgs(["--database=./x.db"])).toMatchObject({ database: "./x.db" });
    expect(parseCliArgs(["./positional.db"])).toMatchObject({ database: "./positional.db" });
    expect(parseCliArgs(["-h"]).help).toBe(true);
    // npm/ts-node forward a literal `--` separator; it must not be an error.
    expect(parseCliArgs(["--", "--down", "--to=0001"])).toMatchObject({ down: true, to: "0001" });
  });

  it("rejects malformed input", () => {
    expect(() => parseCliArgs(["--steps=abc"])).toThrow(/--steps expects/);
    expect(() => parseCliArgs(["--nope"])).toThrow(/Unknown option/);
    expect(() => parseCliArgs(["--to"])).toThrow(/--to expects a value/);
  });
});

describe("runMigrateCli", () => {
  let dir: string;
  let dbPath: string;
  let logSpy: jest.SpyInstance;
  let errorSpy: jest.SpyInstance;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "ainet-cli-"));
    dbPath = join(dir, "ainet.db");
    logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    logSpy.mockRestore();
    errorSpy.mockRestore();
    rmSync(dir, { recursive: true, force: true });
  });

  const output = () => logSpy.mock.calls.map((call) => call.join(" ")).join("\n");
  const errors = () => errorSpy.mock.calls.map((call) => call.join(" ")).join("\n");

  it("migrates a database file that does not exist yet, then reports up to date", () => {
    expect(existsSync(dbPath)).toBe(false);

    expect(runMigrateCli([`--database=${dbPath}`])).toBe(0);
    expect(existsSync(dbPath)).toBe(true);
    expect(output()).toContain("applied 0001_init.sql");
    expect(output()).toContain("applied 0002_indexes.sql");

    logSpy.mockClear();
    expect(runMigrateCli([`--database=${dbPath}`])).toBe(0);
    expect(output()).toContain("already up to date");
  });

  it("reports applied and pending migrations with --status", () => {
    runMigrateCli([`--database=${dbPath}`]);
    logSpy.mockClear();

    expect(runMigrateCli([`--database=${dbPath}`, "--status"])).toBe(0);
    expect(output()).toContain("version:  0002");
    expect(output()).toContain("applied 0001_init.sql");
    expect(output()).toContain("no pending migrations");
  });

  it("does not write during a dry run", () => {
    expect(runMigrateCli([`--database=${dbPath}`, "--dry-run"])).toBe(0);
    expect(output()).toContain("dry run");
    expect(output()).toContain("would apply 0001_init.sql");

    const db = new Database(dbPath, { readonly: true });
    expect(tableNames(db)).toEqual(["schema_migrations"]);
    db.close();
  });

  it("refuses --down without --include-down-migrations", () => {
    runMigrateCli([`--database=${dbPath}`]);
    logSpy.mockClear();

    expect(runMigrateCli([`--database=${dbPath}`, "--down"])).toBe(1);
    expect(errors()).toContain("--down requires --include-down-migrations");
  });

  it("reverts with --include-down-migrations --down", () => {
    runMigrateCli([`--database=${dbPath}`]);
    logSpy.mockClear();

    expect(runMigrateCli([`--database=${dbPath}`, "--include-down-migrations", "--down", "--to=0001"])).toBe(0);
    expect(output()).toContain("reverted 0002_indexes.sql");

    const db = new Database(dbPath);
    expect(appliedIds(db)).toEqual(["0001"]);
    db.close();
  });

  it("lists reversible migrations when --include-down-migrations is used with --status", () => {
    runMigrateCli([`--database=${dbPath}`]);
    logSpy.mockClear();

    expect(runMigrateCli([`--database=${dbPath}`, "--status", "--include-down-migrations"])).toBe(0);
    expect(output()).toContain("down migrations enabled — reversible: 0001, 0002");
  });

  it("fails with a clear message when the database cannot be opened", () => {
    expect(runMigrateCli(["--database=postgresql://user:pw@localhost:5432/ainet"])).toBe(1);
    expect(errors()).toContain("Unsupported database URL");
  });

  it("exits 1 and names the rolled-back migration when a migration fails", () => {
    // DB_MIGRATIONS_DIR overrides discovery, so the shipped set is untouched.
    const migrationsDir = mkdtempSync(join(tmpdir(), "ainet-cli-bad-"));
    writeFileSync(join(migrationsDir, "0001_ok.sql"), "CREATE TABLE IF NOT EXISTS a (id TEXT PRIMARY KEY);");
    writeFileSync(join(migrationsDir, "0002_broken.sql"), "INSERT INTO missing_table VALUES (1);");
    writeFileSync(join(migrationsDir, "0003_after.sql"), "CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY);");
    process.env.DB_MIGRATIONS_DIR = migrationsDir;

    try {
      expect(runMigrateCli([`--database=${dbPath}`])).toBe(1);
      expect(output()).toContain("applied 0001_ok.sql");
      expect(errors()).toContain("0002_broken.sql");
      // 0002 was rolled back and 0003 was never attempted, so both remain pending.
      expect(errors()).toContain("2 migration(s) were not applied");

      const db = new Database(dbPath);
      expect(appliedIds(db)).toEqual(["0001"]);
      expect(tableNames(db)).not.toContain("c");
      db.close();
    } finally {
      delete process.env.DB_MIGRATIONS_DIR;
      rmSync(migrationsDir, { recursive: true, force: true });
    }
  });

  it("prints usage for --help", () => {
    expect(runMigrateCli(["--help"])).toBe(0);
    expect(output()).toContain("Usage: npm run db:migrate");
  });
});

describe("npm script", () => {
  const pkg = JSON.parse(readFileSync(join(__dirname, "..", "..", "..", "package.json"), "utf8"));

  it("exposes db:migrate", () => {
    expect(pkg.scripts["db:migrate"]).toBe("ts-node src/db/cli.ts migrate");
  });
});
