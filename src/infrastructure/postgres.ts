import pg from "pg";
import type { Pool as PgPool } from "pg";
import type {
  ItemsRepository,
  EventsRepository,
} from "../application/ports.js";
import type { EventInput } from "../domain/models.js";
import type { StructuredLogger, Tracing } from "./contracts.js";
import { errorMessage } from "../domain/error-utils.js";
import { trace } from "@opentelemetry/api";
import { logError } from "./observability.js";

interface ItemRow {
  id: number;
  name: string;
  created_at: Date;
}
interface EventRow {
  event_id: string;
  name: string;
  received_at: Date;
  updated_at: Date | null;
}

const { Pool } = pg;

const ITEM_COLUMNS = "id, name, created_at";
const EVENT_COLUMNS = "event_id, name, received_at, updated_at";

const toItem = (row: ItemRow) => ({
  id: row.id,
  name: row.name,
  createdAt: row.created_at,
});
const toEvent = (row: EventRow) => ({
  eventId: row.event_id,
  name: row.name,
  receivedAt: row.received_at,
  updatedAt: row.updated_at,
});

// Registra db_write_ok / db_write_failed (con trace_id) dentro del span activo.
async function loggedWrite<T>(
  log: StructuredLogger,
  table: string,
  operation: string,
  fields: Record<string, unknown>,
  fn: () => Promise<T>,
): Promise<T> {
  trace
    .getActiveSpan()
    ?.setAttributes({ "db.sql.table": table, "db.operation": operation });
  try {
    const result = await fn();
    log("db_write_ok", { table, operation, ...fields });
    return result;
  } catch (err) {
    log(
      "db_write_failed",
      {
        table,
        operation,
        ...fields,
        error_type:
          err && typeof err === "object" && "code" in err
            ? err.code
            : err instanceof Error
              ? err.name
              : "UnknownError",
        error: errorMessage(err),
      },
      "ERROR",
    );
    throw err;
  }
}

export function createPool(databaseUrl: string) {
  const pool = new Pool({ connectionString: databaseUrl });
  pool.on("error", (err) => logError("pg_pool_error", err));
  return pool;
}

export async function ensureSchema(pool: PgPool) {
  await pool.query(
    "CREATE TABLE IF NOT EXISTS node_items (" +
      "id SERIAL PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
  );
  await pool.query(
    "CREATE TABLE IF NOT EXISTS node_events (" +
      "event_id UUID PRIMARY KEY, name TEXT NOT NULL, received_at TIMESTAMPTZ NOT NULL DEFAULT now())",
  );
  await pool.query(
    "ALTER TABLE node_events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ",
  );
}

export class PostgresItemsRepository implements ItemsRepository {
  constructor(
    private readonly pool: PgPool,
    private readonly tracing: Tracing,
    private readonly log: StructuredLogger,
  ) {}

  async create(name: string) {
    const { rows } = await this.tracing.withSpan(
      "node.db_insert",
      { "db.system": "postgresql" },
      () =>
        loggedWrite(this.log, "node_items", "INSERT", {}, () =>
          this.pool.query<ItemRow>(
            `INSERT INTO node_items (name) VALUES ($1) RETURNING ${ITEM_COLUMNS}`,
            [name],
          ),
        ),
    );
    const row = rows[0];
    if (!row) throw new Error("INSERT node_items returned no row");
    return toItem(row);
  }

  async listRecent(limit: number) {
    const { rows } = await this.tracing.withSpan(
      "node.db_select",
      { "db.system": "postgresql" },
      () =>
        this.pool.query<ItemRow>(
          `SELECT ${ITEM_COLUMNS} FROM node_items ORDER BY id DESC LIMIT $1`,
          [limit],
        ),
    );
    return rows.map(toItem);
  }
}

export class PostgresEventsRepository implements EventsRepository {
  constructor(
    private readonly pool: PgPool,
    private readonly tracing: Tracing,
    private readonly log: StructuredLogger,
  ) {}

  // `write` ({ operation, eventId }) activa el log de escritura para INSERT/UPDATE/DELETE.
  async #query(
    spanName: string,
    text: string,
    params: unknown[],
    write?: { operation: string; eventId: string },
  ) {
    return this.tracing.withSpan(
      spanName,
      { "db.system": "postgresql" },
      () => {
        const run = () => this.pool.query<EventRow>(text, params);
        return write
          ? loggedWrite(
              this.log,
              "node_events",
              write.operation,
              { event_id: write.eventId },
              run,
            )
          : run();
      },
    );
  }

  async create({ eventId, name }: EventInput) {
    const { rows } = await this.#query(
      "node.db_insert_event",
      `INSERT INTO node_events (event_id, name) VALUES ($1, $2) RETURNING ${EVENT_COLUMNS}`,
      [eventId, name],
      { operation: "INSERT", eventId },
    );
    const row = rows[0];
    if (!row) throw new Error("INSERT node_events returned no row");
    return toEvent(row);
  }

  async createIfAbsent({ eventId, name }: EventInput) {
    await this.#query(
      "node.db_insert_event",
      "INSERT INTO node_events (event_id, name) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING",
      [eventId, name],
      { operation: "INSERT", eventId },
    );
  }

  async get(eventId: string) {
    const { rows } = await this.#query(
      "node.db_select_event",
      `SELECT ${EVENT_COLUMNS} FROM node_events WHERE event_id = $1`,
      [eventId],
    );
    return rows[0] ? toEvent(rows[0]) : null;
  }

  async listRecent(limit: number) {
    const { rows } = await this.#query(
      "node.db_select_events",
      `SELECT ${EVENT_COLUMNS} FROM node_events ORDER BY received_at DESC LIMIT $1`,
      [limit],
    );
    return rows.map(toEvent);
  }

  async update(eventId: string, name: string) {
    const { rows } = await this.#query(
      "node.db_update_event",
      `UPDATE node_events SET name = $1, updated_at = now() WHERE event_id = $2 RETURNING ${EVENT_COLUMNS}`,
      [name, eventId],
      { operation: "UPDATE", eventId },
    );
    return rows[0] ? toEvent(rows[0]) : null;
  }

  async delete(eventId: string) {
    const { rowCount } = await this.#query(
      "node.db_delete_event",
      "DELETE FROM node_events WHERE event_id = $1",
      [eventId],
      { operation: "DELETE", eventId },
    );
    return (rowCount ?? 0) > 0;
  }
}
