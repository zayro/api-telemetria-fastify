import { EventNotFoundError } from "../../domain/errors.js";
import type {
  FastifyInstance,
  FastifyRequest,
  FastifyReply,
  RouteGenericInterface,
} from "fastify";
import type { Item, StoredEvent } from "../../domain/models.js";
import type { UseCases } from "../../application/use-cases.js";
import type {
  HttpMetrics,
  Tracing,
  StructuredLogger,
} from "../../infrastructure/contracts.js";
import { errorMessage } from "../../domain/error-utils.js";

interface RouteDependencies {
  serviceName: string;
  useCases: UseCases;
  metrics: HttpMetrics;
  tracing: Pick<Tracing, "currentTraceId" | "withSpan">;
  log: StructuredLogger;
  checkReadiness: () => Promise<void>;
}
interface NameBody {
  Body: { name: string };
}
interface EventParams {
  Params: { event_id: string };
}

const MAX_DELAY_MS = 2000;
const DEFAULT_DELAY_MS = 100;

const nameBody = {
  type: "object",
  required: ["name"],
  properties: { name: { type: "string", minLength: 1, maxLength: 100 } },
};
const eventBody = { ...nameBody, additionalProperties: false };
const eventParams = {
  type: "object",
  required: ["event_id"],
  properties: { event_id: { type: "string", format: "uuid" } },
};

const serializeItem = (
  item: Item,
): { id: number; name: string; created_at: Date; created_by?: string } => ({
  id: item.id,
  name: item.name,
  created_at: item.createdAt,
});
const serializeEvent = (event: StoredEvent) => ({
  event_id: event.eventId,
  name: event.name,
  received_at: event.receivedAt,
  updated_at: event.updatedAt,
});

export function toHttpStatus(err: unknown): number {
  if (err instanceof EventNotFoundError) return 404;
  return err &&
    typeof err === "object" &&
    "statusCode" in err &&
    typeof err.statusCode === "number"
    ? err.statusCode
    : 500;
}

function parseDelay(value: string | undefined) {
  const parsed = Number.parseInt(value ?? String(DEFAULT_DELAY_MS), 10);
  return Number.isNaN(parsed)
    ? DEFAULT_DELAY_MS
    : Math.max(0, Math.min(parsed, MAX_DELAY_MS));
}

function parseRoles(raw: string | string[] | undefined): string[] {
  if (!raw) return [];
  try {
    const parsed: unknown =
      typeof raw === "string" && raw.startsWith("[") ? JSON.parse(raw) : raw;
    return (Array.isArray(parsed) ? parsed : [parsed]).filter(
      (role): role is string => typeof role === "string",
    );
  } catch {
    return typeof raw === "string" ? [raw] : raw;
  }
}

function getAuth(request: FastifyRequest) {
  const header = (name: string) => {
    const value = request.headers[name];
    return (Array.isArray(value) ? value[0] : value) || null;
  };
  const user = header("x-auth-user");
  const email = header("x-auth-email");
  const roles = parseRoles(request.headers["x-auth-roles"]);
  return { user, email, roles };
}

export function registerRoutes(
  app: FastifyInstance,
  {
    serviceName,
    useCases,
    metrics,
    tracing,
    log,
    checkReadiness,
  }: RouteDependencies,
) {
  app.setErrorHandler((err, _request, reply) => {
    if (err instanceof EventNotFoundError) {
      return reply.code(404).send({ ok: false, error: err.message });
    }
    return reply.send(err);
  });

  // Registra metricas con el status real, incluidos los errores mapeados por toHttpStatus.
  const tracked =
    <T extends RouteGenericInterface = RouteGenericInterface>(
      route: string,
      handler: (request: FastifyRequest<T>, reply: FastifyReply) => unknown,
    ) =>
    async (request: FastifyRequest<T>, reply: FastifyReply) => {
      const done = metrics.track(route);
      try {
        const result = await handler(request, reply);
        done(reply.statusCode);
        return result;
      } catch (err) {
        done(toHttpStatus(err));
        throw err;
      }
    };

  app.get(
    "/api/node/public",
    tracked("/api/node/public", (request) => {
      const auth = getAuth(request);
      log("node_public_access", { authenticated: Boolean(auth.user) });
      return {
        ok: true,
        service: serviceName,
        message: "Este endpoint es publico. No requiere autenticacion JWT.",
        auth,
      };
    }),
  );

  app.get(
    "/api/node",
    tracked<{ Querystring: { delay_ms?: string; fail?: string } }>(
      "/api/node",
      (request, reply) => {
        const delayMs = parseDelay(request.query.delay_ms);
        const forceFail = request.query.fail === "1";
        const auth = getAuth(request);

        const spanAttributes = {
          "app.delay_ms": delayMs,
          ...(auth.user ? { "enduser.id": auth.user } : {}),
          ...(auth.email ? { "enduser.email": auth.email } : {}),
          ...(auth.roles.length > 0
            ? { "enduser.role": auth.roles.join(",") }
            : {}),
        };

        return tracing.withSpan(
          "node.business_logic",
          spanAttributes,
          async () => {
            await useCases.simulate(delayMs);

            if (forceFail) {
              log("node_simulated_error", { user: auth.user });
              return reply.code(500).send({
                ok: false,
                service: serviceName,
                trace_id: tracing.currentTraceId(),
                error: "Error simulado para validar observabilidad.",
                auth,
              });
            }

            log("node_ok", {
              delay_ms: delayMs,
              user: auth.user,
              email: auth.email,
              roles: auth.roles,
            });
            return {
              ok: true,
              service: serviceName,
              trace_id: tracing.currentTraceId(),
              message: "pong",
              auth,
            };
          },
        );
      },
    ),
  );

  app.post(
    "/api/node/items",
    { schema: { body: nameBody } },
    tracked<NameBody>("/api/node/items", async (request, reply) => {
      const auth = getAuth(request);
      const item = await useCases.createItem(request.body.name);
      log("item_created", {
        item_id: item.id,
        user: auth.user,
        roles: auth.roles,
      });
      const serialized = serializeItem(item);
      if (auth.user) {
        serialized.created_by = auth.user;
      }
      return reply.code(201).send(serialized);
    }),
  );

  app.get(
    "/api/node/items",
    tracked("/api/node/items", async () => {
      const { source, items } = await useCases.listItems();
      if (source === "cache") log("items_cache_hit");
      else log("items_cache_miss", { count: items.length });
      return { ok: true, source, items: items.map(serializeItem) };
    }),
  );

  app.post(
    "/api/node/events",
    { schema: { body: eventBody } },
    tracked<NameBody>("/api/node/events", async (request, reply) => {
      const auth = getAuth(request);
      const event = await useCases.createEvent(request.body.name);
      log("event_created", {
        event_id: event.eventId,
        user: auth.user,
        roles: auth.roles,
      });
      return reply.code(201).send({
        ok: true,
        event: serializeEvent(event),
        ...(auth.user ? { published_by: auth.user } : {}),
      });
    }),
  );

  app.get(
    "/api/node/events",
    tracked("/api/node/events", async () => {
      const events = await useCases.listEvents();
      return { ok: true, events: events.map(serializeEvent) };
    }),
  );

  const eventRoute = "/api/node/events/:event_id";

  app.get(
    eventRoute,
    { schema: { params: eventParams } },
    tracked<EventParams>(eventRoute, async (request) => {
      const event = await useCases.getEvent(request.params.event_id);
      return { ok: true, event: serializeEvent(event) };
    }),
  );

  app.put(
    eventRoute,
    { schema: { params: eventParams, body: eventBody } },
    tracked<EventParams & NameBody>(eventRoute, async (request) => {
      const { event_id: eventId } = request.params;
      const event = await useCases.updateEvent(eventId, request.body.name);
      log("event_updated", { event_id: eventId });
      return { ok: true, event: serializeEvent(event) };
    }),
  );

  app.delete(
    eventRoute,
    { schema: { params: eventParams } },
    tracked<EventParams>(eventRoute, async (request, reply) => {
      const { event_id: eventId } = request.params;
      await useCases.deleteEvent(eventId);
      log("event_deleted", { event_id: eventId });
      return reply.code(204).send();
    }),
  );

  app.get("/metrics", async (_request, reply) => {
    reply.header("Content-Type", metrics.contentType);
    return metrics.render();
  });

  app.get("/health/live", async () => ({ status: "UP", service: serviceName }));

  app.get("/health/ready", async (_request, reply) => {
    try {
      await checkReadiness();
      return { status: "UP", service: serviceName };
    } catch (err) {
      log("readiness_check_failed", { error: errorMessage(err) });
      return reply.code(503).send({ status: "DOWN", service: serviceName });
    }
  });
}
