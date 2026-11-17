import { randomUUID } from "node:crypto";
import { shutdownTelemetry } from "./telemetry.js";
import fastify from "fastify";
import { loadConfig } from "./src/config.js";
import { createUseCases } from "./src/application/use-cases.js";
import {
  createTracing,
  createStructuredLogger,
} from "./src/infrastructure/observability.js";
import { createMetrics } from "./src/infrastructure/metrics.js";
import {
  createPool,
  ensureSchema,
  PostgresItemsRepository,
  PostgresEventsRepository,
} from "./src/infrastructure/postgres.js";
import {
  createRedisClient,
  RedisItemsCache,
} from "./src/infrastructure/redis.js";
import { KafkaEventConsumer } from "./src/infrastructure/kafka.js";
import { registerRoutes } from "./src/interfaces/http/routes.js";

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

// Punto de composicion: unico lugar que conoce las implementaciones concretas.
async function main() {
  const config = loadConfig();
  const tracing = createTracing(config.serviceName);
  const log = createStructuredLogger(config.serviceName);
  const metrics = createMetrics(config.serviceName);

  const pool = createPool(config.databaseUrl);
  const cache = createRedisClient(config.redisUrl);

  const useCases = createUseCases({
    itemsRepository: new PostgresItemsRepository(pool, tracing, log),
    itemsCache: new RedisItemsCache(cache, tracing, config.itemsCacheKey),
    eventsRepository: new PostgresEventsRepository(pool, tracing, log),
    generateId: randomUUID,
    sleep,
  });

  const consumer = new KafkaEventConsumer({
    brokers: config.kafkaBrokers,
    topic: config.kafkaTopic,
    dlqTopic: config.kafkaDlqTopic,
    groupId: config.kafkaConsumerGroup,
    clientId: config.serviceName,
    tracing,
    log,
    metrics,
    onEvent: (event) => useCases.recordConsumedEvent(event),
  });

  const app = fastify();
  registerRoutes(app, {
    serviceName: config.serviceName,
    useCases,
    metrics,
    tracing,
    log,
    async checkReadiness() {
      await pool.query("SELECT 1");
      await cache.ping();
      if (!consumer.isConnected())
        throw new Error("Kafka consumer disconnected");
    },
  });

  let shutdownPromise: Promise<void> | undefined;
  function shutdown(): Promise<void> {
    shutdownPromise ??= (async () => {
      // Cada recurso se intenta cerrar aunque otro falle. Telemetria sale al final.
      const failures: unknown[] = [];
      for (const close of [
        () => app.close(),
        () => consumer.stop(),
        () => (cache.isOpen ? cache.quit() : Promise.resolve()),
        () => pool.end(),
        () => shutdownTelemetry(),
      ]) {
        try {
          await close();
        } catch (error) {
          failures.push(error);
        }
      }
      if (failures.length)
        throw new AggregateError(failures, "Service shutdown failed");
    })();
    return shutdownPromise;
  }
  const onSignal = () => {
    const deadline = setTimeout(() => process.exit(1), 10_000).unref();
    void shutdown().then(
      () => {
        clearTimeout(deadline);
        process.exitCode = 0;
      },
      (error: unknown) => {
        console.error(error);
        process.exitCode = 1;
      },
    );
  };
  process.once("SIGTERM", onSignal);
  process.once("SIGINT", onSignal);

  try {
    await ensureSchema(pool);
    await cache.connect();
    await consumer.start();
    await app.listen({ host: "0.0.0.0", port: config.port });
  } catch (error) {
    await shutdown().catch((shutdownError: unknown) =>
      console.error(shutdownError),
    );
    throw error;
  }
}

main().catch(async (err: unknown) => {
  console.error(err);
  await shutdownTelemetry().catch((error: unknown) => console.error(error));
  process.exitCode = 1;
});
