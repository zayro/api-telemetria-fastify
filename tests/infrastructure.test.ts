import test from "node:test";
import assert from "node:assert/strict";
import { loadConfig } from "../src/config.js";
import { createMetrics } from "../src/infrastructure/metrics.js";
import { RedisItemsCache } from "../src/infrastructure/redis.js";
import { createTracing } from "../src/infrastructure/observability.js";

test("config validates ports, broker lists and connection URLs before startup", () => {
  assert.equal(loadConfig({}).port, 8084);
  assert.deepEqual(
    loadConfig({ KAFKA_BROKERS: "one:9092, two:9092" }).kafkaBrokers,
    ["one:9092", "two:9092"],
  );
  for (const port of ["0", "65536", "abc", "1.5", " "]) {
    assert.throws(() => loadConfig({ SERVICE_PORT: port }), /SERVICE_PORT/);
  }
  assert.throws(
    () => loadConfig({ KAFKA_BROKERS: "one:9092," }),
    /KAFKA_BROKERS/,
  );
  assert.throws(
    () => loadConfig({ DATABASE_URL: "https://db" }),
    /DATABASE_URL/,
  );
  assert.throws(() => loadConfig({ REDIS_URL: "invalid" }), /REDIS_URL/);
});

test("metrics registries are isolated and retain HTTP and Kafka labels", async () => {
  const first = createMetrics("first");
  const second = createMetrics("second");
  first.track("/items")(201);
  first.recordMessage("events", "success");
  assert.match(
    await first.render(),
    /app_http_requests_total\{service="first",route="\/items",status="201"\} 1/,
  );
  assert.match(
    await first.render(),
    /app_messaging_processed_total\{service="first",topic="events",outcome="success"\} 1/,
  );
  assert.doesNotMatch(await second.render(), /service="first"/);
});

test("cache restores dates and treats malformed data as a miss", async () => {
  let value: string | null = null;
  const cache = new RedisItemsCache(
    {
      get: async () => value,
      set: async (_key, data) => {
        value = data;
      },
      del: async () => {
        value = null;
        return 1;
      },
    },
    createTracing("cache-test"),
    "items",
  );
  const items = [
    { id: 1, name: "test", createdAt: new Date("2026-01-01T00:00:00Z") },
  ];
  await cache.set(items, 30);
  assert.deepEqual(await cache.get(), items);
  for (const invalid of [
    "invalid",
    "null",
    "{}",
    '[{"id":1,"name":"test","createdAt":"invalid"}]',
    '[{"id":"1"}]',
  ]) {
    value = invalid;
    assert.equal(await cache.get(), null);
  }
  await cache.invalidate();
  assert.equal(await cache.get(), null);
});
