import test from "node:test";
import assert from "node:assert/strict";
import { KafkaEventConsumer } from "../src/infrastructure/kafka.js";
import type {
  Consumer,
  Producer,
  ProducerRecord,
  EachMessagePayload,
  TopicPartitionOffsetAndMetadata,
} from "kafkajs";
import type { EventInput } from "../src/domain/models.js";
import { createTracing } from "../src/infrastructure/observability.js";

const eventId = "11111111-1111-4111-8111-111111111111";
function fixture({
  processError,
  dlqError,
}: { processError?: Error; dlqError?: Error } = {}) {
  const calls: (
    | ["persist", EventInput]
    | ["commit", TopicPartitionOffsetAndMetadata[]]
    | ["dlq", ProducerRecord]
  )[] = [];
  const consumer = {
    events: {
      GROUP_JOIN: "join",
      DISCONNECT: "disconnect",
      CRASH: "crash",
      STOP: "stop",
    },
    on() {},
    commitOffsets: async (offsets: TopicPartitionOffsetAndMetadata[]) => {
      calls.push(["commit", offsets]);
    },
  };
  const producer = {
    send: async (request: ProducerRecord) => {
      calls.push(["dlq", request]);
      if (dlqError) throw dlqError;
    },
  };
  const instance = new KafkaEventConsumer({
    topic: "test.events",
    dlqTopic: "test.events.dlq",
    groupId: "test-group",
    brokers: ["localhost:9092"],
    clientId: "test",
    kafka: {
      consumer: () => consumer as unknown as Consumer,
      producer: () => producer as unknown as Producer,
    },
    tracing: createTracing("kafka-test"),
    log() {},
    onEvent: async (event) => {
      calls.push(["persist", event]);
      if (processError) throw processError;
    },
  });
  const payload: Pick<EachMessagePayload, "topic" | "partition" | "message"> = {
    topic: "test.events",
    partition: 2,
    message: {
      key: null,
      timestamp: "0",
      attributes: 0,
      offset: "9007199254740993",
      value: Buffer.from(JSON.stringify({ event_id: eventId, name: "event" })),
      headers: {
        traceparent: Buffer.from(
          "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        ),
      },
    },
  };
  return { instance, calls, payload };
}

test("commits the next offset only after persistence, without losing integer precision", async () => {
  const { instance, calls, payload } = fixture();
  await instance.handleMessage(payload);
  assert.deepEqual(calls, [
    ["persist", { eventId, name: "event" }],
    [
      "commit",
      [{ topic: "test.events", partition: 2, offset: "9007199254740994" }],
    ],
  ]);
});

test("database failure leaves the offset uncommitted for retry", async () => {
  const { instance, calls, payload } = fixture({
    processError: new Error("database unavailable"),
  });
  await assert.rejects(instance.handleMessage(payload), /database unavailable/);
  assert.deepEqual(
    calls.map(([type]) => type),
    ["persist"],
  );
});

test("invalid payload is acknowledged only after confirmed dead letter publication", async () => {
  const { instance, calls, payload } = fixture();
  payload.message.value = Buffer.from("invalid-json");
  await instance.handleMessage(payload);
  assert.deepEqual(
    calls.map(([type]) => type),
    ["dlq", "commit"],
  );
  const call = calls[0];
  assert.ok(call && call[0] === "dlq");
  assert.equal(call[1].topic, "test.events.dlq");
  const message = call[1].messages[0];
  assert.ok(message);
  assert.equal(message.value?.toString(), "invalid-json");
  assert.equal(message.headers?.["source-offset"], payload.message.offset);
});

test("dead letter failure leaves the source offset uncommitted", async () => {
  const { instance, calls, payload } = fixture({
    dlqError: new Error("broker unavailable"),
  });
  payload.message.value = null;
  await assert.rejects(instance.handleMessage(payload), /broker unavailable/);
  assert.deepEqual(
    calls.map(([type]) => type),
    ["dlq"],
  );
});
