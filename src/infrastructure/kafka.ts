import {
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
} from "@opentelemetry/api";
import { Kafka, logLevel } from "kafkajs";
import { parseEventMessage } from "../application/event-message.js";
import type { Consumer, Producer, EachMessagePayload } from "kafkajs";
import type {
  Tracing,
  StructuredLogger,
  MessagingMetrics,
} from "./contracts.js";
import type { EventInput } from "../domain/models.js";
import { asError, errorMessage } from "../domain/error-utils.js";

export interface KafkaConsumerOptions {
  brokers: string[];
  topic: string;
  dlqTopic: string;
  groupId: string;
  clientId: string;
  tracing: Pick<Tracing, "tracer">;
  log: StructuredLogger;
  metrics?: MessagingMetrics;
  onEvent: (event: EventInput) => Promise<void>;
  kafka?: Pick<Kafka, "consumer" | "producer">;
}

export class KafkaEventConsumer {
  private readonly topic: string;
  private readonly dlqTopic: string;
  private readonly groupId: string;
  private readonly tracing: Pick<Tracing, "tracer">;
  private readonly log: StructuredLogger;
  private readonly metrics: MessagingMetrics | undefined;
  private readonly onEvent: (event: EventInput) => Promise<void>;
  private connected: boolean;
  private readonly consumer: Consumer;
  private readonly producer: Producer;

  constructor({
    brokers,
    topic,
    dlqTopic,
    groupId,
    clientId,
    tracing,
    log,
    metrics,
    onEvent,
    kafka,
  }: KafkaConsumerOptions) {
    this.topic = topic;
    this.dlqTopic = dlqTopic;
    this.groupId = groupId;
    this.tracing = tracing;
    this.log = log;
    this.metrics = metrics;
    this.onEvent = onEvent;
    this.connected = false;
    const client =
      kafka || new Kafka({ clientId, brokers, logLevel: logLevel.WARN });
    this.consumer = client.consumer({ groupId, allowAutoTopicCreation: false });
    this.producer = client.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 1,
    });
    this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      this.connected = true;
    });
    for (const event of [
      this.consumer.events.DISCONNECT,
      this.consumer.events.CRASH,
      this.consumer.events.STOP,
    ]) {
      this.consumer.on(event, () => {
        this.connected = false;
      });
    }
  }

  isConnected() {
    return this.connected;
  }

  async start() {
    try {
      await this.producer.connect();
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });
      await this.consumer.run({
        autoCommit: false,
        eachMessage: (payload) => this.handleMessage(payload),
      });
    } catch (err) {
      await this.stop();
      throw err;
    }
  }

  async stop() {
    this.connected = false;
    try {
      await this.consumer.disconnect();
    } finally {
      await this.producer.disconnect();
    }
  }

  async handleMessage({
    topic,
    partition,
    message,
  }: Pick<EachMessagePayload, "topic" | "partition" | "message">) {
    const headers = Object.fromEntries(
      Object.entries(message.headers || {}).map(([key, value]) => [
        key,
        Buffer.isBuffer(value) ? value.toString() : value,
      ]),
    );
    const parentContext = propagation.extract(context.active(), headers);
    return this.tracing.tracer.startActiveSpan(
      "node.consume.test_event",
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          "messaging.system": "kafka",
          "messaging.destination.name": topic,
          "messaging.consumer.group.name": this.groupId,
          "messaging.destination.partition.id": String(partition),
          "messaging.kafka.offset": message.offset,
          "messaging.operation.type": "process",
        },
      },
      parentContext,
      async (span) => {
        const end = this.metrics?.startMessageTimer(topic);
        try {
          let event;
          try {
            event = parseEventMessage(message.value?.toString() ?? "");
          } catch (err) {
            // Conserva el mensaje invalido. Solo avanza el offset si Kafka confirma la DLQ.
            await this.producer.send({
              topic: this.dlqTopic,
              acks: -1,
              messages: [
                {
                  key: message.key,
                  value: message.value,
                  headers: {
                    ...message.headers,
                    "source-topic": topic,
                    "source-partition": String(partition),
                    "source-offset": message.offset,
                    "error-message": errorMessage(err),
                  },
                },
              ],
            });
            await this.commit(topic, partition, message.offset);
            this.metrics?.recordMessage(topic, "dead_letter");
            this.log(
              "kafka_invalid_message",
              {
                topic,
                partition,
                offset: message.offset,
                error: errorMessage(err),
              },
              "ERROR",
            );
            return;
          }
          await this.onEvent(event);
          // PostgreSQL confirma primero; el event_id unico protege ante una entrega repetida.
          await this.commit(topic, partition, message.offset);
          this.metrics?.recordMessage(topic, "success");
          this.log("event_consumed", {
            event_id: event.eventId,
            topic,
            partition,
            offset: message.offset,
          });
        } catch (err) {
          this.metrics?.recordMessage(topic, "error");
          span.recordException(asError(err));
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: errorMessage(err),
          });
          this.log(
            "kafka_consume_failed",
            {
              topic,
              partition,
              offset: message.offset,
              error: errorMessage(err),
            },
            "ERROR",
          );
          // KafkaJS reinicia el consumidor desde el ultimo offset confirmado.
          throw err;
        } finally {
          end?.();
          span.end();
        }
      },
    );
  }

  private async commit(topic: string, partition: number, offset: string) {
    await this.consumer.commitOffsets([
      { topic, partition, offset: (BigInt(offset) + 1n).toString() },
    ]);
  }
}
