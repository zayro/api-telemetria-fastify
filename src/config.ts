export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  const port = Number(env.SERVICE_PORT || 8084);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("SERVICE_PORT must be an integer between 1 and 65535");
  }
  const kafkaBrokers = (env.KAFKA_BROKERS || "kafka:9092")
    .split(",")
    .map((broker) => broker.trim());
  if (kafkaBrokers.some((broker) => !broker))
    throw new Error("KAFKA_BROKERS contains an empty broker");
  const databaseUrl =
    env.DATABASE_URL || "postgresql://app:app@postgres:5432/app";
  const redisUrl = env.REDIS_URL || "redis://redis:6379/0";
  for (const [name, value, protocols] of [
    ["DATABASE_URL", databaseUrl, ["postgres:", "postgresql:"]],
    ["REDIS_URL", redisUrl, ["redis:", "rediss:"]],
  ] as const) {
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      throw new Error(`${name} must be a valid URL`);
    }
    if (
      !(protocols as readonly string[]).includes(url.protocol) ||
      !url.hostname
    ) {
      throw new Error(`${name} has an invalid protocol or host`);
    }
  }
  return {
    serviceName: env.SERVICE_NAME || "test-fastify-service",
    port,
    databaseUrl,
    redisUrl,
    kafkaBrokers,
    kafkaTopic: env.KAFKA_TOPIC || "test.events",
    kafkaDlqTopic: env.KAFKA_DLQ_TOPIC || "test.events.dlq",
    kafkaConsumerGroup: env.KAFKA_CONSUMER_GROUP || "test-fastify-service",
    // Sufijo v2: el formato cacheado cambio respecto a versiones anteriores
    itemsCacheKey: "node:items:v2",
  };
}
