import { createClient } from "redis";
import { logError } from "./observability.js";
import type { ItemsCache } from "../application/ports.js";
import type { Item } from "../domain/models.js";
import type { Tracing } from "./contracts.js";

interface RedisCacheClient {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, options: { EX: number }): Promise<unknown>;
  del(key: string): Promise<number>;
}

export function createRedisClient(url: string) {
  const client = createClient({ url });
  client.on("error", (err) => logError("redis_error", err));
  return client;
}

export class RedisItemsCache implements ItemsCache {
  constructor(
    private readonly client: RedisCacheClient,
    private readonly tracing: Tracing,
    private readonly key: string,
  ) {}

  async get() {
    const cached = await this.tracing.withSpan(
      "node.cache_lookup",
      { "db.system": "redis" },
      () => this.client.get(this.key),
    );
    if (!cached) return null;
    try {
      const value: unknown = JSON.parse(cached);
      if (!Array.isArray(value)) return null;
      const items: Item[] = [];
      for (const item of value as unknown[]) {
        if (
          !item ||
          typeof item !== "object" ||
          !("id" in item) ||
          typeof item.id !== "number" ||
          !("name" in item) ||
          typeof item.name !== "string" ||
          !("createdAt" in item) ||
          typeof item.createdAt !== "string"
        )
          return null;
        const createdAt = new Date(item.createdAt);
        if (Number.isNaN(createdAt.getTime())) return null;
        items.push({ id: item.id, name: item.name, createdAt });
      }
      return items;
    } catch {
      return null;
    }
  }

  async set(items: Item[], ttlSeconds: number) {
    await this.client.set(this.key, JSON.stringify(items), { EX: ttlSeconds });
  }

  async invalidate() {
    await this.client.del(this.key);
  }
}
