import type { EventInput, Item, StoredEvent } from "../domain/models.js";

export interface ItemsRepository {
  create(name: string): Promise<Item>;
  listRecent(limit: number): Promise<Item[]>;
}

export interface ItemsCache {
  get(): Promise<Item[] | null>;
  set(items: Item[], ttlSeconds: number): Promise<void>;
  invalidate(): Promise<void>;
}

export interface EventsRepository {
  create(event: EventInput): Promise<StoredEvent>;
  createIfAbsent(event: EventInput): Promise<void>;
  get(eventId: string): Promise<StoredEvent | null>;
  listRecent(limit: number): Promise<StoredEvent[]>;
  update(eventId: string, name: string): Promise<StoredEvent | null>;
  delete(eventId: string): Promise<boolean>;
}

export interface UseCaseDependencies {
  itemsRepository: ItemsRepository;
  itemsCache: ItemsCache;
  eventsRepository: EventsRepository;
  generateId: () => string;
  sleep: (milliseconds: number) => Promise<void>;
}
