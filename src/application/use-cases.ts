import { EventNotFoundError } from "../domain/errors.js";

import type { UseCaseDependencies } from "./ports.js";
import type { EventInput, Item } from "../domain/models.js";

export const CACHE_TTL_SECONDS = 30;
export const LIST_LIMIT = 20;

// Los puertos (repositorios, cache, generador de ids) se inyectan; este modulo no conoce frameworks ni drivers.
export function createUseCases({
  itemsRepository,
  itemsCache,
  eventsRepository,
  generateId,
  sleep,
}: UseCaseDependencies) {
  return {
    simulate: (delayMs: number) => sleep(delayMs),

    async createItem(name: string) {
      const item = await itemsRepository.create(name);
      // El cache queda obsoleto tras escribir
      await itemsCache.invalidate();
      return item;
    },

    async listItems(): Promise<{ source: "cache" | "db"; items: Item[] }> {
      const cached = await itemsCache.get();
      if (cached) return { source: "cache", items: cached };

      const items = await itemsRepository.listRecent(LIST_LIMIT);
      await itemsCache.set(items, CACHE_TTL_SECONDS);
      return { source: "db", items };
    },

    createEvent: (name: string) =>
      eventsRepository.create({ eventId: generateId(), name }),

    listEvents: () => eventsRepository.listRecent(LIST_LIMIT),

    async getEvent(eventId: string) {
      const event = await eventsRepository.get(eventId);
      if (!event) throw new EventNotFoundError(eventId);
      return event;
    },

    async updateEvent(eventId: string, name: string) {
      const event = await eventsRepository.update(eventId, name);
      if (!event) throw new EventNotFoundError(eventId);
      return event;
    },

    async deleteEvent(eventId: string) {
      if (!(await eventsRepository.delete(eventId)))
        throw new EventNotFoundError(eventId);
    },

    // Idempotente: reintentos del broker no duplican eventos
    recordConsumedEvent: (event: EventInput) =>
      eventsRepository.createIfAbsent(event),
  };
}

export type UseCases = ReturnType<typeof createUseCases>;
