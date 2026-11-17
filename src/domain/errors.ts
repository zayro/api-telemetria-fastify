export class EventNotFoundError extends Error {
  readonly eventId: string;

  constructor(eventId: string) {
    super(`Evento ${eventId} no encontrado.`);
    this.name = "EventNotFoundError";
    this.eventId = eventId;
  }
}
