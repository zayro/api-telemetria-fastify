export interface Item {
  id: number;
  name: string;
  createdAt: Date;
}

export interface EventInput {
  eventId: string;
  name: string;
}

export interface StoredEvent extends EventInput {
  receivedAt: Date;
  updatedAt: Date | null;
}
