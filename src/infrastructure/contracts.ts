import type { Attributes, Tracer } from "@opentelemetry/api";

export interface Tracing {
  tracer: Tracer;
  currentTraceId(): string | null;
  withSpan<T>(
    name: string,
    attributes: Attributes,
    fn: () => Promise<T>,
  ): Promise<T>;
}

export type StructuredLogger = (
  event: string,
  fields?: Record<string, unknown>,
  level?: "INFO" | "WARN" | "ERROR",
) => void;

export interface HttpMetrics {
  track(route: string): (status: number) => void;
  contentType: string;
  render(): Promise<string>;
}

export interface MessagingMetrics {
  recordMessage(
    topic: string,
    outcome: "success" | "error" | "dead_letter",
  ): void;
  startMessageTimer(topic: string): () => number;
}
