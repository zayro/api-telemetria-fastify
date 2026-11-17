import { trace, SpanStatusCode } from "@opentelemetry/api";
import { logs, SeverityNumber } from "@opentelemetry/api-logs";

import type { Tracing, StructuredLogger } from "./contracts.js";
import { asError, errorMessage } from "../domain/error-utils.js";

export function createTracing(serviceName: string): Tracing {
  const tracer = trace.getTracer(serviceName);

  return {
    tracer,

    currentTraceId() {
      const ctx = trace.getActiveSpan()?.spanContext();
      return ctx ? ctx.traceId : null;
    },

    withSpan(name, attributes, fn) {
      return tracer.startActiveSpan(name, async (span) => {
        try {
          span.setAttributes(attributes);
          return await fn();
        } catch (err) {
          span.recordException(asError(err));
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: errorMessage(err),
          });
          throw err;
        } finally {
          span.end();
        }
      });
    },
  };
}

// Log JSON por consola y por OTLP, correlacionado con la traza activa.
export function createStructuredLogger(serviceName: string): StructuredLogger {
  const otelLogger = logs.getLogger(serviceName);

  return function structuredLog(event, fields = {}, level = "INFO") {
    const payload: Record<string, unknown> = { event, ...fields };
    const ctx = trace.getActiveSpan()?.spanContext();
    if (ctx) {
      payload.trace_id = ctx.traceId;
      payload.span_id = ctx.spanId;
    }
    const body = JSON.stringify(payload);
    (level === "ERROR" ? console.error : console.log)(body);
    otelLogger.emit({
      severityNumber: SeverityNumber[level],
      severityText: level,
      body,
    });
  };
}

export function logError(event: string, err: unknown) {
  console.error(JSON.stringify({ event, error: errorMessage(err) }));
}
