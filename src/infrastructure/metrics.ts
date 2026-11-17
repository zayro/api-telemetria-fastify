import client from "@prometheus-io/client";

import type { HttpMetrics, MessagingMetrics } from "./contracts.js";

export function createMetrics(
  serviceName: string,
): HttpMetrics & MessagingMetrics {
  const register = new client.Registry();
  const messages = new client.Counter({
    name: "app_messaging_processed_total",
    help: "Eventos Kafka procesados, fallidos o enviados a DLQ",
    registers: [register],
    labelNames: ["service", "topic", "outcome"],
  });
  const messageDuration = new client.Histogram({
    name: "app_messaging_process_duration_seconds",
    help: "Tiempo de procesamiento de eventos Kafka",
    registers: [register],
    labelNames: ["service", "topic"],
  });
  const requests = new client.Counter({
    name: "app_http_requests_total",
    help: "Total de solicitudes HTTP del app",
    registers: [register],
    labelNames: ["service", "route", "status"],
  });
  const duration = new client.Histogram({
    name: "app_http_request_duration_seconds",
    help: "Duracion de solicitudes HTTP del app",
    registers: [register],
    labelNames: ["service", "route"],
  });

  return {
    recordMessage: (topic, outcome) =>
      messages.inc({ service: serviceName, topic, outcome }),
    startMessageTimer: (topic) =>
      messageDuration.startTimer({ service: serviceName, topic }),
    track(route) {
      const end = duration.startTimer({ service: serviceName, route });
      return (status) => {
        requests.inc({ service: serviceName, route, status: String(status) });
        end();
      };
    },
    contentType: register.contentType,
    render: () => register.metrics(),
  };
}
