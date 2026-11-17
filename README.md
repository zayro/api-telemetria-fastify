# Servicio de prueba Fastify

Servicio HTTP del app con Node.js 24, TypeScript 7 y Fastify 5 para probar
observabilidad, persistencia, cache y consumo de eventos. Se expone a traves
de Traefik (`/api/node`); dentro de Docker escucha en el puerto `8084`.

## Arquitectura

```text
app.ts                      composicion e inicio y cierre del servicio
telemetry.ts                SDK OpenTelemetry, precargado con --import
src/
  config.ts                 variables de entorno y validacion al iniciar
  domain/                   modelos y errores de negocio
  application/              casos de uso, puertos y validacion de eventos
  infrastructure/           PostgreSQL, Redis, Kafka, metricas, trazas y logs
  interfaces/http/          rutas Fastify, esquemas y mapeo de errores HTTP
tests/                      pruebas TypeScript con node:test y tsx
dist/                       JavaScript compilado para produccion (generado)
```

Las rutas invocan casos de uso, que reciben repositorios, cache, generador de
ids y `sleep` por inyeccion, sin conocer frameworks ni drivers. Los contratos
estan en `src/application/ports.ts`; `app.ts` conecta las implementaciones.
Los modelos usan `Date` internamente y conservan el formato HTTP snake_case.

TypeScript usa `strict`, `noUncheckedIndexedAccess` y
`exactOptionalPropertyTypes`. Los imports relativos terminan en `.js` para que
el resultado compilado funcione con ESM y `NodeNext`. JSON externo se trata
como `unknown` y se valida antes de usarlo. Un cache malformado se trata como
un miss. Cada instancia de metricas tiene su propio registro.

## Desarrollo

Requiere Node.js 24 o superior y npm. Desde esta carpeta:

```powershell
npm ci
Copy-Item .env.example .env
node --env-file=.env --import tsx --import ./telemetry.ts app.ts
```

PostgreSQL, Redis, Kafka y el colector OTLP deben estar disponibles. El archivo
`.env.example` contiene direcciones locales. Los topics Kafka deben existir,
incluido el de DLQ. El servicio no carga `.env` automaticamente.

Con las variables ya configuradas, `npm run dev` inicia el servicio con recarga
automatica. Para usar `.env` con recarga:

```powershell
node --env-file=.env --import tsx --watch --import ./telemetry.ts app.ts
```

## Calidad y produccion

```powershell
npm run check        # tipos, pruebas y compilacion
npm run format       # aplicar formato Prettier
npm run format:check
npm run build
npm start            # ejecuta dist/ con telemetria y source maps
```

Las versiones estan fijadas en `package.json` y `package-lock.json`; usa
`npm ci` para reproducir la instalacion. Docker compila en una etapa separada
y ejecuta solo dependencias de produccion como usuario `node` con Node.js 24.
`compose.yaml` se incluye desde la plataforma que proporciona las dependencias.
SIGTERM y SIGINT cierran HTTP, Kafka, Redis y PostgreSQL antes de vaciar la
telemetria, con un limite de diez segundos para el cierre por senal.

Las metricas usan `@prometheus-io/client`, sucesor oficial de `prom-client`
([cambio de nombre](https://github.com/prometheus/client_js/blob/main/CHANGELOG.md)).

## Endpoints

- `GET /api/node/public`: informacion publica sin JWT.
- `GET /api/node?delay_ms=100&fail=0`: responde `pong`; `delay_ms` acepta 0-2000
  y `fail=1` genera un error 500 simulado.
- `POST /api/node/items` con `{"name":"app"}` y `GET /api/node/items`: ejemplo
  con PostgreSQL y cache Redis (cache-aside, TTL 30 s).
- `POST /api/node/events`: crea un evento, responde `201` y no publica en Kafka.
- `GET /api/node/events`: los 20 eventos mas recientes.
- `GET|PUT|DELETE /api/node/events/{event_id}`: `404` si no existe; `400` si el
  id no es UUID o el nombre es invalido (1-100 caracteres).
- `GET /health/live`: el proceso responde.
- `GET /health/ready`: verifica PostgreSQL, Redis y la conexion del consumidor
  Kafka; responde `503` si alguno falla.
- `GET /metrics`: metricas Prometheus.

## Kafka

El servicio consume `test.events`, publicado por FastAPI, con KafkaJS.
Confirma manualmente el siguiente offset despues de guardar en PostgreSQL;
si falla, reintenta desde el ultimo offset confirmado. Los mensajes invalidos
se conservan en `test.events.dlq` antes de confirmar su offset original.
La insercion es idempotente por `event_id`. Los grupos nuevos leen desde el
principio del historial retenido; el grupo predeterminado es `test-fastify-service`.
Los spans de consumo recuperan el contexto W3C de las cabeceras del evento.

## Configuracion

`SERVICE_NAME`, `SERVICE_PORT`, `DATABASE_URL`, `REDIS_URL`, `KAFKA_BROKERS`,
`KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`, `KAFKA_CONSUMER_GROUP`,
`OTEL_EXPORTER_OTLP_ENDPOINT` y `DEPLOYMENT_ENVIRONMENT`. En Compose apuntan
a los nombres de servicio de la red Docker. Fuera de Docker, usar
`KAFKA_BROKERS=localhost:29092`.

Al iniciar se validan el puerto (entero entre 1 y 65535), los brokers no vacios
y los protocolos de las URLs de PostgreSQL y Redis.

## Pruebas

Las pruebas usan dobles tipados y `Fastify.inject`; no requieren servicios
externos ni Docker. Cubren casos de uso, rutas HTTP, offsets y DLQ de Kafka,
configuracion, cache y metricas:

```powershell
npm test
npm run typecheck
```

GitHub Actions ejecuta formato, tipos, pruebas y build en cada push y pull request.
