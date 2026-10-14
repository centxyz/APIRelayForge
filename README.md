# APIRelayForge

[![CI](https://github.com/centxyz/APIRelayForge/actions/workflows/ci.yml/badge.svg)](https://github.com/centxyz/APIRelayForge/actions/workflows/ci.yml)

APIRelayForge is a durable webhook and event bridge. Applications publish JSON events to named routes; the bridge queues them, signs outbound requests, retries failures, records delivery history, and prevents duplicate submissions with idempotency keys.

## Features

- Persistent named webhook routes
- Durable queued event deliveries
- HMAC-SHA256 request signatures
- Idempotent publishing with `Idempotency-Key`
- Configurable bounded retries and request timeouts
- Recovery of interrupted deliveries after restart
- Delivery history and queue health metrics
- Atomic on-disk state updates

## Install

```bash
git clone https://github.com/centxyz/APIRelayForge.git
cd APIRelayForge
npm install
npm test
npm start
```

The service listens on port `3000` by default and stores state at `.apibridge/state.json`.

## Example

```bash
curl -X POST http://localhost:3000/v1/routes \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","targetUrl":"https://example.com/webhooks/orders","secret":"shared-secret"}'

curl -X POST http://localhost:3000/v1/routes/orders/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"orderId":42,"status":"paid"}'
```

Signed routes receive `X-API-Bridge-Signature-256: sha256=...`, calculated over the exact outbound JSON body. Every delivery also includes `X-API-Bridge-Delivery`.

## API

- `GET /health`
- `GET /v1/routes`
- `POST /v1/routes`
- `DELETE /v1/routes/:name`
- `POST /v1/routes/:name/events`
- `GET /v1/deliveries`
- `GET /v1/deliveries/:id`

## Configuration

- `PORT` — listening port, default `3000`
- `DATA_FILE` — state file, default `.apibridge/state.json`

## Test

```bash
npm test
```

The suite verifies route validation, secret redaction, signatures, delivery, retries, idempotency, persistence, and health metrics.

## License

MIT © cent

## Current limitations

- Restart recovery automatically resumes pending deliveries within their attempt limit. A crash after a receiver processes a request but before acknowledgement is saved can cause duplicate delivery; receivers should deduplicate `X-API-Bridge-Delivery`.
- The API has no built-in authentication. Run it on a trusted network or behind an authenticated proxy that restricts route administration and publishing. Restrict outbound destinations and redirects at the network boundary; caller-selected webhook URLs can otherwise reach internal services.
- Delivery acknowledgement confirms the destination HTTP response, not the destination application's internal side effects.
- The file-backed queue is designed for a single service instance, not a distributed cluster.
- Operators must secure route configuration, signing secrets, storage, and network access.
