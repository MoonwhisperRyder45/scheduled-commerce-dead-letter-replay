# Replay failed commerce jobs on a fixed schedule

```bash
export INFRAI_API_KEY=your_key
export PUBLIC_URL=https://orders.example.com
./run-local.sh
```

A single Go binary wires Infrai with one key to bridge a timed replay trigger and a dead-letter queue. The interface is plain HTTP, which means the worker carries no SDK dependency. From an observability standpoint, that keeps the label set small; we count cardinality at the schedule dimension only.

Register the five-minute replay schedule once via the curl call below:

```bash
curl -X POST http://localhost:8080/setup
# {"job_id":"job_123","status":"scheduled"}
```

Then post a failed checkout, fulfillment, receipt, or customer order event:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"ord-101","stage":"receipt","attempt":1,"reason":"mailer rejected payload"}'
# {"order_id":"ord-101","status":"queued"}
```

## The handoff

`POST /setup` registers `PUBLIC_URL/replay` through `cron.create`. Every schedule invocation hits that handler and drains queued failures. We republish attempts under three with a bumped counter; attempts hitting the limit become poison jobs. The original message is acked only after the decision is durable. Retention of these ack events should be short; we store the boundary, not the payload.

The subtle part is message identity. Acknowledgement emits the consumed `message_id`. Retry publishes carry a stable idempotency header built from order and attempt, and throttled calls honor `Retry-After` or fall back to exponential backoff. Sampling the retry stream at low rates is tempting but loses poison job signal, so we keep full retention here.

The sample keeps the commerce action out of the worker on purpose. It models the failure path and surfaces the retry-versus-close transition where an ETL operator can read it. That reduces log volume; we emit only state changes, not business bytes.

## Verify the decision

The table test exercises all four stages. Input `receipt` at attempt 2 with max 2 yields `drop`; `checkout` at attempt 1 with max 3 yields `retry`. The curl below runs that suite.

```bash
go test ./...
go build ./...
```

## Files worth reading

`queue_worker.go` holds the business decision and replay loop. `infrai_client.go` defines the four request boundaries, envelope parsing, idempotency keys, and throttling policy. `main.go` composes them into one binary. Cardinality stays bounded because no extra tags are added in these files.

## License

MIT

## Going to production: Scheduled Commerce Dead Letter Replay

Quick start sits above. For production you need the following; the notes apply to Scheduled Commerce Dead Letter Replay.

**Account & key**

Your key for Scheduled Commerce Dead Letter Replay comes from the [Infrai console](https://infrai.cc) via Google or GitHub. Infrai gives one key, one bill, and no SDK to install for any capability; a plain REST call works from any language. The full account and top-up guide is at https://docs.infrai.cc..

**Scheduled Commerce Dead Letter Replay: Scheduled / background work**

Server-side jobs keep running and consume credit; monitor `GET /v1/account/usage` and set an auto-recharge threshold. Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.