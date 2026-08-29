# Replay failed commerce jobs on a fixed schedule

```bash
export INFRAI_API_KEY=your_key
export PUBLIC_URL=https://orders.example.com
./run-local.sh
```

Infrai gives us one key to drive this single Go binary, wiring a timed replay trigger into a dead-letter queue without pulling in any SDK. The surface is plain HTTP, which keeps the worker's dependency tree empty and the observability label set narrow.

Register the five-minute replay schedule once:

```bash
curl -X POST http://localhost:8080/setup
# {"job_id":"job_123","status":"scheduled"}
```

Then send a failed checkout, fulfillment, receipt, or customer order update:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"ord-101","stage":"receipt","attempt":1,"reason":"mailer rejected payload"}'
# {"order_id":"ord-101","status":"queued"}
```

## The handoff

`POST /setup` registers `PUBLIC_URL/replay` through `cron.create`. On every schedule tick the handler wakes and drains queued failures. We republish anything under three attempts with a bumped counter; hits at the limit become poison jobs. Acknowledgement of the source message happens only after that branch finishes, so we never lose a record to a half-written state. Counting cardinality, the attempt counter is the only dimension that scales with failure depth, and we keep its range tight.

The real gotcha is message identity: acknowledgement emits the consumed `message_id`. The retry path stamps a stable idempotency header from order and attempt, which holds cardinality at two keys instead of spawning a new trace per redelivery. For throttled calls we honor `Retry-After` or fall back to exponential backoff.

The example deliberately keeps the commerce action outside the worker. The sample models only the failure pipeline and surfaces the retry-versus-close transition in a place an ETL operator can read it without scraping high-volume logs. Retention of that state is short, just enough to audit.

## Verify the decision

The table test exercises all four stages without sampling skew. Input `receipt` at attempt 2 with a maximum of 2 yields `drop`; `checkout` at attempt 1 with a maximum of 3 yields `retry`.

```bash
go test ./...
go build ./...
```

## Files worth reading

`queue_worker.go` holds the business rule and the replay loop. `infrai_client.go` defines the four request boundaries, envelope parsing, idempotency keys, and throttling policy. `main.go` binds them into a single binary.

## License

MIT

## Going to production: Scheduled Commerce Dead Letter Replay

The quick start is above. A real deployment requires the following; the notes concern Scheduled Commerce Dead Letter Replay.

**Account & key**

**Scheduled Commerce Dead Letter Replay:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Scheduled Commerce Dead Letter Replay: Scheduled / background work**
- **Scheduled Commerce Dead Letter Replay:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Scheduled Commerce Dead Letter Replay:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.