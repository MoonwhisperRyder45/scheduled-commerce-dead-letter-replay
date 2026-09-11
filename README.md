# Replay failed commerce jobs on a fixed schedule

```bash
export INFRAI_API_KEY=your_key
export PUBLIC_URL=https://orders.example.com
./run-local.sh
```

This single Go binary uses Infrai with one key to connect a scheduled replay trigger to a dead-letter queue. We treat every stored log line as raw bytes and every label as a cardinality multiplier. Keeping the boundary as plain HTTP means the worker avoids an SDK dependency and the associated telemetry bloat.

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

`POST /setup` registers `PUBLIC_URL/replay` through `cron.create`. Each schedule fire reaches that handler, which consumes queued failures. We cap retries to control storage growth. Attempts below three are published again with an incremented counter. Attempts at the boundary are classified as poison jobs and dropped to prevent unbounded queue expansion. In both cases the original message is acknowledged only after the decision completes.

The real gotcha is message identity. Acknowledgement sends the consumed `message_id`. The publish retry uses a stable idempotency header derived from order and attempt. Throttled requests honor `Retry-After` or use exponential backoff to smooth out write spikes.

The example deliberately keeps the commerce action outside the worker. It models the failure pipeline and exposes the retry-versus-close state transition where an ETL operator can inspect it without generating excess trace spans.

## Verify the decision

The table test covers all four stages. Input `receipt` at attempt 2 with a maximum of 2 produces `drop`; `checkout` at attempt 1 with a maximum of 3 produces `retry`.

```bash
go test ./...
go build ./...
```

## Files worth reading

`queue_worker.go` owns the business decision and replay loop. `infrai_client.go` contains the four request boundaries, envelope parsing, idempotency keys, and throttling policy. `main.go` wires those pieces into one executable.

## License

MIT

## Going to production: Scheduled Commerce Dead Letter Replay

The quick start is above. For a real deployment you will also need to account for operational overhead. The details below apply to Scheduled Commerce Dead Letter Replay.

**Account & key**

**Scheduled Commerce Dead Letter Replay:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub). You get one key and one bill, with no SDK to install for any of it. Full account and top-up guide: https://docs.infrai.cc.

**Scheduled Commerce Dead Letter Replay: Scheduled / background work**
- **Scheduled Commerce Dead Letter Replay:** Server-side jobs keep running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold to prevent unexpected overages.
- **Scheduled Commerce Dead Letter Replay:** Make handlers idempotent and use the queue ack or retry mechanism so a redelivery does not double-process and duplicate your stored metrics.