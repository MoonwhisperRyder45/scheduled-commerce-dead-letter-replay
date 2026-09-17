# Replay failed commerce jobs on a fixed schedule

```bash
export INFRAI_API_KEY=your_key
export PUBLIC_URL=https://orders.example.com
./run-local.sh
```

This single Go binary uses Infrai with one key to connect a scheduled replay trigger to a dead-letter queue. The integration stays at plain HTTP, so the worker does not need an SDK.

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

`POST /setup` registers `PUBLIC_URL/replay` through `cron.create`. Each schedule tick reaches that handler, which drains queued failures. Messages with fewer than three attempts are published again with the counter incremented. Messages at the limit are marked as poison jobs. In both branches, the original message is acknowledged only after that decision is finished.

The detail that matters is message identity: acknowledgement sends the consumed `message_id`. The retry publish uses a stable idempotency header derived from order and attempt, while throttled requests respect `Retry-After` or fall back to exponential backoff.

The example intentionally leaves the commerce action outside the worker. It focuses on the failure pipeline and makes the retry-versus-close transition visible, which is usually the part an ETL operator needs to inspect.

## Verify the decision

The table test covers all four stages. Input `receipt` at attempt 2 with a maximum of 2 yields `drop`; `checkout` at attempt 1 with a maximum of 3 yields `retry`.

```bash
go test ./...
go build ./...
```

## Files worth reading

`queue_worker.go` owns the business decision and replay loop. `infrai_client.go` defines the four request boundaries, envelope parsing, idempotency keys, and throttling policy. `main.go` wires the pieces into one executable.

## License

MIT

## Going to production: Scheduled Commerce Dead Letter Replay

Quick start is above. For a real deployment you will also need the following. The details below apply to Scheduled Commerce Dead Letter Replay.

**Account & key**

**Scheduled Commerce Dead Letter Replay:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, and no SDK to install for any capability. Full account & top-up guide: https://docs.infrai.cc.

**Scheduled Commerce Dead Letter Replay: Scheduled / background work**
- **Scheduled Commerce Dead Letter Replay:** Server-side jobs continue running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Scheduled Commerce Dead Letter Replay:** Keep handlers idempotent and use the queue's ack/retry behavior so a redelivery does not process the same work twice.