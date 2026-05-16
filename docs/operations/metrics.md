# DataStream Metrics Reference

DataStream exposes Prometheus metrics on the HTTP API server at `/metrics`.
All metrics share the `datastream_` prefix.

## Configuration

```toml
cluster = "prod-east"   # value of the 'cluster' label on every metric

[metrics]
enabled = true              # set false to disable pull-mode gauges
scrape-interval = "5s"      # how often StatsCollector polls connectors
stats-timeout = "1s"        # per-connector Stats() timeout
```

Or via env: `DATASTREAM_CLUSTER=prod-east` (custom flag wiring is environment-specific).

## Metric Catalog

### Task

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_task_total` | Gauge | cluster, status | Cluster-level task state distribution |
| `datastream_task_state` | Gauge | cluster, task, state | Per-task current state (0/1) |
| `datastream_task_events_total` | Counter | cluster, task, type, result | Events processed (type=insert/update/delete/truncate/ddl/heartbeat/tombstone; result=success/failed) |
| `datastream_task_events_bytes` | Counter | cluster, task | Total bytes processed (estimate via ChangeEvent.Size) |
| `datastream_task_latency_seconds` | Histogram | cluster, task | End-to-end event latency |

### Source

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_source_position` | Gauge | cluster, task, source | Numeric position (when applicable) |
| `datastream_source_snapshot_progress` | Gauge | cluster, task | Snapshot 0-100% |
| `datastream_source_lag_seconds` | Gauge | cluster, task, source | CDC lag (now - event_time) |
| `datastream_source_last_event_seconds` | Gauge | cluster, task, source | Unix timestamp of last event |
| `datastream_snapshot_tables_total` | Gauge | cluster, task | Tables to snapshot |
| `datastream_snapshot_tables_remaining` | Gauge | cluster, task | Tables not yet snapshotted |

### Sink

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_sink_write_latency_seconds` | Histogram | cluster, task, sink | Sink write latency |
| `datastream_sink_write_errors_total` | Counter | cluster, task, sink, error_type | Errors classified retriable/non_retriable |

### Pipeline & Connector

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_pipeline_queue_size` | Gauge | cluster, task, stage | Current queue depth |
| `datastream_pipeline_queue_capacity` | Gauge | cluster, task, stage | Max queue depth |
| `datastream_pipeline_process_time_seconds` | Histogram | cluster, task, stage | Per-stage processing time |
| `datastream_connector_connected` | Gauge | cluster, task, role, type | Connection health 0/1 |

## Recommended Alerts

```yaml
- alert: DataStreamSourceLagHigh
  expr: datastream_source_lag_seconds > 60
  for: 5m
  annotations: { summary: "CDC lag > 60s on {{ $labels.task }}" }

- alert: DataStreamSinkErrorsRising
  expr: sum by (task) (rate(datastream_sink_write_errors_total[5m])) > 0.1
  for: 10m
  annotations: { summary: "Sink errors >0.1/s on {{ $labels.task }}" }

- alert: DataStreamConnectorDown
  expr: datastream_connector_connected == 0
  for: 2m
  annotations: { summary: "{{ $labels.role }} {{ $labels.type }} disconnected" }
```

## Caveats

- **`result=failed` undercounts** when sinks have internal retries (counted
  only when error escapes the sink). Precise accounting requires the
  separate "retry architecture unification" task.
- **`result=success` ≠ "written to sink"** — counts events flowing past
  the Pipeline consume point. Sink failures appear separately via
  `result=failed` on the sink decorator path.
- **`source_lag_seconds` depends on NTP** — clock skew may yield brief
  spikes. Negative values are clamped to 0.
- **Connector `Stats()` is best-effort** — connectors implementing
  `StatsProvider` populate fields they track; others remain zero.
  All 12 connectors currently emit `Connected` and `Position` only;
  richer fields (snapshot progress, lag) will be filled in as each
  connector's internal tracking is enhanced.
- **MySQL/MariaDB Position** currently uses `BinlogFile:BinlogPos`.
  GTID support is planned and will appear in this field automatically
  when added to `event.Position`.
