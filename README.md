# KafkaScope

A production-quality terminal UI for real-time Kafka cluster monitoring. Watch topic throughput, consumer-group lag, partition leadership, and broker status — all in one glanceable dashboard.

```
┌── KafkaScope  localhost:9092  brokers: 1  refreshed: 14:03:22 ⠋
│ [1] Topics  [2] Groups  [3] Partitions  [4] Brokers
├────────────────────────────────────────────────────────────────
│  Topic           Partitions  RF  Messages   msg/s
│  ads-events      3            1  124,501    45.3
│  orders          3            1   98,712    62.1
│  payments        3            1   54,030    29.8
├────────────────────────────────────────────────────────────────
│  q quit  r refresh  / filter  esc back/clear  ↑↓/jk navigate
```

## Screenshot

> Run `make demo-up && make demo-seed && make run` then take a screenshot here.

## Quickstart (demo)

```bash
# 1. Start a single-node Kafka 3.7 in KRaft mode
make demo-up

# 2. Create topics and start producers / consumer group
#    (wait ~15 s for Kafka to finish starting first)
make demo-seed

# 3. Launch KafkaScope
make run
```

`make demo-down` tears everything down and removes the volume.

## Installation

```bash
git clone https://github.com/KhalidLam/kafkaScope
cd kafkaScope
make build           # produces bin/kafkascope
```

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--brokers` | `localhost:9092` | Comma-separated broker list (also `KAFKA_BROKERS` env) |
| `--refresh` | `3s` | Polling interval |
| `--version` | — | Print version and exit |

```bash
kafkascope --brokers kafka1:9092,kafka2:9092 --refresh 5s
```

## Keybindings

| Key | Action |
|-----|--------|
| `1` `2` `3` `4` | Switch to Topics / Groups / Partitions / Brokers tab |
| `tab` / `shift-tab` | Cycle through tabs |
| `↑` `↓` / `j` `k` | Navigate table rows |
| `enter` | Drill down (topic → partitions, group → per-partition lag) |
| `/` | Filter current table |
| `esc` | Clear filter / exit drill-down / go back |
| `r` | Force immediate refresh |
| `q` | Quit |

## How it works

```mermaid
flowchart LR
    K[Kafka Cluster]        -->|sarama ClusterAdmin + Client| C[saramaClusterClient]
    C                       -->|FetchSnapshot every N s|      P[Poller goroutine]
    P                       -->|program.Send SnapshotMsg|     B[Bubble Tea event loop]
    B                       -->|Update AppModel|              V[View → terminal]
    U[User keystrokes]      -->|tea.KeyMsg|                   B
    R[r key / refreshCh]    -->|channel token|                P
```

**Poller → snapshot → Bubble Tea messages**

1. A `Poller` goroutine owns all sarama calls. It ticks on an interval and can also be triggered immediately via a shared channel (the `r` key).
2. Each tick calls `FetchSnapshot()` — brokers, topics with partition metadata, log-end and oldest offsets, and all consumer-group committed offsets.
3. `EnrichWithThroughput` in `lag.go` diffs consecutive snapshots to compute `msg/s` per topic.
4. The enriched `ClusterSnapshot` is sent to the Bubble Tea event loop as `SnapshotMsg`.
5. The root `AppModel.Update` fans the snapshot out to all four sub-views which rebuild their table rows.

The UI never calls Kafka; data flows only inward via messages.

## Design decisions

**Polling over admin API watches** — Kafka's admin API does not support server-push notifications. `ListTopics`, `GetOffset`, and `ListConsumerGroupOffsets` are cheap idempotent reads that work against any broker version ≥ 3.x. Polling on a 1–10 s interval is sufficient for interactive monitoring and avoids the complexity of long-polling or watch streams.

**Lag math is isolated and pure** — `internal/kafka/lag.go` imports no sarama types. `ComputeLag` and `ComputeThroughput` operate on plain `int64` / `*TopicInfo` values derived from the snapshot. This makes the critical business logic trivially unit-testable without any Kafka infrastructure.

## Stack

- [IBM/sarama](https://github.com/IBM/sarama) — Kafka client (ClusterAdmin + Client)
- [charmbracelet/bubbletea](https://github.com/charmbracelet/bubbletea) — TUI framework
- [charmbracelet/bubbles](https://github.com/charmbracelet/bubbles) — table, textinput, spinner
- [charmbracelet/lipgloss](https://github.com/charmbracelet/lipgloss) — layout and styling

## License

MIT
