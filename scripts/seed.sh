#!/usr/bin/env bash
# seed.sh — creates demo topics, starts a background producer, and runs a consumer group.
# Requires docker compose to be running (make demo-up).
set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="localhost:9092"
TOPICS=(orders payments ads-events)
GROUP="demo-group"

log() { echo "[seed] $*"; }

wait_for_kafka() {
  log "Waiting for Kafka to be ready…"
  for i in $(seq 1 30); do
    if docker compose exec -T kafka "${KAFKA_BIN}/kafka-topics.sh" \
        --bootstrap-server "${BOOTSTRAP}" --list &>/dev/null; then
      log "Kafka is up."
      return 0
    fi
    sleep 2
  done
  echo "ERROR: Kafka did not become ready in time." >&2
  exit 1
}

create_topics() {
  for topic in "${TOPICS[@]}"; do
    log "Creating topic: ${topic} (3 partitions, RF=1)"
    docker compose exec -T kafka "${KAFKA_BIN}/kafka-topics.sh" \
      --bootstrap-server "${BOOTSTRAP}" \
      --create --if-not-exists \
      --topic "${topic}" \
      --partitions 3 \
      --replication-factor 1 || true
  done
}

start_producer() {
  local topic="$1"
  log "Starting background producer → ${topic}"
  # Write one message per second in a loop inside the container.
  docker compose exec -dT kafka bash -c "
    while true; do
      echo \"msg-\$(date +%s%N)\" | ${KAFKA_BIN}/kafka-console-producer.sh \
        --bootstrap-server ${BOOTSTRAP} --topic ${topic}
      sleep 0.3
    done
  "
}

start_consumer() {
  log "Starting consumer group '${GROUP}' on topic: orders"
  # Run the consumer in the background with a deliberate slow read to create visible lag.
  docker compose exec -dT kafka bash -c "
    ${KAFKA_BIN}/kafka-console-consumer.sh \
      --bootstrap-server ${BOOTSTRAP} \
      --topic orders \
      --group ${GROUP} \
      --from-beginning 2>/dev/null &
    # Also consume from payments and ads-events at a slower pace to show lag.
    for t in payments ads-events; do
      ${KAFKA_BIN}/kafka-console-consumer.sh \
        --bootstrap-server ${BOOTSTRAP} \
        --topic \$t \
        --group ${GROUP} 2>/dev/null &
    done
    wait
  "
}

main() {
  wait_for_kafka
  create_topics

  for topic in "${TOPICS[@]}"; do
    start_producer "${topic}"
  done

  # Brief pause so producers publish a few messages before the consumer starts,
  # ensuring non-zero lag is visible immediately.
  sleep 3

  start_consumer

  log "Done. Run 'make run' to launch KafkaScope."
  log "Topics: ${TOPICS[*]}"
  log "Consumer group: ${GROUP}"
}

main
