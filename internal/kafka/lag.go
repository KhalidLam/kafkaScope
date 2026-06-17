// Package kafka contains pure math functions for lag and throughput.
// No sarama imports — this file is independently unit-testable.
package kafka

import "time"

// ComputeLag returns the consumer lag for a single partition.
//   - committed < 0  → returns -1 (no commit ever recorded; display "-")
//   - otherwise      → max(0, logEnd − committed)
func ComputeLag(committed, logEnd int64) int64 {
	if committed < 0 {
		return -1
	}
	if lag := logEnd - committed; lag > 0 {
		return lag
	}
	return 0
}

// ComputeThroughput returns messages/second for a topic between two snapshots.
// Returns 0 when prev is nil, elapsed ≤ 0, or the log-end delta is non-positive.
func ComputeThroughput(prev, curr *TopicInfo, elapsed time.Duration) float64 {
	if prev == nil || elapsed <= 0 {
		return 0
	}

	var prevTotal, currTotal int64
	for _, p := range prev.Partitions {
		if p.LogEndOffset > 0 {
			prevTotal += p.LogEndOffset
		}
	}
	for _, p := range curr.Partitions {
		if p.LogEndOffset > 0 {
			currTotal += p.LogEndOffset
		}
	}

	delta := currTotal - prevTotal
	if delta <= 0 {
		return 0
	}
	return float64(delta) / elapsed.Seconds()
}

// EnrichWithThroughput computes MsgPerSec for every topic in curr using prev
// as the baseline. Returns curr unchanged if either snapshot is nil.
func EnrichWithThroughput(prev, curr *ClusterSnapshot) *ClusterSnapshot {
	if prev == nil || curr == nil {
		return curr
	}
	elapsed := curr.CollectedAt.Sub(prev.CollectedAt)
	if elapsed <= 0 {
		return curr
	}

	prevByName := make(map[string]*TopicInfo, len(prev.Topics))
	for i := range prev.Topics {
		prevByName[prev.Topics[i].Name] = &prev.Topics[i]
	}

	for i := range curr.Topics {
		curr.Topics[i].MsgPerSec = ComputeThroughput(prevByName[curr.Topics[i].Name], &curr.Topics[i], elapsed)
	}
	return curr
}
