package kafka

import (
	"testing"
	"time"
)

func TestComputeLag(t *testing.T) {
	tests := []struct {
		name      string
		committed int64
		logEnd    int64
		want      int64
	}{
		{"no commit returns -1", -1, 100, -1},
		{"exact match returns 0", 50, 50, 0},
		{"normal lag", 40, 50, 10},
		{"consumer ahead clamps to 0", 60, 50, 0},
		{"zero committed zero logEnd", 0, 0, 0},
		{"zero committed with messages", 0, 100, 100},
		{"large lag", 0, 1_000_000, 1_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeLag(tt.committed, tt.logEnd)
			if got != tt.want {
				t.Errorf("ComputeLag(%d, %d) = %d; want %d",
					tt.committed, tt.logEnd, got, tt.want)
			}
		})
	}
}

func TestComputeThroughput(t *testing.T) {
	makePartitions := func(offsets ...int64) []PartitionInfo {
		ps := make([]PartitionInfo, len(offsets))
		for i, o := range offsets {
			ps[i] = PartitionInfo{ID: int32(i), LogEndOffset: o}
		}
		return ps
	}

	prev := &TopicInfo{Name: "test", Partitions: makePartitions(100, 200, 300)}
	curr := &TopicInfo{Name: "test", Partitions: makePartitions(110, 220, 330)}
	elapsed := time.Second

	got := ComputeThroughput(prev, curr, elapsed)
	// delta = (110+220+330) - (100+200+300) = 660 - 600 = 60 msg/s
	if got != 60.0 {
		t.Errorf("ComputeThroughput = %f; want 60.0", got)
	}

	// First snapshot (no previous) must return 0.
	if got := ComputeThroughput(nil, curr, elapsed); got != 0 {
		t.Errorf("first snapshot: ComputeThroughput(nil, ...) = %f; want 0", got)
	}

	// Zero elapsed must return 0.
	if got := ComputeThroughput(prev, curr, 0); got != 0 {
		t.Errorf("zero elapsed: ComputeThroughput = %f; want 0", got)
	}

	// No new messages returns 0.
	same := &TopicInfo{Name: "test", Partitions: makePartitions(100, 200, 300)}
	if got := ComputeThroughput(prev, same, elapsed); got != 0 {
		t.Errorf("no delta: ComputeThroughput = %f; want 0", got)
	}
}

func TestEnrichWithThroughput(t *testing.T) {
	t0 := time.Now()
	t1 := t0.Add(2 * time.Second)

	prev := &ClusterSnapshot{
		CollectedAt: t0,
		Topics: []TopicInfo{
			{Name: "orders", Partitions: []PartitionInfo{{ID: 0, LogEndOffset: 100}}},
		},
	}
	curr := &ClusterSnapshot{
		CollectedAt: t1,
		Topics: []TopicInfo{
			{Name: "orders", Partitions: []PartitionInfo{{ID: 0, LogEndOffset: 120}}},
			{Name: "new-topic", Partitions: []PartitionInfo{{ID: 0, LogEndOffset: 50}}},
		},
	}

	result := EnrichWithThroughput(prev, curr)

	// orders: delta=20 over 2s → 10 msg/s
	if result.Topics[0].MsgPerSec != 10.0 {
		t.Errorf("orders MsgPerSec = %f; want 10.0", result.Topics[0].MsgPerSec)
	}
	// new-topic has no prev baseline → 0
	if result.Topics[1].MsgPerSec != 0 {
		t.Errorf("new-topic MsgPerSec = %f; want 0", result.Topics[1].MsgPerSec)
	}

	// nil prev must be a no-op.
	result2 := EnrichWithThroughput(nil, curr)
	if result2 != curr {
		t.Error("EnrichWithThroughput(nil, curr) should return curr unchanged")
	}
}
