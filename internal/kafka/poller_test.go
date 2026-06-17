package kafka

import (
	"errors"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// fakeSender records messages instead of sending them to a real Tea program.
type fakeSender struct {
	mu   sync.Mutex
	msgs []tea.Msg
}

func (f *fakeSender) Send(msg tea.Msg) {
	f.mu.Lock()
	f.msgs = append(f.msgs, msg)
	f.mu.Unlock()
}

func (f *fakeSender) received() []tea.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]tea.Msg, len(f.msgs))
	copy(cp, f.msgs)
	return cp
}

// fakeClient implements ClusterClient for testing.
type fakeClient struct {
	snap *ClusterSnapshot
	err  error
}

func (f *fakeClient) FetchSnapshot() (*ClusterSnapshot, error) { return f.snap, f.err }
func (f *fakeClient) Close() error                             { return nil }

func TestPollerEmitsSnapshot(t *testing.T) {
	snap := &ClusterSnapshot{
		CollectedAt: time.Now(),
		Brokers:     []BrokerInfo{{ID: 1, Addr: "localhost:9092"}},
	}
	client := &fakeClient{snap: snap}
	sender := &fakeSender{}
	refreshCh := make(chan struct{}, 1)

	p := NewPoller(client, 10*time.Second, sender, refreshCh)

	// Run one immediate fetch (what Start does at launch).
	var prev *ClusterSnapshot
	p.fetch(&prev)

	msgs := sender.received()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message; got %d", len(msgs))
	}
	sm, ok := msgs[0].(SnapshotMsg)
	if !ok {
		t.Fatalf("expected SnapshotMsg; got %T", msgs[0])
	}
	if sm.Snapshot == nil {
		t.Fatal("SnapshotMsg.Snapshot is nil")
	}
	if sm.Snapshot.Brokers[0].ID != 1 {
		t.Errorf("broker ID = %d; want 1", sm.Snapshot.Brokers[0].ID)
	}
}

func TestPollerEmitsErrMsg(t *testing.T) {
	client := &fakeClient{err: errors.New("broker unreachable")}
	sender := &fakeSender{}
	refreshCh := make(chan struct{}, 1)

	p := NewPoller(client, 10*time.Second, sender, refreshCh)

	var prev *ClusterSnapshot
	p.fetch(&prev)

	msgs := sender.received()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message; got %d", len(msgs))
	}
	em, ok := msgs[0].(ErrMsg)
	if !ok {
		t.Fatalf("expected ErrMsg; got %T", msgs[0])
	}
	if em.Err == nil {
		t.Fatal("ErrMsg.Err is nil")
	}
}

func TestPollerThroughputEnrichedOnSecondFetch(t *testing.T) {
	t0 := time.Now()

	snap1 := &ClusterSnapshot{
		CollectedAt: t0,
		Topics: []TopicInfo{
			{Name: "orders", Partitions: []PartitionInfo{{ID: 0, LogEndOffset: 100}}},
		},
	}
	snap2 := &ClusterSnapshot{
		CollectedAt: t0.Add(2 * time.Second),
		Topics: []TopicInfo{
			{Name: "orders", Partitions: []PartitionInfo{{ID: 0, LogEndOffset: 120}}},
		},
	}

	calls := 0
	snaps := []*ClusterSnapshot{snap1, snap2}
	client := &callbackClient{fn: func() (*ClusterSnapshot, error) {
		s := snaps[calls]
		calls++
		return s, nil
	}}
	sender := &fakeSender{}
	refreshCh := make(chan struct{}, 1)
	p := NewPoller(client, 10*time.Second, sender, refreshCh)

	var prev *ClusterSnapshot
	p.fetch(&prev) // first: no prev, throughput = 0
	p.fetch(&prev) // second: enrich with delta

	msgs := sender.received()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages; got %d", len(msgs))
	}

	sm2 := msgs[1].(SnapshotMsg)
	if sm2.Snapshot.Topics[0].MsgPerSec == 0 {
		t.Error("expected non-zero MsgPerSec on second snapshot")
	}
}

type callbackClient struct {
	fn func() (*ClusterSnapshot, error)
}

func (c *callbackClient) FetchSnapshot() (*ClusterSnapshot, error) { return c.fn() }
func (c *callbackClient) Close() error                             { return nil }
