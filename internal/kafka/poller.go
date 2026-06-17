package kafka

import (
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// SnapshotMsg is delivered to the Tea program each time a fresh snapshot is ready.
type SnapshotMsg struct {
	Snapshot *ClusterSnapshot
}

// ErrMsg is delivered when a polling cycle fails.
type ErrMsg struct {
	Err error
}

// Sender is satisfied by *tea.Program (and by test doubles).
type Sender interface {
	Send(msg tea.Msg)
}

// Poller owns all Kafka I/O. It pushes SnapshotMsg / ErrMsg to the program
// via program.Send — the UI never calls Kafka directly.
type Poller struct {
	client    ClusterClient
	interval  time.Duration
	sender    Sender
	refreshCh <-chan struct{}
}

// NewPoller creates a Poller. refreshCh receives a token whenever the UI
// requests an out-of-band refresh (e.g. user presses 'r').
func NewPoller(client ClusterClient, interval time.Duration, sender Sender, refreshCh <-chan struct{}) *Poller {
	return &Poller{
		client:    client,
		interval:  interval,
		sender:    sender,
		refreshCh: refreshCh,
	}
}

// Start runs the polling loop. Call it with go poller.Start().
func (p *Poller) Start() {
	var prev *ClusterSnapshot
	p.fetch(&prev) // immediate first fetch before the ticker fires

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.fetch(&prev)
		case <-p.refreshCh:
			p.fetch(&prev)
			ticker.Reset(p.interval) // restart the interval so we don't double-poll
		}
	}
}

func (p *Poller) fetch(prev **ClusterSnapshot) {
	snap, err := p.client.FetchSnapshot()
	if err != nil {
		p.sender.Send(ErrMsg{Err: err})
		return
	}
	snap = EnrichWithThroughput(*prev, snap)
	*prev = snap
	p.sender.Send(SnapshotMsg{Snapshot: snap})
}
