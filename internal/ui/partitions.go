package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lamsadikhalid/kafkascope/internal/kafka"
)

// PartitionsModel renders the partition detail view (tab 3).
// It shows all partitions for a selected topic, or all partitions when no topic
// is pinned (e.g. direct tab navigation).
type PartitionsModel struct {
	tbl       table.Model
	filter    textinput.Model
	filtering bool
	selTopic  string // empty = show all
	snapshot  *kafka.ClusterSnapshot
	width     int
	height    int
}

func NewPartitionsModel() PartitionsModel {
	ti := textinput.New()
	ti.Placeholder = "filter…"
	ti.CharLimit = 60

	t := table.New(
		table.WithColumns(partitionCols(80)),
		table.WithFocused(true),
		table.WithHeight(10),
	)
	t.SetStyles(DefaultTableStyles())

	return PartitionsModel{tbl: t, filter: ti}
}

func partitionCols(w int) []table.Column {
	fixed := 10 + 8 + 12 + 12 + 12 // Part + Leader + ISR + Log-End + Oldest
	replicaW := w - fixed - 8
	if replicaW < 12 {
		replicaW = 12
	}
	return []table.Column{
		{Title: "Partition", Width: 10},
		{Title: "Leader", Width: 8},
		{Title: "Replicas", Width: replicaW},
		{Title: "ISR", Width: 12},
		{Title: "Log-End", Width: 12},
		{Title: "Oldest", Width: 12},
	}
}

func (m PartitionsModel) SetSize(w, h int) PartitionsModel {
	m.width = w
	m.height = h
	tableH := h - 2
	if tableH < 2 {
		tableH = 2
	}
	m.tbl.SetColumns(partitionCols(w))
	m.tbl.SetHeight(tableH)
	return m
}

// SetTopic pins a specific topic and rebuilds rows from the current snapshot.
func (m PartitionsModel) SetTopic(topic string, snap *kafka.ClusterSnapshot) PartitionsModel {
	m.selTopic = topic
	m.snapshot = snap
	return m.rebuildRows()
}

func (m PartitionsModel) SetSnapshot(snap *kafka.ClusterSnapshot) PartitionsModel {
	m.snapshot = snap
	return m.rebuildRows()
}

func int32SliceStr(ids []int32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}

func (m PartitionsModel) rebuildRows() PartitionsModel {
	if m.snapshot == nil {
		m.tbl.SetRows(nil)
		return m
	}

	filterStr := strings.ToLower(m.filter.Value())
	var rows []table.Row

	for _, t := range m.snapshot.Topics {
		if m.selTopic != "" && t.Name != m.selTopic {
			continue
		}
		if filterStr != "" && !strings.Contains(strings.ToLower(t.Name), filterStr) {
			continue
		}
		for _, p := range t.Partitions {
			rows = append(rows, table.Row{
				fmt.Sprintf("%d", p.ID),
				fmt.Sprintf("%d", p.Leader),
				int32SliceStr(p.Replicas),
				int32SliceStr(p.ISR),
				fmt.Sprintf("%d", p.LogEndOffset),
				fmt.Sprintf("%d", p.OldestOffset),
			})
		}
	}

	m.tbl.SetRows(rows)
	return m
}

func (m PartitionsModel) Update(msg tea.Msg) (PartitionsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.filtering {
			switch msg.String() {
			case "esc", "enter":
				m.filtering = false
				m.filter.Blur()
				if msg.String() == "esc" {
					m.filter.Reset()
					m = m.rebuildRows()
				}
				return m, nil
			default:
				var cmd tea.Cmd
				m.filter, cmd = m.filter.Update(msg)
				m = m.rebuildRows()
				return m, cmd
			}
		}

		switch msg.String() {
		case "/":
			m.filtering = true
			m.filter.Focus()
			return m, textinput.Blink
		case "esc":
			if m.selTopic != "" {
				m.selTopic = ""
				m = m.rebuildRows()
				return m, nil
			}
			if m.filter.Value() != "" {
				m.filter.Reset()
				m = m.rebuildRows()
			}
			return m, nil
		default:
			var cmd tea.Cmd
			m.tbl, cmd = m.tbl.Update(msg)
			return m, cmd
		}
	}

	var cmd tea.Cmd
	m.tbl, cmd = m.tbl.Update(msg)
	return m, cmd
}

func (m PartitionsModel) View() string {
	var b strings.Builder

	if m.selTopic != "" {
		b.WriteString(sectionTitleStyle.Render("Topic: "+m.selTopic) + "  esc to show all\n")
	} else if m.filtering {
		b.WriteString(filterPromptStyle.Render("/ "))
		b.WriteString(m.filter.View())
		b.WriteString("\n")
	} else if m.filter.Value() != "" {
		b.WriteString(filterPromptStyle.Render("filter: "))
		b.WriteString(filterActiveStyle.Render(m.filter.Value()))
		b.WriteString("  esc to clear\n")
	} else {
		b.WriteString("\n")
	}

	b.WriteString(m.tbl.View())
	return b.String()
}
