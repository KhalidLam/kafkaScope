package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lamsadikhalid/kafkascope/internal/kafka"
)

// TopicsModel renders the topics table (tab 1).
type TopicsModel struct {
	tbl       table.Model
	filter    textinput.Model
	filtering bool
	snapshot  *kafka.ClusterSnapshot
	width     int
	height    int
}

func NewTopicsModel() TopicsModel {
	ti := textinput.New()
	ti.Placeholder = "filter topics…"
	ti.CharLimit = 60

	t := table.New(
		table.WithColumns(topicCols(80)),
		table.WithFocused(true),
		table.WithHeight(10),
	)
	t.SetStyles(DefaultTableStyles())

	return TopicsModel{tbl: t, filter: ti}
}

// topicCols computes column widths to fill terminal width w.
func topicCols(w int) []table.Column {
	fixed := 10 + 4 + 14 + 10 // Partitions + RF + Messages + msg/s
	nameW := w - fixed - 6    // 6 for separators / padding
	if nameW < 20 {
		nameW = 20
	}
	return []table.Column{
		{Title: "Topic", Width: nameW},
		{Title: "Partitions", Width: 10},
		{Title: "RF", Width: 4},
		{Title: "Messages", Width: 14},
		{Title: "msg/s", Width: 10},
	}
}

// SetSize is called on terminal resize.
func (m TopicsModel) SetSize(w, h int) TopicsModel {
	m.width = w
	m.height = h
	m.tbl.SetColumns(topicCols(w))
	tableH := h - 2 // reserve 2 lines for filter row
	if tableH < 2 {
		tableH = 2
	}
	m.tbl.SetHeight(tableH)
	return m
}

// SetSnapshot updates data and rebuilds rows.
func (m TopicsModel) SetSnapshot(snap *kafka.ClusterSnapshot) TopicsModel {
	m.snapshot = snap
	return m.rebuildRows()
}

func (m TopicsModel) rebuildRows() TopicsModel {
	if m.snapshot == nil {
		m.tbl.SetRows(nil)
		return m
	}

	filterStr := strings.ToLower(m.filter.Value())
	rows := make([]table.Row, 0, len(m.snapshot.Topics))

	for _, t := range m.snapshot.Topics {
		if filterStr != "" && !strings.Contains(strings.ToLower(t.Name), filterStr) {
			continue
		}

		msgps := ""
		if t.MsgPerSec > 0 {
			msgps = fmt.Sprintf("%.1f", t.MsgPerSec)
		}

		rows = append(rows, table.Row{
			t.Name,
			fmt.Sprintf("%d", len(t.Partitions)),
			fmt.Sprintf("%d", t.ReplicationFactor),
			fmt.Sprintf("%d", t.TotalMessages),
			msgps,
		})
	}

	m.tbl.SetRows(rows)
	return m
}

// Update handles key messages for the topics view.
// Returns (updated model, cmd, selected topic name for drill-down — empty if none).
func (m TopicsModel) Update(msg tea.Msg) (TopicsModel, tea.Cmd, string) {
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
				return m, nil, ""
			default:
				var cmd tea.Cmd
				m.filter, cmd = m.filter.Update(msg)
				m = m.rebuildRows()
				return m, cmd, ""
			}
		}

		switch msg.String() {
		case "/":
			m.filtering = true
			m.filter.Focus()
			return m, textinput.Blink, ""
		case "esc":
			if m.filter.Value() != "" {
				m.filter.Reset()
				m = m.rebuildRows()
			}
			return m, nil, ""
		case "enter":
			row := m.tbl.SelectedRow()
			if len(row) > 0 {
				return m, nil, row[0] // drill-down to partitions
			}
		default:
			var cmd tea.Cmd
			m.tbl, cmd = m.tbl.Update(msg)
			return m, cmd, ""
		}
	}

	var cmd tea.Cmd
	m.tbl, cmd = m.tbl.Update(msg)
	return m, cmd, ""
}

func (m TopicsModel) View() string {
	var b strings.Builder

	if m.filtering {
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
