package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lamsadikhalid/kafkascope/internal/kafka"
)

// BrokersModel renders the brokers table (tab 4).
type BrokersModel struct {
	tbl       table.Model
	filter    textinput.Model
	filtering bool
	snapshot  *kafka.ClusterSnapshot
	width     int
	height    int
}

func NewBrokersModel() BrokersModel {
	ti := textinput.New()
	ti.Placeholder = "filter…"
	ti.CharLimit = 60

	t := table.New(
		table.WithColumns(brokerCols(80)),
		table.WithFocused(true),
		table.WithHeight(10),
	)
	t.SetStyles(DefaultTableStyles())

	return BrokersModel{tbl: t, filter: ti}
}

func brokerCols(w int) []table.Column {
	fixed := 6 + 12 + 16 // ID + Controller + Rack
	addrW := w - fixed - 6
	if addrW < 20 {
		addrW = 20
	}
	return []table.Column{
		{Title: "ID", Width: 6},
		{Title: "Address", Width: addrW},
		{Title: "Controller", Width: 12},
		{Title: "Rack", Width: 16},
	}
}

func (m BrokersModel) SetSize(w, h int) BrokersModel {
	m.width = w
	m.height = h
	tableH := h - 2
	if tableH < 2 {
		tableH = 2
	}
	m.tbl.SetColumns(brokerCols(w))
	m.tbl.SetHeight(tableH)
	return m
}

func (m BrokersModel) SetSnapshot(snap *kafka.ClusterSnapshot) BrokersModel {
	m.snapshot = snap
	return m.rebuildRows()
}

func (m BrokersModel) rebuildRows() BrokersModel {
	if m.snapshot == nil {
		m.tbl.SetRows(nil)
		return m
	}

	filterStr := strings.ToLower(m.filter.Value())
	rows := make([]table.Row, 0, len(m.snapshot.Brokers))

	for _, b := range m.snapshot.Brokers {
		if filterStr != "" && !strings.Contains(strings.ToLower(b.Addr), filterStr) {
			continue
		}
		ctrl := "no"
		if b.IsController {
			ctrl = "yes ✓"
		}
		rows = append(rows, table.Row{
			fmt.Sprintf("%d", b.ID),
			b.Addr,
			ctrl,
			b.Rack,
		})
	}

	m.tbl.SetRows(rows)
	return m
}

func (m BrokersModel) Update(msg tea.Msg) (BrokersModel, tea.Cmd) {
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

func (m BrokersModel) View() string {
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
