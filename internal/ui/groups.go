package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/lamsadikhalid/kafkascope/internal/kafka"
)

// GroupsModel renders the consumer-groups table (tab 2) and its drill-down.
type GroupsModel struct {
	tbl        table.Model
	detailTbl  table.Model
	filter     textinput.Model
	filtering  bool
	showDetail bool
	selGroup   string
	snapshot   *kafka.ClusterSnapshot
	width      int
	height     int
}

func NewGroupsModel() GroupsModel {
	ti := textinput.New()
	ti.Placeholder = "filter groups…"
	ti.CharLimit = 60

	t := table.New(
		table.WithColumns(groupCols(80)),
		table.WithFocused(true),
		table.WithHeight(10),
	)
	t.SetStyles(DefaultTableStyles())

	dt := table.New(
		table.WithColumns(groupDetailCols(80)),
		table.WithFocused(true),
		table.WithHeight(10),
	)
	dt.SetStyles(DefaultTableStyles())

	return GroupsModel{tbl: t, detailTbl: dt, filter: ti}
}

func groupCols(w int) []table.Column {
	fixed := 12 + 9 + 12 // State + Members + TotalLag
	nameW := w - fixed - 6
	if nameW < 20 {
		nameW = 20
	}
	return []table.Column{
		{Title: "Group", Width: nameW},
		{Title: "State", Width: 12},
		{Title: "Members", Width: 9},
		{Title: "Total Lag", Width: 12},
	}
}

func groupDetailCols(w int) []table.Column {
	fixed := 10 + 14 + 14 + 10 // Partition + Committed + Log-End + Lag
	topicW := w - fixed - 6
	if topicW < 20 {
		topicW = 20
	}
	return []table.Column{
		{Title: "Topic", Width: topicW},
		{Title: "Partition", Width: 10},
		{Title: "Committed", Width: 14},
		{Title: "Log-End", Width: 14},
		{Title: "Lag", Width: 10},
	}
}

func (m GroupsModel) SetSize(w, h int) GroupsModel {
	m.width = w
	m.height = h
	tableH := h - 2
	if tableH < 2 {
		tableH = 2
	}
	m.tbl.SetColumns(groupCols(w))
	m.tbl.SetHeight(tableH)
	m.detailTbl.SetColumns(groupDetailCols(w))
	m.detailTbl.SetHeight(tableH - 1) // -1 for the detail header line
	return m
}

func (m GroupsModel) SetSnapshot(snap *kafka.ClusterSnapshot) GroupsModel {
	m.snapshot = snap
	m = m.rebuildRows()
	if m.showDetail {
		m = m.rebuildDetailRows()
	}
	return m
}

func (m GroupsModel) rebuildRows() GroupsModel {
	if m.snapshot == nil {
		m.tbl.SetRows(nil)
		return m
	}

	filterStr := strings.ToLower(m.filter.Value())
	rows := make([]table.Row, 0, len(m.snapshot.Groups))

	for _, g := range m.snapshot.Groups {
		if filterStr != "" && !strings.Contains(strings.ToLower(g.Name), filterStr) {
			continue
		}

		lagStr := fmt.Sprintf("%d", g.TotalLag)

		rows = append(rows, table.Row{
			g.Name,
			g.State,
			fmt.Sprintf("%d", g.Members),
			lagStr,
		})
	}

	m.tbl.SetRows(rows)
	return m
}

func (m GroupsModel) rebuildDetailRows() GroupsModel {
	if m.snapshot == nil {
		m.detailTbl.SetRows(nil)
		return m
	}

	var group *kafka.GroupInfo
	for i := range m.snapshot.Groups {
		if m.snapshot.Groups[i].Name == m.selGroup {
			group = &m.snapshot.Groups[i]
			break
		}
	}
	if group == nil {
		m.detailTbl.SetRows(nil)
		return m
	}

	rows := make([]table.Row, 0, len(group.Offsets))
	for _, o := range group.Offsets {
		lagStr := fmt.Sprintf("%d", o.Lag)
		if o.Lag < 0 {
			lagStr = "-"
		}
		rows = append(rows, table.Row{
			o.Topic,
			fmt.Sprintf("%d", o.Partition),
			fmt.Sprintf("%d", o.Committed),
			fmt.Sprintf("%d", o.LogEndOffset),
			lagStr,
		})
	}
	m.detailTbl.SetRows(rows)
	return m
}

// Update handles key messages. Returns (updated model, cmd).
func (m GroupsModel) Update(msg tea.Msg) (GroupsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.showDetail {
			switch msg.String() {
			case "esc", "q":
				if msg.String() == "esc" {
					m.showDetail = false
					m.selGroup = ""
					return m, nil
				}
			default:
				var cmd tea.Cmd
				m.detailTbl, cmd = m.detailTbl.Update(msg)
				return m, cmd
			}
		}

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
		case "enter":
			row := m.tbl.SelectedRow()
			if len(row) > 0 {
				m.selGroup = row[0]
				m.showDetail = true
				m = m.rebuildDetailRows()
				return m, nil
			}
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

func (m GroupsModel) View() string {
	var b strings.Builder

	if m.showDetail {
		b.WriteString(sectionTitleStyle.Render("Group: "+m.selGroup) + "  esc to go back\n")
		b.WriteString(m.detailTbl.View())
		return b.String()
	}

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
