package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/lamsadikhalid/kafkascope/internal/kafka"
)

// Tab indices for the four views.
type Tab int

const (
	TabTopics Tab = iota
	TabGroups
	TabPartitions
	TabBrokers
	numTabs
)

var tabNames = [numTabs]string{"Topics", "Groups", "Partitions", "Brokers"}

// AppModel is the root bubbletea model. It owns routing, the global header/footer,
// and delegates key events and data to the four sub-views.
type AppModel struct {
	brokerAddr      string
	refreshInterval time.Duration
	refreshCh       chan<- struct{}

	activeTab Tab
	width     int
	height    int

	topics     TopicsModel
	groups     GroupsModel
	partitions PartitionsModel
	brokers    BrokersModel

	snapshot    *kafka.ClusterSnapshot
	lastRefresh time.Time
	lastErr     error
	isPolling   bool

	spinner spinner.Model
}

// NewApp creates the root model.
// refreshCh is the send side of the poller's force-refresh channel.
func NewApp(brokerAddr string, refreshInterval time.Duration, refreshCh chan<- struct{}) AppModel {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(clrAccent)

	return AppModel{
		brokerAddr:      brokerAddr,
		refreshInterval: refreshInterval,
		refreshCh:       refreshCh,
		activeTab:       TabTopics,
		isPolling:       true,
		spinner:         sp,
		topics:          NewTopicsModel(),
		groups:          NewGroupsModel(),
		partitions:      NewPartitionsModel(),
		brokers:         NewBrokersModel(),
	}
}

func (m AppModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		h := m.contentHeight()
		m.topics = m.topics.SetSize(m.width, h)
		m.groups = m.groups.SetSize(m.width, h)
		m.partitions = m.partitions.SetSize(m.width, h)
		m.brokers = m.brokers.SetSize(m.width, h)

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)

	case kafka.SnapshotMsg:
		m.snapshot = msg.Snapshot
		m.lastRefresh = msg.Snapshot.CollectedAt
		m.lastErr = nil
		m.isPolling = false
		m.topics = m.topics.SetSnapshot(msg.Snapshot)
		m.groups = m.groups.SetSnapshot(msg.Snapshot)
		m.partitions = m.partitions.SetSnapshot(msg.Snapshot)
		m.brokers = m.brokers.SetSnapshot(msg.Snapshot)

	case kafka.ErrMsg:
		m.lastErr = msg.Err
		m.isPolling = false

	case tea.KeyMsg:
		// Global shortcuts — only active when no sub-view is filtering.
		if !m.anyFiltering() {
			switch msg.String() {
			case "q", "ctrl+c":
				return m, tea.Quit
			case "r":
				m.isPolling = true
				select {
				case m.refreshCh <- struct{}{}:
				default:
				}
				cmds = append(cmds, m.spinner.Tick)
				return m, tea.Batch(cmds...)
			case "1":
				m.activeTab = TabTopics
				return m, tea.Batch(cmds...)
			case "2":
				m.activeTab = TabGroups
				return m, tea.Batch(cmds...)
			case "3":
				m.activeTab = TabPartitions
				return m, tea.Batch(cmds...)
			case "4":
				m.activeTab = TabBrokers
				return m, tea.Batch(cmds...)
			case "tab":
				m.activeTab = (m.activeTab + 1) % numTabs
				return m, tea.Batch(cmds...)
			case "shift+tab":
				m.activeTab = (m.activeTab + numTabs - 1) % numTabs
				return m, tea.Batch(cmds...)
			}
		}

		// Delegate to the active sub-view.
		switch m.activeTab {
		case TabTopics:
			newTopics, cmd, drillTopic := m.topics.Update(msg)
			m.topics = newTopics
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
			if drillTopic != "" {
				m.partitions = m.partitions.SetTopic(drillTopic, m.snapshot)
				m.activeTab = TabPartitions
			}
		case TabGroups:
			newGroups, cmd := m.groups.Update(msg)
			m.groups = newGroups
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		case TabPartitions:
			newParts, cmd := m.partitions.Update(msg)
			m.partitions = newParts
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		case TabBrokers:
			newBrokers, cmd := m.brokers.Update(msg)
			m.brokers = newBrokers
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
	}

	return m, tea.Batch(cmds...)
}

func (m AppModel) View() string {
	if m.width == 0 {
		return "Connecting…\n"
	}
	return strings.Join([]string{
		m.renderHeader(),
		m.renderTabs(),
		m.renderContent(),
		m.renderFooter(),
	}, "\n")
}

func (m AppModel) renderHeader() string {
	left := titleStyle.Render("KafkaScope")

	brokerCount := 0
	if m.snapshot != nil {
		brokerCount = len(m.snapshot.Brokers)
	}

	refreshStr := "connecting…"
	if !m.lastRefresh.IsZero() {
		refreshStr = m.lastRefresh.Format("15:04:05")
	}

	spinStr := ""
	if m.isPolling {
		spinStr = " " + m.spinner.View()
	}

	right := infoStyle.Render(fmt.Sprintf(
		"%s  brokers: %d  refreshed: %s%s",
		m.brokerAddr, brokerCount, refreshStr, spinStr,
	))

	header := lipgloss.JoinHorizontal(lipgloss.Top, left, right)

	if m.lastErr != nil {
		banner := errorBannerStyle.Width(m.width).Render(
			"  ✗ disconnected — retrying: " + m.lastErr.Error(),
		)
		return lipgloss.JoinVertical(lipgloss.Left, header, banner)
	}
	return header
}

func (m AppModel) renderTabs() string {
	parts := make([]string, numTabs)
	for i := Tab(0); i < numTabs; i++ {
		label := fmt.Sprintf("[%d] %s", i+1, tabNames[i])
		if i == m.activeTab {
			parts[i] = activeTabStyle.Render(label)
		} else {
			parts[i] = inactiveTabStyle.Render(label)
		}
	}
	line := strings.Join(parts, " ")
	return tabBorderStyle.Width(m.width).Render(line)
}

func (m AppModel) renderContent() string {
	switch m.activeTab {
	case TabTopics:
		return m.topics.View()
	case TabGroups:
		return m.groups.View()
	case TabPartitions:
		return m.partitions.View()
	case TabBrokers:
		return m.brokers.View()
	}
	return ""
}

func (m AppModel) renderFooter() string {
	help := "q quit  r refresh  / filter  esc back/clear  ↑↓ / jk navigate  tab/shift-tab switch  enter drill-down"
	return footerStyle.Width(m.width).Render(help)
}

// contentHeight is terminal height minus the fixed chrome lines.
func (m AppModel) contentHeight() int {
	// Header(1 or 2) + blank + tabs(2) + blank + footer(2) ≈ 8 lines worst-case
	overhead := 8
	if m.lastErr != nil {
		overhead++ // extra banner line
	}
	h := m.height - overhead
	if h < 4 {
		h = 4
	}
	return h
}

func (m AppModel) anyFiltering() bool {
	return m.topics.filtering ||
		m.groups.filtering ||
		m.partitions.filtering ||
		m.brokers.filtering
}
