package ui

import (
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
)

// Colour palette — one accent, subtle borders, semantic red/green.
const (
	clrAccent    = lipgloss.Color("#7C3AED")
	clrBorder    = lipgloss.Color("#374151")
	clrFg        = lipgloss.Color("#E4E4E4")
	clrDim       = lipgloss.Color("#6B7280")
	clrGreen     = lipgloss.Color("#10B981")
	clrRed       = lipgloss.Color("#EF4444")
	clrYellow    = lipgloss.Color("#F59E0B")
	clrSelected  = lipgloss.Color("#4C1D95")
	clrHeaderBg  = lipgloss.Color("#1E1B4B")
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(clrAccent).
			Padding(0, 2)

	infoStyle = lipgloss.NewStyle().
			Foreground(clrDim).
			Padding(0, 1)

	activeTabStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(clrAccent).
			Underline(true).
			Padding(0, 1)

	inactiveTabStyle = lipgloss.NewStyle().
				Foreground(clrDim).
				Padding(0, 1)

	tabBorderStyle = lipgloss.NewStyle().
			BorderBottom(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(clrBorder)

	footerStyle = lipgloss.NewStyle().
			Foreground(clrDim).
			BorderTop(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(clrBorder)

	errorBannerStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(clrRed).
				Padding(0, 1)

	filterPromptStyle = lipgloss.NewStyle().
				Foreground(clrYellow).
				Bold(true)

	filterActiveStyle = lipgloss.NewStyle().
				Foreground(clrYellow)

	msgPerSecStyle = lipgloss.NewStyle().
			Foreground(clrGreen).
			Bold(true)

	lagHighStyle = lipgloss.NewStyle().
			Foreground(clrRed).
			Bold(true)

	controllerStyle = lipgloss.NewStyle().
			Foreground(clrGreen).
			Bold(true)

	sectionTitleStyle = lipgloss.NewStyle().
				Foreground(clrAccent).
				Bold(true).
				Padding(0, 0, 0, 1)
)

// DefaultTableStyles returns the lipgloss table styles used across all views.
func DefaultTableStyles() table.Styles {
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(clrBorder).
		BorderBottom(true).
		Foreground(clrDim).
		Background(clrHeaderBg).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(clrFg).
		Background(clrSelected).
		Bold(false)
	s.Cell = s.Cell.
		Foreground(clrFg)
	return s
}
