package styles

import (
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/charmbracelet/lipgloss/v2/compat"
)

const (
	Top    = lipgloss.Top
	Bottom = lipgloss.Bottom
	Center = lipgloss.Center
	Left   = lipgloss.Left
	Right  = lipgloss.Right
)

var (
	NormalBorder   = lipgloss.NormalBorder()
	RoundedBorder  = lipgloss.RoundedBorder()
	DoubleBorder   = lipgloss.DoubleBorder()
	ThickBorder    = lipgloss.ThickBorder()
	HiddenBorder   = lipgloss.HiddenBorder()
)

func WhitespaceStyle(bg compat.AdaptiveColor) lipgloss.WhitespaceOption {
	return lipgloss.WithWhitespaceStyle(NewStyle().Background(bg).Lipgloss())
}
