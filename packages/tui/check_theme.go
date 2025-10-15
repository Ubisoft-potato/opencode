package main

import (
	"fmt"
	"log"

	"github.com/sst/opencode/internal/theme"
)

func main() {
	// Load themes from JSON first
	if err := theme.LoadThemesFromJSON(); err != nil {
		log.Printf("Warning: Failed to load themes from JSON: %v", err)
	}

	// Load themes from directories (requires 3 parameters)
	if err := theme.LoadThemesFromDirectories("", "", ""); err != nil {
		log.Printf("Warning: Failed to load themes from directories: %v", err)
	}

	// Get current theme
	currentTheme := theme.CurrentThemeName()
	fmt.Printf("当前主题: %s\n\n", currentTheme)

	// List all available themes
	availableThemes := theme.AvailableThemes()
	fmt.Printf("可用主题 (%d个):\n", len(availableThemes))
	for i, themeName := range availableThemes {
		if themeName == currentTheme {
			fmt.Printf("  %d. %s (当前使用)\n", i+1, themeName)
		} else {
			fmt.Printf("  %d. %s\n", i+1, themeName)
		}
	}
	
	fmt.Println("\n推荐的舒适主题:")
	comfortableThemes := []string{"dracula", "gruvbox", "tokyonight", "catppuccin", "nord", "rosepine"}
	for _, comfortable := range comfortableThemes {
		for _, available := range availableThemes {
			if available == comfortable {
				if available == currentTheme {
					fmt.Printf("  • %s (当前使用)\n", available)
				} else {
					fmt.Printf("  • %s\n", available)
				}
				break
			}
		}
	}
}
