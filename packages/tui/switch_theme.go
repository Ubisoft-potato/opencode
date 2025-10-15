package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sst/opencode/internal/ipc"
	"github.com/sst/opencode/internal/theme"
	"github.com/sst/opencode/internal/types"
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
		for i, available := range availableThemes {
			if available == comfortable {
				if available == currentTheme {
					fmt.Printf("  %d. %s (当前使用)\n", i+1, available)
				} else {
					fmt.Printf("  %d. %s (推荐)\n", i+1, available)
				}
				break
			}
		}
	}

	// Ask user to select a theme
	fmt.Print("\n请选择一个主题 (输入数字): ")
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("读取输入失败: %v", err)
	}

	input = strings.TrimSpace(input)
	choice, err := strconv.Atoi(input)
	if err != nil || choice < 1 || choice > len(availableThemes) {
		fmt.Printf("无效选择: %s\n", input)
		return
	}

	selectedTheme := availableThemes[choice-1]
	if selectedTheme == currentTheme {
		fmt.Printf("已经在使用主题 '%s'\n", selectedTheme)
		return
	}

	// Apply the selected theme locally
	if err := theme.SetTheme(selectedTheme); err != nil {
		log.Fatalf("切换主题失败: %v", err)
	}

	// Connect to IPC socket and send theme change event
	socketPath := os.Getenv("OPENCODE_SOCKET")
	if socketPath == "" {
		homeDir, _ := os.UserHomeDir()
		socketPath = filepath.Join(homeDir, ".opencode", "ipc.sock")
	}
	
	client := ipc.NewSocketClient(socketPath, "theme-switcher", "tool")
	
	err = client.Connect()
	if err != nil {
		fmt.Printf("Warning: Failed to connect to IPC socket: %v\n", err)
		return
	}
	defer client.Disconnect()
	
	// Send theme change event
	themePayload := types.ThemeChangePayload{
		Theme: selectedTheme,
	}
	
	update := types.StateUpdate{
		Type:      types.ThemeChanged,
		Timestamp: time.Now(),
		Payload:   themePayload,
	}
	
	_, err = client.SendStateUpdateAndWait(update)
	if err != nil {
		fmt.Printf("Warning: Failed to send theme change event: %v\n", err)
	} else {
		fmt.Printf("Theme change event sent successfully\n")
	}

	fmt.Printf("成功切换到主题: %s\n", selectedTheme)
	fmt.Println("主题已应用！请查看TUI界面的变化。")
}