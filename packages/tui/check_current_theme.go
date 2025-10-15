package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// State represents the application state structure
type State struct {
	Theme string `json:"theme"`
}

func main() {
	// Get the state file path
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to get home directory: %v", err)
	}
	
	stateFilePath := filepath.Join(homeDir, ".opencode", "state.json")
	
	// Read the state file
	file, err := os.Open(stateFilePath)
	if err != nil {
		log.Fatalf("Failed to open state file: %v", err)
	}
	defer file.Close()
	
	// The state file contains multiple JSON objects
	// We need to find the one that contains the theme field
	scanner := bufio.NewScanner(file)
	var jsonBuffer strings.Builder
	var braceCount int
	var foundTheme string
	
	for scanner.Scan() {
		line := scanner.Text()
		jsonBuffer.WriteString(line)
		
		// Count braces to detect complete JSON objects
		for _, char := range line {
			if char == '{' {
				braceCount++
			} else if char == '}' {
				braceCount--
				if braceCount == 0 {
					// We have a complete JSON object
					jsonStr := jsonBuffer.String()
					var state State
					if err := json.Unmarshal([]byte(jsonStr), &state); err == nil {
						if state.Theme != "" {
							foundTheme = state.Theme
							// Continue reading to get the latest theme setting
						}
					}
					// Reset buffer for next JSON object
					jsonBuffer.Reset()
				}
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading state file: %v", err)
	}
	
	// Display the current theme
	if foundTheme == "" {
		fmt.Println("当前主题: 未设置 (使用默认主题)")
	} else {
		fmt.Printf("当前主题: %s\n", foundTheme)
	}
	
	fmt.Println("\n注意: 这是从状态文件直接读取的主题设置")
	fmt.Println("如果主题显示不正确，可能是因为主题系统需要重新加载")
}