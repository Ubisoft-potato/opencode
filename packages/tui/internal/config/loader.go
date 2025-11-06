package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type File struct {
	Version string  `yaml:"version"`
	Mode    string  `yaml:"mode"`
	Session Session `yaml:"session"`
	Panels  []Panel `yaml:"panels"`
	Splits  []Split `yaml:"splits"`
	Raw     []byte  `yaml:"-"`
	path    string
}

type Session struct {
	Name string `yaml:"name"`
}

type Panel struct {
	ID      string `yaml:"id"`
	Type    string `yaml:"type"`
	Width   string `yaml:"width"`
	Height  string `yaml:"height"`
	Command string `yaml:"command"`
}

type Split struct {
	Type   string   `yaml:"type"`
	Target string   `yaml:"target"`
	Panels []string `yaml:"panels"`
	Ratio  string   `yaml:"ratio"`
}

func Default() *File {
	return &File{
		Version: "1.0",
		Mode:    "raw",
		Session: Session{Name: "opencode"},
		Panels: []Panel{
			{ID: "sessions", Type: "sessions", Width: "20%"},
			{ID: "messages", Type: "messages"},
			{ID: "input", Type: "input", Height: "20%"},
		},
		Splits: []Split{
			{Type: "horizontal", Target: "root", Panels: []string{"sessions", "messages"}},
			{Type: "vertical", Target: "messages", Panels: []string{"messages", "input"}},
		},
	}
}

func Load(path string) (*File, error) {
	cfg := Default()
	cfg.path = path

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read tmux config: %w", err)
	}

	cfg.Raw = data
	text := strings.TrimSpace(string(data))
	if text == "" {
		return cfg, nil
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse tmux config: %w", err)
	}

	cfg.ensureDefaults()
	return cfg, nil
}

func (f *File) ensureDefaults() {
	if f.Session.Name == "" {
		f.Session.Name = "opencode"
	}

	if len(f.Panels) == 0 {
		base := Default()
		f.Panels = base.Panels
	}

	if len(f.Splits) == 0 {
		base := Default()
		f.Splits = base.Splits
	}
}

func (f *File) RatioPercents(value string) (int, int, bool) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, 0, false
	}

	a, errA := strconv.Atoi(strings.TrimSpace(parts[0]))
	if errA != nil {
		return 0, 0, false
	}

	b, errB := strconv.Atoi(strings.TrimSpace(parts[1]))
	if errB != nil {
		return 0, 0, false
	}

	total := a + b
	if total == 0 {
		return 0, 0, false
	}

	first := a * 100 / total
	second := 100 - first
	return first, second, true
}
