# OpenCode Tmux YAML Configuration

The tmux orchestrator reads its layout configuration from a YAML file. By default the loader looks for `~/.opencode/tmux.yaml`. Set `OPENCODE_TMUX_CONFIG` to override the location.

## Schema Overview

```yaml
version: "1.0"
mode: "raw"

session:
  name: "opencode-support"

panels:
  - id: "sessions"
    type: "sessions"
    width: "22%"
  - id: "messages"
    type: "messages"
  - id: "input"
    type: "input"
    height: "20%"

splits:
  - type: "horizontal"
    target: "root"
    panels: ["sessions", "messages"]
  - type: "vertical"
    target: "messages"
    panels: ["messages", "input"]
    ratio: "4:1"
```

### Fields

- `session.name` – overrides the tmux session name unless provided on the command line.
- `panels` – describes each pane. `type` accepts `sessions`, `messages`, `input`. `width` and `height` accept tmux-friendly values (e.g. `80`, `20%`). `command` (optional) runs a custom program instead of a built-in panel.
- `splits` – defines how panes are split (first ID stays on the original pane, second ID becomes the newly created pane):
  - `type`: `horizontal` (left/right) or `vertical` (top/bottom).
  - `target`: identifier of the pane to split. `root` refers to the initial window.
  - `panels`: two panel IDs; the first reuses the existing pane, the second becomes the newly created pane.
  - `ratio`: optional `A:B` string to size the first pane relative to the second (e.g. `4:1` keeps the first pane at ~80%).

## Defaults

If the YAML file is missing or empty, the orchestrator falls back to the classic layout:

1. Horizontal split of `root` into `sessions` (left) and `messages` (right).
2. Vertical split of `messages` into `messages` (top) and `input` (bottom).

Widths and heights default to `20%` for the side (`sessions`) and bottom (`input`) panes.

## Notes

- Missing panels or invalid references are ignored during layout creation.
- Custom `command` entries run as-is inside tmux after the environment variables are exported.
- Set `OPENCODE_SERVER` before launching the orchestrator; the loader forwards it to all panes automatically.
