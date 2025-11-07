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
  - id: "messages"
    type: "messages"
  - id: "input"
    type: "input"

splits:
  - type: "horizontal"
    target: "root"
    panels: ["sessions", "messages"]
    ratio: "1:4"
  - type: "vertical"
    target: "messages"
    panels: ["messages", "input"]
    ratio: "4:1"
```

### Fields

- `session.name` – overrides the tmux session name unless provided on the command line.
- `panels` – describes each pane. Each panel requires:
  - `id`: unique identifier for the panel
  - `type`: panel type, accepts `sessions`, `messages`, `input`, `shell`
    - `sessions`: Session list panel (shows all OpenCode sessions)
    - `messages`: Message display panel (shows conversation messages)
    - `input`: Input panel (user input area)
- `splits` – defines how panes are split (first ID stays on the original pane, second ID becomes the newly created pane):
  - `type`: `horizontal` (left/right) or `vertical` (top/bottom)
  - `target`: identifier of the pane to split. `root` refers to the initial window
  - `panels`: two panel IDs; the first reuses the existing pane, the second becomes the newly created pane
  - `ratio`: (optional) `A:B` format to control pane sizes relative to each other (e.g. `1:4` makes the first pane 20% and second pane 80%; `4:1` makes the first pane 80% and second pane 20%)

## Defaults

If the YAML file is missing or empty, the orchestrator uses the following default configuration:

```yaml
version: "1.0"
mode: "raw"

session:
  name: "opencode"

panels:
  - id: "sessions"
    type: "sessions"
  - id: "messages"
    type: "messages"
  - id: "input"
    type: "input"

splits:
  - type: "horizontal"
    target: "root"
    panels: ["sessions", "messages"]
  - type: "vertical"
    target: "messages"
    panels: ["messages", "input"]
```

This creates a classic three-panel layout:
1. Horizontal split of `root` into `sessions` (left, 20%) and `messages` (right, 80%)
2. Vertical split of `messages` into `messages` (top, 80%) and `input` (bottom, 20%)

