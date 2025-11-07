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
    - `shell`: Interactive shell panel (launches user's default shell, useful for running commands)
  - `command`: (optional) custom command to run in this pane instead of default panel app
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

## Example: Adding a Shell Panel

You can add a shell panel to your layout for running commands alongside OpenCode panels:

```yaml
version: "1.0"
mode: "raw"

session:
  name: "opencode-dev"

panels:
  - id: "sessions"
    type: "sessions"
  - id: "messages"
    type: "messages"
  - id: "input"
    type: "input"
  - id: "terminal"
    type: "shell"

splits:
  - type: "horizontal"
    target: "root"
    panels: ["sessions", "messages"]
    ratio: "1:4"
  - type: "vertical"
    target: "messages"
    panels: ["messages", "input"]
    ratio: "3:1"
  - type: "horizontal"
    target: "input"
    panels: ["input", "terminal"]
    ratio: "1:1"
```

This creates a four-panel layout with sessions list, messages, input, and an interactive shell terminal.

## Notes

- Missing panels or invalid references are ignored during layout creation.
- Custom `command` entries run as-is inside tmux after the environment variables are exported.
- Set `OPENCODE_SERVER` before launching the orchestrator; the loader forwards it to all panes automatically.
- The `shell` type uses your `$SHELL` environment variable (defaults to `/bin/bash` if not set).
- Shell panels are independent from OpenCode panels and can be used for general terminal operations.
