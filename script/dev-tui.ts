#!/usr/bin/env bun

import { $ } from "bun"

async function buildPanels() {
  // Build tmux orchestrator
  await $`cd packages/tui/cmd/opencode-tmux && go build -o opencode-tmux main.go`
  // Build sessions panel
  await $`cd packages/tui/cmd/opencode-sessions && mkdir -p dist && go build -o dist/sessions-pane main.go`
  // Build messages panel
  await $`cd packages/tui/cmd/opencode-messages && mkdir -p dist && go build -o dist/messages-pane main.go`
  // Build input panel
  await $`cd packages/tui/cmd/opencode-input && mkdir -p dist && go build -o dist/input-pane main.go`
}

async function startServer(): Promise<{ url: string; proc: ReturnType<typeof Bun.spawn> }> {
  // Start the opencode server via the TypeScript entrypoint
  const proc = Bun.spawn({
    cmd: ["bun", "run", "packages/opencode/src/index.ts", "serve"],
    stdout: "pipe",
    stderr: "pipe",
  })

  let buffer = ""
  const url = await new Promise<string>(async (resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Timeout waiting for opencode server to start"))
    }, 10000)

    try {
      const reader = proc.stdout!.getReader()
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        const chunk = typeof value === "string" ? value : new TextDecoder().decode(value)
        buffer += chunk
        const lines = buffer.split("\n")
        for (const line of lines) {
          if (line.startsWith("opencode server listening")) {
            const match = line.match(/on\s+(https?:\/\/[^\s]+)/)
            if (match && match[1]) {
              clearTimeout(timeout)
              resolve(match[1])
              return
            }
          }
        }
      }
    } catch (err) {
      clearTimeout(timeout)
      reject(err as Error)
      return
    }

    // If we reach here, stream ended without the expected line
    clearTimeout(timeout)
    reject(new Error("Failed to detect server URL from output"))
  })

  return { url, proc }
}

async function startTmux(url: string) {
  // Inherit stdio so tmux attaches in the current terminal
  const tmux = Bun.spawn({
    cmd: ["./opencode-tmux"],
    cwd: "packages/tui/cmd/opencode-tmux",
    stdout: "inherit",
    stderr: "inherit",
    stdin: "inherit",
    env: {
      ...process.env,
      OPENCODE_SERVER: url,
      // Do NOT set OPENCODE_SOCKET; opencode-tmux will default to ~/.opencode/ipc.sock
    },
  })
  return tmux
}

try {
  // 1) Build panel binaries
  await buildPanels()

  // 2) Start server and parse its URL
  const { url, proc: serverProc } = await startServer()
  console.log(`Detected opencode server at ${url}`)

  // Ensure the server is killed on exit
  const cleanup = () => {
    try {
      serverProc.kill()
    } catch {}
  }
  process.on("SIGINT", cleanup)
  process.on("SIGTERM", cleanup)
  process.on("exit", cleanup)

  // 3) Start tmux orchestrator with dynamic OPENCODE_SERVER
  const tmuxProc = await startTmux(url)
  const code = await tmuxProc.exited
  cleanup()
  process.exit(code)
} catch (err) {
  console.error(err)
  process.exit(1)
}