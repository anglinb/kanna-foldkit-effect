#!/usr/bin/env bash
# Sets up a Debian/Ubuntu machine as a Kanna runner, without Docker: the same
# tools as hosted/runner/Dockerfile. Run as a normal user with sudo; the
# runner and the agent CLIs run as that user.
#
#   hosted/runner/setup.sh            # install the tools
#   KANNA_RUNNER_TOKEN=… bun run runner -- --host 0.0.0.0 --port 7420
#
# On macOS there is nothing to install beyond Bun and the CLIs you already
# use; run `bun run runner` from a checkout.
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
  echo "Run as the user the agents should run as, not root (Claude Code refuses to skip permission prompts as root)." >&2
  exit 1
fi
if ! command -v apt-get >/dev/null; then
  echo "This script is for Debian/Ubuntu. On other systems install: git, gh, ripgrep, lsof, procps, Node 22, Bun, and the agent CLIs." >&2
  exit 1
fi

echo "==> system packages"
sudo apt-get update
sudo apt-get install -y --no-install-recommends ca-certificates curl git gnupg jq less lsof openssh-client procps ripgrep unzip zsh

if ! command -v gh >/dev/null; then
  echo "==> GitHub CLI"
  sudo mkdir -p -m 755 /etc/apt/keyrings
  curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg >/dev/null
  sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
  sudo apt-get update && sudo apt-get install -y gh
fi

if ! command -v node >/dev/null || [ "$(node -p 'process.versions.node.split(".")[0]')" -lt 20 ]; then
  echo "==> Node 22"
  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi

if ! command -v bun >/dev/null; then
  echo "==> Bun"
  curl -fsSL https://bun.sh/install | bash
  export PATH="$HOME/.bun/bin:$PATH"
fi

echo "==> agent CLIs (into ~/.local, no sudo)"
npm config set prefix "$HOME/.local"
npm install -g @anthropic-ai/claude-code @openai/codex
if [ "${INSTALL_CURSOR:-false}" = "true" ]; then curl -fsS https://cursor.com/install | bash; fi

cat <<'DONE'

Done. Make sure ~/.local/bin and ~/.bun/bin are on PATH in your login shell
(the runner reads PATH from it when it starts), then sign in to the CLIs,
here or from Kanna's Settings → Providers once the server is connected:

  claude          # then /login
  codex login
  gh auth login

Start the runner:

  KANNA_RUNNER_TOKEN=<secret> bun run runner -- --host 0.0.0.0 --port 7420
DONE
