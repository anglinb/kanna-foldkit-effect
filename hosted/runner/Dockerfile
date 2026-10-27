# The runner: the machine half of hosted Kanna. It holds the repos and the
# agent CLIs (and their logins), and runs every process the hosted server
# asks for. The server (hosted/worker.ts) connects to it over KANNA_RUNNER_URL.
#
#   docker build -f hosted/runner/Dockerfile -t kanna-runner .
#   docker run -d --name kanna-runner -p 7420:7420 \
#     -e KANNA_RUNNER_TOKEN=… -v kanna-home:/home/kanna kanna-runner
#
# /home/kanna is the volume: repos, CLI logins (~/.claude, ~/.codex) and
# Kanna's per-account config dirs (~/.kanna/accounts) live there. Its path is
# what the hosted server assumes before the runner first connects
# (KANNA_RUNNER_HOME).

# ── Build: the runner as one file, no node_modules at run time ──────────────
FROM --platform=$BUILDPLATFORM oven/bun:1 AS build
WORKDIR /src
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --ignore-scripts
COPY src ./src
COPY tsconfig.json ./
RUN bun build ./src/server/runner-bun.ts --target bun --outfile /out/runner.js

# ── Run ──────────────────────────────────────────────────────────────────────
# Node, because the agent CLIs ship through npm; Bun for the runner itself.
FROM node:22-bookworm-slim
COPY --from=oven/bun:1 /usr/local/bin/bun /usr/local/bin/bun

# What agents and Kanna's git features run: git and gh, ripgrep for search,
# ps/lsof for Kanna's process and port views, ssh for git remotes.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates curl git gnupg jq less lsof openssh-client procps ripgrep unzip zsh \
  && mkdir -p -m 755 /etc/apt/keyrings \
  && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /etc/apt/keyrings/githubcli-archive-keyring.gpg \
  && chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
  && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list \
  && apt-get update && apt-get install -y --no-install-recommends gh \
  && rm -rf /var/lib/apt/lists/*

# The agent CLIs Kanna drives. Pin with --build-arg to reproduce a build.
ARG CLAUDE_CODE_VERSION=latest
ARG CODEX_VERSION=latest
RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" "@openai/codex@${CODEX_VERSION}" \
  && npm cache clean --force

# Not root: Claude Code refuses to skip permission prompts as root, and the
# agents shouldn't own the container.
RUN useradd --create-home --home-dir /home/kanna --shell /bin/bash kanna
USER kanna
WORKDIR /home/kanna
ENV HOME=/home/kanna \
    PATH=/home/kanna/.local/bin:/usr/local/bin:/usr/bin:/bin \
    PORT=7420

# Optional: Cursor's CLI installs into ~/.local/bin for this user.
ARG INSTALL_CURSOR=false
RUN if [ "$INSTALL_CURSOR" = "true" ]; then curl -fsS https://cursor.com/install | bash; fi

COPY --from=build /out/runner.js /opt/kanna/runner.js
VOLUME /home/kanna
EXPOSE 7420
HEALTHCHECK --interval=30s --timeout=5s CMD curl -fsS http://127.0.0.1:7420/health || exit 1
# KANNA_RUNNER_TOKEN must be set; the runner refuses to start without it.
CMD ["bun", "/opt/kanna/runner.js", "--host", "0.0.0.0", "--port", "7420"]
