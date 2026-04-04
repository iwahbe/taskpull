FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    python3 \
    python3-venv \
    curl \
    ripgrep \
    libicu72 \
    libssl3 \
    socat \
    tmux \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install mise (into /usr/local/bin so it's available to all users)
RUN curl https://mise.run | MISE_INSTALL_PATH=/usr/local/bin/mise sh

# Install Node.js (via NodeSource) and Claude CLI
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @anthropic-ai/claude-code

# Install GitHub CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update && apt-get install -y gh && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Install taskpull (for hooks and MCP server commands inside the container)
COPY taskpull*.whl /tmp/
RUN uv tool install /tmp/taskpull*.whl \
    && chmod a+x /root \
    && chmod -R a+rX /root/.local

# Create non-root user for running Claude
RUN useradd -m -s /bin/bash worker \
    && mkdir -p /workspace \
    && chown worker:worker /workspace
ENV PATH="/root/.local/bin:$PATH"

USER worker
WORKDIR /workspace
