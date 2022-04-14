#!/bin/bash
# 
# Run this on a new MacOS Build Agent to install and start the agent with the correct settings
# requires a TOKEN environment variable to be set with the buildkite token

# Install xcode-cli tools
softwareupdate --install "Command Line Tools for Xcode-13.3"

# Install rust
curl https://sh.rustup.rs -sSf | sh -s -- -y

# Install homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> /Users/administrator/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
brew update

# buildkite
brew install awscli
brew install buildkite-agent
BUILDKITE_AGENT_CONFIG=/opt/homebrew/etc/buildkite-agent/buildkite-agent.cfg

# Tailscale
brew install go
go install tailscale.com/cmd/tailscale{,d}@main
sudo $HOME/go/bin/tailscaled install-system-daemon
$HOME/go/bin/tailscale up --accept-routes

# Manual steps
echo "### Start Manual Steps ###"
echo "run `aws configure` now"
echo "### End Manual Steps ###"

# Start agents
$HOME/go/bin/tailscale up --accept-routes 
brew services start buildkite/buildkite/buildkite-agent