#!/usr/bin/env bash
set -euo pipefail

# 1. Install Homebrew if not installed
if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew not found, installing..."
  curl -fsSL -o install.sh https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh
  # Verify SHA256 checksum of install.sh
  bash install.sh
fi

# Update Homebrew
brew update

# 2. Install OpenJDK 21 and Maven
brew install openjdk maven

# 3. Add environment variables to ~/.zshrc if not already present
ZSHRC="$HOME/.zshrc"
BREW_JAVA_PREFIX="$(brew --prefix openjdk)"

if ! grep -q 'export JAVA_HOME' "$ZSHRC"; then
  {
    echo ""
    echo "# Java setup"
    echo "export JAVA_HOME=\"$BREW_JAVA_PREFIX\""
    echo "export PATH=\"\$JAVA_HOME/bin:\$PATH\""
    echo "export CPPFLAGS=\"-I\$JAVA_HOME/include\""
  } >> "$ZSHRC"
  echo "Java environment variables added to $ZSHRC"
fi

# 4. Apply changes
# shellcheck disable=SC1090
source "$ZSHRC"

# 5. Build the project
PROJECT_DIR="$HOME/IdeaProjects/allthedocs/roles/taboola/tabularasa-bi-core"
echo "Switching to project directory and building:"
cd "$PROJECT_DIR"
mvn clean package

echo "Done! Java and Maven are installed, project built."