#!/bin/bash
#
# Parallel CLI Installer
#
# Installs the standalone parallel-cli binary (includes bundled Python runtime).
# Automatically detects your platform (macOS/Linux, x64/arm64) and downloads
# the appropriate pre-built binary.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/parallel-web/parallel-web-tools/main/install-cli.sh | bash
#
# Or with a specific version:
#   curl -fsSL https://raw.githubusercontent.com/parallel-web/parallel-web-tools/main/install-cli.sh | bash -s -- v0.0.1
#

set -e

VERSION="${1:-latest}"
INSTALL_DIR="${HOME}/.local/bin"
REPO="parallel-web/parallel-web-tools"
BINARY_NAME="parallel-cli"
GITHUB_RELEASES="https://github.com/${REPO}/releases"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${CYAN}==>${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1" >&2
}

# Check for required dependencies
DOWNLOADER=""
if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
else
    print_error "Either curl or wget is required but neither is installed"
    exit 1
fi

# Download function that works with both curl and wget
download() {
    local url="$1"
    local output="$2"

    if [ "$DOWNLOADER" = "curl" ]; then
        if [ -n "$output" ]; then
            curl -fsSL -o "$output" "$url"
        else
            curl -fsSL "$url"
        fi
    elif [ "$DOWNLOADER" = "wget" ]; then
        if [ -n "$output" ]; then
            wget -q -O "$output" "$url"
        else
            wget -q -O - "$url"
        fi
    else
        return 1
    fi
}

# Detect platform
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Darwin)
            os="darwin"
            ;;
        Linux)
            os="linux"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            os="windows"
            ;;
        *)
            print_error "Unsupported operating system: $(uname -s)"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)
            arch="x64"
            ;;
        arm64|aarch64)
            arch="arm64"
            ;;
        *)
            print_error "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac

    echo "${os}-${arch}"
}

# Get latest version from GitHub releases
get_latest_version() {
    local api_url="https://api.github.com/repos/${REPO}/releases/latest"
    local version

    version=$(download "$api_url" "" 2>/dev/null | grep '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/' || echo "")

    if [ -z "$version" ]; then
        print_error "Failed to fetch latest version from GitHub"
        print_error "Check: ${GITHUB_RELEASES}"
        exit 1
    fi

    echo "$version"
}

# Setup shell PATH
setup_path() {
    local shell_name rc_file

    shell_name=$(basename "$SHELL")

    case "$shell_name" in
        bash)
            rc_file="$HOME/.bashrc"
            [ -f "$HOME/.bash_profile" ] && rc_file="$HOME/.bash_profile"
            ;;
        zsh)
            rc_file="$HOME/.zshrc"
            ;;
        fish)
            rc_file="$HOME/.config/fish/config.fish"
            ;;
        *)
            print_warning "Unknown shell: $shell_name"
            print_warning "Add ${INSTALL_DIR} to your PATH manually"
            return
            ;;
    esac

    # Check if already in PATH
    if [[ ":$PATH:" == *":${INSTALL_DIR}:"* ]]; then
        return
    fi

    print_status "Adding ${INSTALL_DIR} to PATH in ${rc_file}..."

    if [ "$shell_name" = "fish" ]; then
        echo "set -gx PATH ${INSTALL_DIR} \$PATH" >> "$rc_file"
    else
        echo "export PATH=\"${INSTALL_DIR}:\$PATH\"" >> "$rc_file"
    fi

    print_warning "Restart your shell or run: source ${rc_file}"
}

# Main
main() {
    echo ""
    echo "╔════════════════════════════════════════════╗"
    echo "║         Parallel CLI Installer             ║"
    echo "╚════════════════════════════════════════════╝"
    echo ""

    # Detect platform
    local platform
    platform=$(detect_platform)
    print_status "Detected platform: ${platform}"

    # Determine version
    if [ "$VERSION" = "latest" ]; then
        print_status "Fetching latest release..."
        VERSION=$(get_latest_version)
    fi
    print_status "Installing version: ${VERSION}"

    # Build download URLs
    local binary_url="${GITHUB_RELEASES}/download/${VERSION}/${BINARY_NAME}-${platform}"
    local checksum_url="${binary_url}.sha256"

    # Windows binary has .exe extension
    if [[ "$platform" == windows-* ]]; then
        binary_url="${binary_url}.exe"
        checksum_url="${binary_url}.sha256"
    fi

    # Create install directory and temp directory
    mkdir -p "$INSTALL_DIR"
    local tmp_dir
    tmp_dir=$(mktemp -d)
    local tmp_file="${tmp_dir}/${BINARY_NAME}"

    # Download binary
    print_status "Downloading ${BINARY_NAME}..."
    if ! download "$binary_url" "$tmp_file" 2>/dev/null; then
        print_error "Failed to download binary"
        print_error "URL: ${binary_url}"
        print_error ""
        print_error "This could mean:"
        print_error "  - The release doesn't exist yet"
        print_error "  - Your platform (${platform}) isn't supported"
        print_error ""
        print_error "Check available releases: ${GITHUB_RELEASES}"
        rm -rf "$tmp_dir"
        exit 1
    fi

    # Download and verify checksum
    print_status "Verifying checksum..."
    local checksum_file="${tmp_dir}/checksum"
    if download "$checksum_url" "$checksum_file" 2>/dev/null; then
        local expected_checksum
        expected_checksum=$(cat "$checksum_file")
        local actual_checksum

        if [ "$(uname -s)" = "Darwin" ]; then
            actual_checksum=$(shasum -a 256 "$tmp_file" | cut -d' ' -f1)
        else
            actual_checksum=$(sha256sum "$tmp_file" | cut -d' ' -f1)
        fi

        if [ "$actual_checksum" != "$expected_checksum" ]; then
            print_error "Checksum verification failed!"
            print_error "Expected: ${expected_checksum}"
            print_error "Got:      ${actual_checksum}"
            rm -rf "$tmp_dir"
            exit 1
        fi
        print_success "Checksum verified"
    else
        print_warning "Checksum file not available, skipping verification"
    fi

    # Install binary
    local install_path="${INSTALL_DIR}/${BINARY_NAME}"
    mv "$tmp_file" "$install_path"
    chmod +x "$install_path"
    rm -rf "$tmp_dir"

    print_success "Installed to ${install_path}"

    # Setup PATH
    setup_path

    # Verify installation
    export PATH="${INSTALL_DIR}:$PATH"
    if command -v "${BINARY_NAME}" >/dev/null 2>&1; then
        echo ""
        print_success "Installation complete!"
        echo ""
        "${BINARY_NAME}" --version 2>/dev/null || true
        echo ""
        echo "Next steps:"
        echo "  1. Authenticate:  ${BINARY_NAME} login"
        echo "  2. Search:        ${BINARY_NAME} search \"your query\""
        echo "  3. Extract:       ${BINARY_NAME} extract https://example.com"
        echo "  4. Enrich:        ${BINARY_NAME} enrich run --help"
        echo ""
    else
        print_error "Installation verification failed"
        exit 1
    fi
}

main "$@"
