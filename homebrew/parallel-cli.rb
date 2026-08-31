cask "parallel-cli" do
  version "PLACEHOLDER_VERSION"

  on_macos do
    on_arm do
      sha256 "PLACEHOLDER_DARWIN_ARM64"
      url "https://github.com/parallel-web/parallel-web-tools/releases/download/v#{version}/parallel-cli-darwin-arm64.zip"
    end
    on_intel do
      sha256 "PLACEHOLDER_DARWIN_X64"
      url "https://github.com/parallel-web/parallel-web-tools/releases/download/v#{version}/parallel-cli-darwin-x64.zip"
    end
  end
  on_linux do
    on_arm do
      sha256 "PLACEHOLDER_LINUX_ARM64"
      url "https://github.com/parallel-web/parallel-web-tools/releases/download/v#{version}/parallel-cli-linux-arm64.zip"
    end
    on_intel do
      sha256 "PLACEHOLDER_LINUX_X64"
      url "https://github.com/parallel-web/parallel-web-tools/releases/download/v#{version}/parallel-cli-linux-x64.zip"
    end
  end

  name "Parallel CLI"
  desc "CLI for the Parallel API - search, extract, research, and enrich data"
  homepage "https://github.com/parallel-web/parallel-web-tools"

  binary "parallel-cli/parallel-cli"

  preflight do
    # Strip quarantine — binary is not notarized (same approach as kreuzberg, etc.)
    if OS.mac?
      system_command "/usr/bin/xattr",
                     args: ["-dr", "com.apple.quarantine", "#{staged_path}/parallel-cli"]
    end
  end

  zap trash: "~/.parallel-cli"
end
