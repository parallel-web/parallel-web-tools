"use strict";

const https = require("https");
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { execSync } = require("child_process");

// Skip download if env var is set (useful for CI)
if (process.env.PARALLEL_CLI_SKIP_DOWNLOAD === "1") {
  console.log("PARALLEL_CLI_SKIP_DOWNLOAD is set, skipping binary download.");
  process.exit(0);
}

const PLATFORM_MAP = {
  "darwin-arm64": "darwin-arm64",
  "darwin-x64": "darwin-x64",
  "linux-x64": "linux-x64",
  "linux-arm64": "linux-arm64",
  "win32-x64": "windows-x64",
};

const platformKey = `${process.platform}-${process.arch}`;
const platform = PLATFORM_MAP[platformKey];

if (!platform) {
  console.error(
    `Unsupported platform: ${platformKey}\n` +
    `Supported platforms: ${Object.keys(PLATFORM_MAP).join(", ")}\n\n` +
    "You can download the binary manually from:\n" +
    "  https://github.com/parallel-industries/parallel-web-tools/releases"
  );
  process.exit(1);
}

const packageJson = JSON.parse(
  fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8")
);
const version = packageJson.version;

const baseUrl =
  process.env.PARALLEL_CLI_MIRROR ||
  `https://github.com/parallel-industries/parallel-web-tools/releases/download/v${version}`;

const zipFilename = `parallel-cli-${platform}.zip`;
const zipUrl = `${baseUrl}/${zipFilename}`;
const checksumUrl = `${zipUrl}.sha256`;

const vendorDir = path.join(__dirname, "..", "vendor");
const zipPath = path.join(vendorDir, zipFilename);

function fetch(url) {
  return new Promise((resolve, reject) => {
    const get = url.startsWith("https:") ? https.get : http.get;
    const options = {};

    // Add GitHub token for rate limit avoidance
    if (process.env.GITHUB_TOKEN) {
      const parsed = new URL(url);
      options.hostname = parsed.hostname;
      options.path = parsed.pathname + parsed.search;
      options.headers = {
        Authorization: `token ${process.env.GITHUB_TOKEN}`,
        "User-Agent": "parallel-cli-npm",
        Accept: "application/octet-stream",
      };
    }

    const req = get(url, options, (res) => {
      // Follow redirects (GitHub releases redirect to S3)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location).then(resolve, reject);
      }

      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode} downloading ${url}`));
        return;
      }

      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    });

    req.on("error", reject);
  });
}

function verifyChecksum(data, expected) {
  const actual = crypto.createHash("sha256").update(data).digest("hex");
  const expectedHash = expected.trim().split(/\s+/)[0];
  if (actual !== expectedHash) {
    throw new Error(
      `Checksum mismatch!\n  Expected: ${expectedHash}\n  Actual:   ${actual}`
    );
  }
}

function extractZip(zipPath, destDir) {
  if (process.platform === "win32") {
    execSync(
      `powershell -Command "Expand-Archive -Force -Path '${zipPath}' -DestinationPath '${destDir}'"`,
      { stdio: "inherit" }
    );
  } else {
    execSync(`unzip -o -q "${zipPath}" -d "${destDir}"`, { stdio: "inherit" });
  }
}

function setExecutable(dir) {
  if (process.platform === "win32") return;
  const binaryPath = path.join(dir, "parallel-cli");
  if (fs.existsSync(binaryPath)) {
    fs.chmodSync(binaryPath, 0o755);
  }
}

async function main() {
  console.log(`Installing parallel-cli v${version} for ${platformKey}...`);

  // Ensure vendor directory exists
  fs.mkdirSync(vendorDir, { recursive: true });

  // Download checksum
  console.log("Downloading checksum...");
  const checksumData = await fetch(checksumUrl);
  const checksumText = checksumData.toString("utf8");

  // Download binary
  console.log(`Downloading ${zipFilename}...`);
  const zipData = await fetch(zipUrl);

  // Verify checksum
  console.log("Verifying checksum...");
  verifyChecksum(zipData, checksumText);

  // Write zip to disk
  fs.writeFileSync(zipPath, zipData);

  // Extract
  console.log("Extracting...");
  extractZip(zipPath, vendorDir);

  // Set executable permissions
  setExecutable(vendorDir);

  // Clean up zip
  fs.unlinkSync(zipPath);

  console.log("parallel-cli installed successfully!");
}

main().catch((err) => {
  console.error(
    `Failed to install parallel-cli: ${err.message}\n\n` +
    "You can download the binary manually from:\n" +
    `  ${baseUrl}/${zipFilename}\n\n` +
    "Then extract it to:\n" +
    `  ${vendorDir}`
  );
  process.exit(1);
});
