#!/usr/bin/env bash
set -euo pipefail

# Release script for parallel-web-tools
# Usage:
#   ./scripts/release.sh rc       # bump to next RC (0.1.0rc1 -> 0.1.0rc2, or 0.1.0 -> 0.2.0rc1)
#   ./scripts/release.sh stable   # promote current RC to stable (0.1.0rc2 -> 0.1.0)
#   ./scripts/release.sh 0.2.0    # set an explicit version

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Helpers ---

die() { echo "error: $*" >&2; exit 1; }

get_current_version() {
    grep '^version = ' "$PROJECT_ROOT/pyproject.toml" | sed 's/version = "\(.*\)"/\1/'
}

# Convert Python version (0.1.0rc1) to npm semver (0.1.0-rc.1)
to_npm_version() {
    local v="$1"
    if [[ "$v" =~ ^([0-9]+\.[0-9]+\.[0-9]+)rc([0-9]+)$ ]]; then
        echo "${BASH_REMATCH[1]}-rc.${BASH_REMATCH[2]}"
    else
        echo "$v"
    fi
}

calculate_next_version() {
    local current="$1"
    local bump_type="$2"

    case "$bump_type" in
        rc)
            if [[ "$current" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)rc([0-9]+)$ ]]; then
                # Already an RC: increment RC number
                local major="${BASH_REMATCH[1]}"
                local minor="${BASH_REMATCH[2]}"
                local patch="${BASH_REMATCH[3]}"
                local rc="${BASH_REMATCH[4]}"
                echo "${major}.${minor}.${patch}rc$((rc + 1))"
            elif [[ "$current" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
                # Stable: bump minor, start RC1
                local major="${BASH_REMATCH[1]}"
                local minor="${BASH_REMATCH[2]}"
                local patch="${BASH_REMATCH[3]}"
                echo "${major}.$((minor + 1)).${patch}rc1"
            else
                die "cannot parse version: $current"
            fi
            ;;
        stable)
            if [[ "$current" =~ ^([0-9]+\.[0-9]+\.[0-9]+)rc[0-9]+$ ]]; then
                echo "${BASH_REMATCH[1]}"
            else
                die "current version ($current) is not an RC — nothing to promote"
            fi
            ;;
        *)
            # Explicit version provided
            if [[ "$bump_type" =~ ^[0-9]+\.[0-9]+\.[0-9]+(rc[0-9]+)?$ ]]; then
                echo "$bump_type"
            else
                die "invalid version format: $bump_type (expected X.Y.Z or X.Y.Zrc#)"
            fi
            ;;
    esac
}

update_version_files() {
    local new_version="$1"
    local npm_version
    npm_version="$(to_npm_version "$new_version")"

    echo "updating versions to $new_version (npm: $npm_version)"

    # 1. pyproject.toml
    sed -i '' "s/^version = \".*\"/version = \"$new_version\"/" "$PROJECT_ROOT/pyproject.toml"

    # 2. parallel_web_tools/__init__.py
    sed -i '' "s/__version__ = \".*\"/__version__ = \"$new_version\"/" "$PROJECT_ROOT/parallel_web_tools/__init__.py"

    # 3. bigquery cloud function requirements.txt
    sed -i '' "s/parallel-web-tools>=.*/parallel-web-tools>=$new_version/" \
        "$PROJECT_ROOT/parallel_web_tools/integrations/bigquery/cloud_function/requirements.txt"

    # 5. npm/package.json
    sed -i '' "s/\"version\": \".*\"/\"version\": \"$npm_version\"/" "$PROJECT_ROOT/npm/package.json"
}

# --- Main ---

if [[ $# -lt 1 ]]; then
    echo "usage: ./scripts/release.sh <rc|stable|X.Y.Z>"
    echo ""
    echo "  rc       bump to next release candidate"
    echo "  stable   promote current RC to stable"
    echo "  X.Y.Z    set explicit version"
    exit 1
fi

BUMP_TYPE="$1"
CURRENT_VERSION="$(get_current_version)"
NEW_VERSION="$(calculate_next_version "$CURRENT_VERSION" "$BUMP_TYPE")"
NPM_VERSION="$(to_npm_version "$NEW_VERSION")"

# Determine if this is a prerelease
IS_PRERELEASE=false
if [[ "$NEW_VERSION" =~ rc ]]; then
    IS_PRERELEASE=true
fi

echo ""
echo "  current version:  $CURRENT_VERSION"
echo "  new version:      $NEW_VERSION (npm: $NPM_VERSION)"
echo "  tag:              v$NEW_VERSION"
echo "  prerelease:       $IS_PRERELEASE"
echo ""

# Safety checks
if [[ -n "$(git status --porcelain)" ]]; then
    die "working tree is not clean — commit or stash changes first"
fi

CURRENT_BRANCH="$(git branch --show-current)"
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    die "must be on main branch (currently on $CURRENT_BRANCH)"
fi

# Check if tag already exists
if git rev-parse "v$NEW_VERSION" >/dev/null 2>&1; then
    die "tag v$NEW_VERSION already exists"
fi

# Confirm
read -r -p "proceed? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "aborted."
    exit 0
fi

# Update files
update_version_files "$NEW_VERSION"

# Create branch, commit, push, and open PR
BRANCH="release/v$NEW_VERSION"
git checkout -b "$BRANCH"
git add \
    pyproject.toml \
    parallel_web_tools/__init__.py \
    parallel_web_tools/integrations/bigquery/cloud_function/requirements.txt \
    npm/package.json

# Commit — if pre-commit hooks modify files (e.g. uv.lock), re-stage and retry
if ! git commit -m "chore: bump version to $NEW_VERSION"; then
    echo "pre-commit hooks modified files, re-staging and retrying..."
    git add \
        pyproject.toml \
        parallel_web_tools/__init__.py \
        tests/test_cli.py \
        parallel_web_tools/integrations/bigquery/cloud_function/requirements.txt \
        npm/package.json
    # Also stage any lock files updated by hooks
    git diff --name-only | xargs -r git add
    git commit -m "chore: bump version to $NEW_VERSION"
fi

echo ""
echo "pushing branch and creating PR..."
git push -u origin "$BRANCH"

PRERELEASE_NOTE=""
if [[ "$IS_PRERELEASE" == "true" ]]; then
    PRERELEASE_NOTE=" (pre-release)"
fi

gh pr create \
    --title "chore: bump version to $NEW_VERSION" \
    --body "$(cat <<EOF
## Release $NEW_VERSION${PRERELEASE_NOTE}

Bumps version from $CURRENT_VERSION to $NEW_VERSION.

When this PR is merged to main, the release workflow will automatically:
- Create tag \`v$NEW_VERSION\`
- Create a GitHub Release with auto-generated notes
- Build binaries for all platforms
- Publish to PyPI
- Publish to npm
EOF
)"

echo ""
echo "done! merge the PR to trigger the release."
