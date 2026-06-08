#!/usr/bin/env bash
set -euo pipefail

# USAGE: (Run from the repository root or in a GitHub Action workflow [See .github/workflows/jscpd_localonly.yml])
# bash check_jscpd.sh                # Precedently: compares HEAD..GITHUB_BASE_REF, unstaged..HEAD, or HEAD^..HEAD
# bash check_jscpd.sh main           # compare main..HEAD
# bash check_jscpd.sh origin/main    # compare origin/main..HEAD
# bash check_jscpd.sh abc1234        # compare abc1234..HEAD

# Files to check
FILE_EXT_RE='\.(py|js)$'

# Files to exclude
EXCLUDE_PATS=(
    '(^|/)migrations(/|$)'
    '(^|/)\.venv'
    '^static/bootstrap-[^/]+/'
)

# Optional argument (when running locally, to compare with a different commit/branch)
COMPARE_REF="${1:-}"

# Validate user-supplied comparison reference
if [[ -n "$COMPARE_REF" ]]; then
    if ! git rev-parse --verify "$COMPARE_REF^{commit}" >/dev/null 2>&1; then
        echo "ERROR: Invalid commit, tag, or branch: $COMPARE_REF" >&2
        exit 1
    fi
fi

status=0
checked_count=0

# GitHub Actions supplies the GITHUB_BASE_REF variable
if [[ -n "${GITHUB_BASE_REF:-}" ]]; then
    echo "Comparing changes with merge-base ($GITHUB_BASE_REF..HEAD)."

    # GitHub Actions: all files changed in the PR
    BASE=$(git merge-base "origin/${GITHUB_BASE_REF}" HEAD)

    mapfile -d '' FILES < <(
        git diff --name-only -z "$BASE" HEAD
    )
elif [[ -n "$COMPARE_REF" ]]; then
    # Explicit override supplied by user
    echo "Comparing $COMPARE_REF..HEAD."

    mapfile -d '' FILES < <(
        git diff --name-only -z "$COMPARE_REF" HEAD
    )
else
    # Local: modified + untracked files
    mapfile -d '' FILES < <(
        git ls-files \
            --modified \
            --others \
            --exclude-standard \
            -z
    )

    # If there are no local changes, fall back to HEAD^
    if (( ${#FILES[@]} == 0 )); then
        echo "No local changes detected; comparing HEAD^..HEAD."

        mapfile -d '' FILES < <(
            git diff --name-only -z HEAD^ HEAD
        )
    fi

    # If there are no local changes, fall back to comparing with HEAD^ (or the supplied COMPARE_REF)
    if (( ${#FILES[@]} == 0 )); then
        if [[ -n "$COMPARE_REF" ]]; then
            echo "No local changes detected; comparing $COMPARE_REF..HEAD."

            mapfile -d '' FILES < <(
                git diff --name-only -z "$COMPARE_REF" HEAD
            )
        else
            echo "No local changes detected; comparing HEAD^..HEAD."

            mapfile -d '' FILES < <(
                git diff --name-only -z HEAD^ HEAD
            )
        fi
    else
        echo "Comparing unstaged changes."
    fi
fi

# Now actually run jscpd
for file in "${FILES[@]}"; do
    [[ "$file" =~ $FILE_EXT_RE ]] || continue

    skip=false
    for pat in "${EXCLUDE_PATS[@]}"; do
        if [[ "$file" =~ $pat ]]; then
            echo "Skipping $file"
            skip=true
            break
        fi
    done

    $skip && continue

    [[ -f "$file" ]] || continue

    echo "Checking $file"
    ((++checked_count))

    if ! jscpd --config .github/linters/.jscpd.json "$file"; then
        status=1
    fi
done

if (( checked_count == 0 )); then
    echo "No changed files matching $FILE_EXT_RE found to check."
    exit 0
fi

exit $status