#!/usr/bin/env sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [ -n "$(git status --porcelain)" ]; then
  echo "Local changes are present. Commit, stash, or review them before syncing."
  git status --short
  exit 1
fi

git fetch --prune origin

upstream="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
if [ -z "$upstream" ]; then
  echo "No upstream branch is configured for the current branch."
  git status --short --branch
  exit 1
fi

local_head="$(git rev-parse @)"
remote_head="$(git rev-parse "$upstream")"
base_head="$(git merge-base @ "$upstream")"

if [ "$local_head" = "$remote_head" ]; then
  echo "Already up to date with $upstream."
elif [ "$local_head" = "$base_head" ]; then
  echo "Fast-forwarding to $upstream."
  git merge --ff-only "$upstream"
elif [ "$remote_head" = "$base_head" ]; then
  echo "Local branch is ahead of $upstream. Push or review before starting new work."
else
  echo "Local branch and $upstream have diverged. Resolve intentionally before editing."
  exit 2
fi

git status --short --branch
