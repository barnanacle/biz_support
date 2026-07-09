# Agent Collaboration Guide

This repository is shared between Claude Code, Codex, GitHub Actions, and Cloudflare Pages.
Follow these rules before changing files so agent-to-agent handoffs stay predictable.

## Repository Facts

- Repo root: `/Users/ryu/Antigravity/biz_support/biz_support_web`
- Remote: `https://github.com/barnanacle/biz_support.git`
- Production: `https://biz-support.pages.dev/`
- Main branch: `main`
- Deployment source: Cloudflare Pages deploys from the GitHub repository.
- Scheduled automation: `.github/workflows/crawl.yml` updates `data.json`.

## Start-of-Work Checklist

1. Work from the repo root, not the parent `biz_support` folder.
2. Run `scripts/agent-sync.sh` before editing.
3. If the script reports local changes, inspect them first. Do not overwrite another agent's work.
4. If the script reports diverged history, stop and resolve intentionally instead of running a blind pull.
5. Keep each change focused and commit/push before switching tools.

## Git Rules

- Use fast-forward-only syncs. This repo has local `pull.ff=only` configured for that purpose.
- Do not use `git reset --hard`, `git checkout -- <file>`, or destructive cleanup unless the user explicitly asks.
- Do not amend, squash, or rewrite published commits unless the user explicitly asks.
- Before pushing, run `git fetch origin` and confirm the branch is not behind `origin/main`.
- Prefer small commits with prefixes already used in the repo, such as `fix:`, `feat:`, `docs:`, or `auto:`.

## File Ownership Notes

- `data.json` is an automated crawl output and changes frequently.
- `.github/workflows/crawl.yml` owns the scheduled crawl and commit flow.
- `crawler/unified_crawler.py` owns data collection and enrichment behavior.
- `index.html` owns the static production UI.
- `requirements.txt` owns crawler/runtime dependencies.

Avoid editing `data.json` manually unless the task is specifically about data correction or crawl output review.
When changing crawler logic, expect the next scheduled workflow to change `data.json` again.

## Validation

- For UI-only changes, open `index.html` locally and verify the page still loads `data.json`.
- For crawler changes, run the smallest practical crawler check before a full crawl.
- For workflow changes, review `.github/workflows/crawl.yml` carefully because it can push directly to `main`.
- If a command needs network access or credentials, ask for approval instead of inventing a workaround.

## Handoff Format

When handing off to another tool or agent, leave:

- current branch and sync state,
- files changed,
- validation commands run,
- whether `data.json` was intentionally changed,
- any commands that failed or required approval.
