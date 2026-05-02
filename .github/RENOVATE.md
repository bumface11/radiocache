# Renovate Configuration

This repository uses the [Renovate GitHub App](https://github.com/apps/renovate) to
keep Python dependencies up to date.

## Why Renovate?

The previous "Update Python Dependencies" GitHub Actions workflow (`update-dependencies.yml`)
was failing with:

> GitHub Actions is not permitted to create or approve pull requests.

Renovate opens PRs as the Renovate App installation, avoiding that restriction entirely.
It also handles `uv.lock` regeneration natively via `postUpdateOptions: ["uvLockfile"]`.

## What Renovate does

Renovate replaces the retired `update-dependencies.yml` workflow with equivalent behaviour:

| Behaviour | Old workflow | Renovate |
|---|---|---|
| Trigger | First Monday of each month (cron) | First of each month |
| Updates | `uv lock --upgrade` | `uv lock --upgrade` via `uvLockfile` post-update |
| Excludes recent releases | `--exclude-newer 14 days` | `minimumReleaseAge: "14 days"` |
| Grouping | Single PR for all packages | Single grouped PR (`groupName: "Python dependencies"`) |
| PR creator | `peter-evans/create-pull-request` (broken) | Renovate App (works) |

## Configuration

The Renovate config lives at [`renovate.json`](../renovate.json) in the repo root:

```json
{
  "$schema": "https://docs.renovatebot.com/renovate-schema.json",
  "extends": ["config:recommended"],
  "labels": ["dependencies"],
  "assignees": ["bumface11"],
  "postUpdateOptions": ["uvLockfile"],
  "packageRules": [
    {
      "matchManagers": ["uv"],
      "groupName": "Python dependencies",
      "schedule": ["on the first day of the month"],
      "minimumReleaseAge": "14 days"
    }
  ]
}
```

Key settings:

- **`postUpdateOptions: ["uvLockfile"]`** — tells Renovate to run `uv lock` after updating
  dependencies, so `uv.lock` is always committed alongside `pyproject.toml` changes.
- **`minimumReleaseAge: "14 days"`** — packages released within the last 14 days are
  skipped, matching the original workflow's `--exclude-newer` behaviour.
- **`schedule: ["on the first day of the month"]`** — monthly cadence.
- **`groupName: "Python dependencies"`** — all Python package bumps land in a single PR.

## Enabling Renovate

> ⚠️  The Renovate GitHub App must be installed and granted access to this repository
> before any PRs will appear.

1. Go to <https://github.com/apps/renovate> and click **Install**.
2. Select **bumface11/radiocache** (or your fork).
3. Renovate will detect `renovate.json` and open an onboarding PR (if it hasn't already).
4. Merge the onboarding PR to activate the schedule.

## Other dependency managers

- **GitHub Actions** versions are still managed by **Dependabot** (see
  [dependabot.yml](dependabot.yml)) — weekly, grouped into a single PR.
- **Docker base image** is also managed by **Dependabot** — monthly.
- Python (uv) is managed exclusively by **Renovate** (the `uv` entry in
  `dependabot.yml` was removed to avoid duplicate PRs).
