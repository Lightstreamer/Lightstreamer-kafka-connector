---
applyTo: "**/EXTRA/PR/**/*.md"
---

# Pull Request Document Conventions

This document defines how to author a **PR document** for the Kafka Connector: the Markdown file that holds the pull request description (the text meant to be pasted into the GitHub PR body).

## Location and naming

- PR documents live under `kafka-connector-project/EXTRA/PR/`.
- One file per PR, named in lowercase kebab-case after the branch or the change theme (e.g. `minor-fixes-docs-and-example-secrets.md`).
- Keep PR documents separate from **design notes**, which live under `kafka-connector-project/EXTRA/DOCS/`. A design note captures rationale, alternatives, and deeper reasoning; a PR document describes *what the PR changes* and is structured for review.

## Title (the `H1`)

The first line is a single `#` heading written in [Conventional Commits](https://www.conventionalcommits.org/) style:

```
# <type>: <description>
```

- Pick a `type` prefix that matches the dominant change: `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `chore`, `perf`.
- Start the description **lowercase**, use the **imperative mood** ("add", not "added"/"adds"), and use **no trailing period**.
- Keep **proper nouns** capitalized (`Kubernetes`, `Helm Chart`, `TLS`, `README`, `Confluent`, `Schema Registry`).
- Keep it short (aim for ≤ 72 characters) and specific about the primary outcome.

**Good:** `docs: unify parameter-reference wording and regenerate example TLS secrets`
**Avoid:** `docs: Add Helm Chart and Kubernetes deployment references` (capital `A`dd deviates from the convention).

> [!NOTE]
> This is a convention, not a hard rule. If the repository history ever standardizes on a different casing, match the repository for consistency over this guidance.

## Document structure

Model the body on the reference PR description format. Include the sections that apply; omit the ones that do not.

1. **Intro paragraph** — one short paragraph stating *what* the PR does and *why*. Call out up front if it is docs-only / examples-only / has no production code changes.
2. **`Target release:`** — a single line naming the intended release, e.g. `Target release: `[2.1.1]`.`
3. **`## Table of contents`** — a bullet list linking to the sections below (anchor links).
4. **`## Highlights`** — the handful of most important bullets a reviewer should read first.
5. **`## New features`** / **`## Changes`** — the detailed body, grouped into `###` subsections by area (e.g. by parameter, component, or file group). Group related edits; do not list every line.
6. **`## Breaking changes`** — only when something is not backward-compatible; describe the migration.
7. **`## Behavioral changes`** — runtime/semantic changes (or an explicit "None.").
8. **`## Compatibility`** — state backward-compatibility explicitly, and who is affected.
9. **`## Examples and documentation`** — changes to `README.md`, example configs, factory `adapters.xml`, demos.
10. **`## Testing`** — what was run/verified (`./gradlew check`, manual steps). State when the test suite is unaffected and why.

## Content style

- **Link referenced files** with relative Markdown links. From `kafka-connector-project/EXTRA/PR/`, the repository root is three levels up, so use `../../../` (e.g. `[README.md](../../../README.md)`). Anchor into a specific section where helpful (e.g. `(../../../README.md#consumermode)`).
- **Wrap in backticks** parameter names, configuration values, file names, and identifiers (`consumer.mode`, `MANUAL`, `adapters.xml`).
- **Reference the PR number** where the convention calls for it (e.g. trailing `([#NN](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/NN))`), matching the style used in [`CHANGELOG.md`](../../CHANGELOG.md).
- **Summarize, do not transcribe.** Describe the shape of the change (what/why), not a line-by-line diff.
- **State compatibility explicitly.** Always say whether the change is backward-compatible and, if not, how to migrate.
- Use GitHub alert callouts (`> [!NOTE]`, `> [!IMPORTANT]`, `> [!WARNING]`) for caveats, mirroring the documentation style of `README.md`.

## Relationship to the CHANGELOG

The PR document is the richer, review-oriented narrative. The [`CHANGELOG.md`](../../CHANGELOG.md) entry is its condensed, user-facing counterpart. When opening a PR, ensure the two agree: the CHANGELOG entry should distill the PR document's Highlights and Breaking/Behavioral changes into the project's changelog format, each bullet closing with the `([#NN](…))` PR link.

## Managing the CHANGELOG entry separately

Do **not** edit the root [`CHANGELOG.md`](../../CHANGELOG.md) directly while the PR is in progress. Instead, draft the entry as a **separate companion file** next to the PR document, so the release owner merges it into `CHANGELOG.md` only when the release is cut.

> [!IMPORTANT]
> **Released entries are immutable.** Never edit, restyle, or reformat an existing `## [<version>]` entry in `CHANGELOG.md` — each one is a historical record of a shipped release. Style conventions apply only to the **new** entry you are adding. When in doubt about "matching the style", take the most recent entry as the *reference* to imitate in your new entry; do not change the older entries to match each other.

- **Location and naming** — place the draft alongside the PR document, using the same base name with a `.changelog.md` suffix (e.g. `minor-fixes-docs-and-example-secrets.md` → `minor-fixes-docs-and-example-secrets.changelog.md`).
- **Drop-in format** — write the draft exactly as it will appear in `CHANGELOG.md`:
  - A top HTML comment (`<!-- … -->`) with merge instructions (where to paste, what to replace) that is stripped out on merge.
  - A single `## [<version>] (<YYYY-MM-DD>)` heading followed by the categorized bold sections used by the changelog (`**Breaking Changes**`, `**New Features**`, `**Improvements**`, `**Bug Fixes**`, `**Examples and Documentation**`, `**Third-Party Library Updates**`), in that order. Omit the sections that do not apply.
  - **Repository-root-relative links** (`README.md`, `kafka-connector-project/…`), **not** the `../../../` form used by the PR document — because the block is pasted into `CHANGELOG.md`, which lives at the repository root.
  - Each bullet ends with the PR link `([#NN](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/NN))`. Use `#NN` as a placeholder until the PR number is known, and note the placeholder in the top comment.
  - **Keep the bullet lead-in style consistent within a section.** Either give *every* bullet in a category a bold `**lead-in label**:` prefix, or give *none* of them one — do not mix the two forms in the same entry. Prefer the bold-label form when the entry has several heterogeneous bullets, as it makes each change scannable.
- **Versioning** — pick the next [Semantic Versioning](https://semver.org/) number relative to the latest released version: patch (`x.y.Z`) for docs/examples/bug-fix-only changes, minor (`x.Y.0`) for backward-compatible features, major (`X.0.0`) for breaking changes. Use the expected release date; leave it to be confirmed at merge time.
- **At release time** — strip the top comment, replace `#NN` with the real PR number, confirm the version and date, paste the block directly under the `# Changelog` heading in `CHANGELOG.md` (above the previous top entry), and delete the companion `.changelog.md` draft.
