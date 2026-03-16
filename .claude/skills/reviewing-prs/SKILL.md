---
name: reviewing-prs
description: Reviews a GitHub PR using three parallel sub-agents (software engineer, data engineer, QA engineer), synthesizes their findings, and posts a structured review comment. Use when reviewing a PR, when the user says /review-pr, or when asked to review code changes.
---

# PR Review

## Workflow

1. **Fetch PR context** using the commands below
2. **Spawn 3 sub-agents in parallel** — one per reviewer persona (see [REVIEWERS.md](REVIEWERS.md))
3. **Collect results** from all three
4. **Synthesize** into a single review using the format in [COMMENT-TEMPLATE.md](COMMENT-TEMPLATE.md)
5. **Post the comment** to the PR

## Step 1: Fetch PR Context

```bash
gh pr view <number> --json title,body,baseRefName,headRefName,files,additions,deletions
gh pr diff <number>
gh pr view <number> --json reviews,comments
```

Pass the PR number, title, diff, and file list to each sub-agent.

## Step 2: Spawn Reviewers

Launch all three sub-agents **in parallel** using the Agent tool. Each agent receives:
- The full PR diff
- The file list with additions/deletions
- The PR description
- Their specific reviewer instructions from [REVIEWERS.md](REVIEWERS.md)

Each sub-agent must return a structured review following the format in their instructions. Do not ask them to post comments — only the main agent posts.

## Step 3: Synthesize

Read all three reviews. Deduplicate overlapping findings. Assign a final severity to each issue:
- **Critical** — must fix before merge (bugs, security, data loss)
- **Warning** — should fix, but not a blocker
- **Suggestion** — nice to have, optional

## Step 4: Post Comment

```bash
gh pr comment <number> --body "$(cat <<'EOF'
<synthesized review — see COMMENT-TEMPLATE.md>
EOF
)"
```

## Boundaries

**Always:**
- Read the full diff before reviewing
- Run all 3 reviewers in parallel
- Include the issue number from the PR body in the review
- Attribute findings to the reviewer who raised them

**Ask first:**
- Requesting changes (blocking the PR) vs leaving comments
- If the PR touches more than 500 lines changed

**Never:**
- Approve or merge the PR automatically
- Edit code or push commits to the PR branch
- Post multiple comments (always one synthesized comment)