#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# apply_branch_protection.sh
#
# WHAT THIS DOES
#   Migrates geoparquet/geoparquet-io from legacy "classic" branch protection
#   to GitHub repository rulesets, and enables repo-level auto-merge.
#
#   It is a one-shot admin task: run it ONCE after the CI-hardening PR merges.
#
#   Concretely it:
#     1. Enables auto-merge on the repository.
#     2. Deletes the classic branch-protection rule on `main` (if present).
#     3. Creates (or updates) TWO branch rulesets targeting `main`:
#          A. "main: PR + green checks" - forces PR + passing status checks
#             for EVERYONE (no bypass actors, including admins).
#          B. "main: review required"  - requires 1 approving review, but the
#             repository-admin role can bypass it (so admin merges and
#             auto-approved bot PRs don't get blocked on human review).
#     4. Prints a verification summary.
#
# REQUIREMENTS
#   - `gh` CLI authenticated as a user with ADMIN permission on the repo.
#     (Ruleset + branch-protection + repo-settings writes all require admin.)
#   - `jq` on PATH (used to build/validate the JSON payloads and parse output).
#
# SAFE TO RE-RUN
#   Idempotent. Rulesets are matched by NAME via a GET, then updated with PUT
#   if they already exist, or created with POST if they don't. Re-running
#   converges the live config to what this script declares - it will not create
#   duplicate rulesets. Enabling auto-merge and deleting classic protection are
#   likewise no-ops on a second run.
#
# RELATED MANUAL STEP (NOT done by this script)
#   The security-audit bot opens/updates PRs and needs a token that can bypass
#   the rulesets' identity. Create a fine-grained Personal Access Token scoped
#   to THIS repository only, with:
#       - Contents:      Read and write
#       - Pull requests: Read and write
#   and store it as the repository Actions secret named BOT_PR_TOKEN.
#   (A fine-grained PAT is preferred over a classic PAT so the grant stays
#   limited to geoparquet/geoparquet-io.)
# =============================================================================

REPO="geoparquet/geoparquet-io"

# -----------------------------------------------------------------------------
# 1. Enable repository auto-merge.
# -----------------------------------------------------------------------------
echo ">> Enabling auto-merge on ${REPO} ..."
gh api -X PATCH "repos/${REPO}" -F allow_auto_merge=true >/dev/null

# -----------------------------------------------------------------------------
# 2. Delete classic branch protection on main (tolerate 404 = already gone).
# -----------------------------------------------------------------------------
echo ">> Removing classic branch protection on main (if present) ..."
if gh api "repos/${REPO}/branches/main/protection" >/dev/null 2>&1; then
  gh api -X DELETE "repos/${REPO}/branches/main/protection" >/dev/null
  echo "   classic protection deleted."
else
  echo "   no classic protection found (nothing to delete)."
fi

# -----------------------------------------------------------------------------
# Helper: create-or-update a ruleset by name.
#   $1 = ruleset name
#   $2 = full ruleset JSON payload (must contain matching "name")
# Looks the name up in the existing rulesets; PUTs to update if found,
# POSTs to create otherwise. This name-keyed lookup is the idempotency
# mechanism that makes the whole script safe to re-run.
# -----------------------------------------------------------------------------
apply_ruleset() {
  local name="$1"
  local payload="$2"
  local existing_id

  existing_id="$(
    gh api "repos/${REPO}/rulesets" \
      --jq ".[] | select(.name == \"${name}\") | .id" 2>/dev/null || true
  )"

  if [[ -n "${existing_id}" ]]; then
    echo ">> Updating ruleset '${name}' (id ${existing_id}) ..."
    printf '%s' "${payload}" \
      | gh api -X PUT "repos/${REPO}/rulesets/${existing_id}" \
          --input - >/dev/null
  else
    echo ">> Creating ruleset '${name}' ..."
    printf '%s' "${payload}" \
      | gh api -X POST "repos/${REPO}/rulesets" \
          --input - >/dev/null
  fi
}

# -----------------------------------------------------------------------------
# 3a. Ruleset A: "main: PR + green checks"
#
#     enforcement: active, NO bypass actors -> applies to EVERYONE, admins
#     included. Requires a PR (0 approvals) and all 13 status checks green,
#     and blocks force-pushes / deletion of main.
#
#     integration_id is intentionally omitted from each required check so a
#     check with that name counts no matter which app/source reports it.
#     strict_required_status_checks_policy=false: PR branch need not be
#     up-to-date with main before merging.
# -----------------------------------------------------------------------------
RULESET_A_NAME="main: PR + green checks"
RULESET_A_PAYLOAD="$(
  jq -n --arg name "${RULESET_A_NAME}" '
  {
    name: $name,
    target: "branch",
    enforcement: "active",
    bypass_actors: [],
    conditions: {
      ref_name: {
        include: ["~DEFAULT_BRANCH"],
        exclude: []
      }
    },
    rules: [
      {
        type: "pull_request",
        parameters: {
          required_approving_review_count: 0,
          dismiss_stale_reviews_on_push: false,
          require_code_owner_review: false,
          require_last_push_approval: false,
          required_review_thread_resolution: false
        }
      },
      {
        type: "required_status_checks",
        parameters: {
          strict_required_status_checks_policy: false,
          required_status_checks: [
            {context: "lint"},
            {context: "security"},
            {context: "notebooks"},
            {context: "test (ubuntu-latest, 3.10)"},
            {context: "test (ubuntu-latest, 3.11)"},
            {context: "test (ubuntu-latest, 3.12)"},
            {context: "test (ubuntu-latest, 3.13)"},
            {context: "test (macos-latest, 3.11)"},
            {context: "test (macos-latest, 3.12)"},
            {context: "test (macos-latest, 3.13)"},
            {context: "test (windows-latest, 3.11)"},
            {context: "test (windows-latest, 3.12)"},
            {context: "test (windows-latest, 3.13)"}
          ]
        }
      },
      { type: "non_fast_forward" },
      { type: "deletion" }
    ]
  }'
)"

# -----------------------------------------------------------------------------
# 3b. Ruleset B: "main: review required"
#
#     enforcement: active, with a bypass actor for the repository-admin role
#     (actor_id 5 = the built-in "admin" RepositoryRole, bypass_mode always).
#     Requires 1 approving review from everyone EXCEPT admins.
# -----------------------------------------------------------------------------
RULESET_B_NAME="main: review required"
RULESET_B_PAYLOAD="$(
  jq -n --arg name "${RULESET_B_NAME}" '
  {
    name: $name,
    target: "branch",
    enforcement: "active",
    bypass_actors: [
      {
        actor_id: 5,
        actor_type: "RepositoryRole",
        bypass_mode: "always"
      }
    ],
    conditions: {
      ref_name: {
        include: ["~DEFAULT_BRANCH"],
        exclude: []
      }
    },
    rules: [
      {
        type: "pull_request",
        parameters: {
          required_approving_review_count: 1,
          dismiss_stale_reviews_on_push: false,
          require_code_owner_review: false,
          require_last_push_approval: false,
          required_review_thread_resolution: false
        }
      }
    ]
  }'
)"

# -----------------------------------------------------------------------------
# NET EFFECT of A + B together
#   * Ruleset A has NO bypass, so absolutely everyone - contributors AND
#     admins - must open a PR and get all 13 checks green before merging to
#     main. Nobody can push straight to main or force-push/delete it.
#   * Ruleset B additionally demands 1 approving review, but the admin role
#     bypasses B. So:
#       - regular collaborators: PR + green checks + 1 approval;
#       - repo admins and the auto-approved security-audit bot PRs (merged by
#         an admin / with the admin-bypass token): PR + green checks, no
#         second human approval required.
#   Because rulesets are additive, the strictest applicable rule wins for each
#   actor - exactly the layered policy we want.
# -----------------------------------------------------------------------------

apply_ruleset "${RULESET_A_NAME}" "${RULESET_A_PAYLOAD}"
apply_ruleset "${RULESET_B_NAME}" "${RULESET_B_PAYLOAD}"

# -----------------------------------------------------------------------------
# 4. Verification.
# -----------------------------------------------------------------------------
echo ""
echo ">> Rulesets now on ${REPO}:"
gh api "repos/${REPO}/rulesets" \
  --jq '.[] | "   id=\(.id)  name=\(.name)  enforcement=\(.enforcement)"'

echo ""
echo -n ">> allow_auto_merge = "
gh api "repos/${REPO}" --jq .allow_auto_merge

echo ""
echo ">> Done. Remember to create the BOT_PR_TOKEN repo secret (fine-grained"
echo "   PAT, contents+PRs write, this repo only) for the security-audit bot."
