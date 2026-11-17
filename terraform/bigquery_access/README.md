# terraform/bigquery_access

Table-level BigQuery access, as code. This folder is the source of truth for **who can read
which tables**. Project `data-499514`.

One YAML file per team in `teams/` maps a Google group to tables and a role. Terraform turns
those files into `google_bigquery_table_iam_binding` resources. Git history and PRs are the
audit trail:

| Question | Where to look |
|---|---|
| Who has access to table X? | `grep -r "dataset.table" teams/` |
| What does team T have? | `teams/<team>.yaml` |
| When was it granted, by whom? | `git log -p -- teams/<team>.yaml`, then open the PR |
| Why? | the `reason:` field and the PR description |
| Who approved? | the PR's approving review |

## Request access

1. Make sure the people are in a Google group for the team (e.g. `billing@progresspartners.net`).
   Never grant to individuals: membership changes happen in the group, not here.
2. Branch from `develop`. Edit or create `teams/<team>.yaml` (format below).
3. Open a PR. Everything the reviewer needs is in the YAML: team, group, tables, role,
   `reason`, and optionally `sensitivity` (plus `signed_off_by` when sensitivity is not `none`).
4. The **Terraform plan** check validates the files and shows the exact bindings that change.
5. The data owner approves the PR (and is named in `signed_off_by` for sensitive data). The
   requester cannot approve their own PR.
6. Merge, wait for release to `main` then run the apply (see below). Merging alone changes nothing in BigQuery.

Revoking is the same flow: remove the tables or the file.

## File format

```yaml
# teams/billing.yaml
team: billing
members:
  - group:billing@progresspartners.net      # group:... or serviceAccount:...
grants:
  - role: roles/bigquery.dataViewer         # must be in var.allowed_roles
    reason: "Billing reconciles invoices against the billing exports (ticket DATA-123)."
    sensitivity: none                       # optional, free text; anything but none needs signed_off_by
    # signed_off_by: data-owner-handle      # required when sensitivity is not none
    tables:
      - bronze.files_editor_billing_orders  # dataset.table, no wildcards
      - bronze.files_editor_billing_customers
```

The plan fails (so the PR check goes red) if a role is not in `var.allowed_roles`, a table is
not `dataset.table`, a `reason` is missing or shorter than 10 characters, a grant with a
`sensitivity` other than `none` has no `signed_off_by`, or a member is an individual `user:`.
Roles are allowlisted in `variables.tf`; widening that list is itself an
access decision and gets reviewed like one.

Several teams may list the same table and role: Terraform merges their members into one
binding.

### Legacy files

`teams/legacy_*.yaml` hold grants that existed in BigQuery before this process, captured as-is
on 2026-10-26 from the live IAM policies. They are the only files allowed to use `user:`
members (`legacy: true`), and only for the people listed in `local.grandfathered_users` in
`access.tf`; a new individual grant fails the plan. Their original reason and approver were not
recorded. To retire one: confirm the need with the person, move the tables into a team file
with a group, and delete the legacy file.

## How changes are applied

- **PR:** `.github/workflows/terraform-plan.yml` runs `fmt`, `validate` and `plan` for this
  stack. It never applies and uses no BigQuery permissions (it compares the files with the
  state).
- **After merging, apply by hand:** GitHub → Actions → **Terraform apply** → Run workflow →
  branch `main`, `stack` = `bigquery_access`. `.github/workflows/terraform-apply.yml` plans
  against live BigQuery and applies exactly that plan; the run summary shows what changed.
  Until someone runs it, the repo and BigQuery differ. Running it also reverts manual changes.
- Don't apply from a laptop. For a local read-only check:
  `terraform init && terraform plan` (uses your gcloud login).

## What is and isn't enforced

Bindings are **authoritative per table and role**. For every table listed here, whoever holds
`roles/bigquery.dataViewer` (or another listed role) on it is exactly the members in the
files. A grant added by hand in the console is removed on the next apply.

Not managed here, and so not protected from manual changes:
- tables that no file mentions (their IAM is untouched),
- roles other than the ones listed for a table,
- dataset-level and project-level IAM, and the `analytics_*` GA4 export datasets.

A view needs access to the tables it reads (or to be an authorized view); granting a view does
not grant its source tables.

An apply fails if a listed table doesn't exist yet. If a pipeline drops and recreates a table,
its table-level IAM can be lost until the next apply: re-run the apply workflow.

