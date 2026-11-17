# terraform/starburst

Starburst Galaxy setup: clusters, catalogs, roles, service accounts and grants.

- `main.tf` – providers and state backend
- `variables.tf` – values shared by several files (GCP project, Mongo host and regions)
- `cluster.tf` – clusters and the catalogs attached to them
- `access.tf` – roles, service accounts, privilege grants
- `catalog_<name>.tf` – one file per connection (catalog)

Secrets live in Secret Manager (project `data-499514`) and are read as ephemeral values, so
they never land in state. Each `catalog_<name>.tf` names the secret it reads; the Galaxy API
token is in `starburst-galaxy-client-id` and `starburst-galaxy-client-secret`.

To add a connection:
1. Create its password secret in Secret Manager and give
   `terraform-ci@data-499514.iam.gserviceaccount.com` both `roles/secretmanager.secretAccessor`
   (value) and `roles/secretmanager.viewer` (metadata, to resolve `latest`) on it.
2. Create `catalog_<name>.tf` (copy an existing one of the same type).
3. Attach the catalog to its cluster in the Galaxy UI, and add it to `catalog_refs` of that
   cluster in `cluster.tf` too. Terraform ignores changes to `catalog_refs` (Galaxy reorders
   the list), so the code alone doesn't attach it.
4. Give roles access in `access.tf` (e.g. `catalog_rw_role`: Select and CreateSchema).

CI logs in to GCP without a key (Workload Identity Federation):
- `.github/workflows/terraform-plan.yml` – `plan` on PRs that touch `terraform/`. Never applies
  and holds no secrets: it runs as `terraform-plan` (read-only on the state), with
  `-refresh=false -var=plan_only=true`, so it compares the code with the state and never
  calls Galaxy.
- `.github/workflows/terraform-apply.yml` – manual (Actions → Terraform apply → Run workflow,
  branch `main`): plans against live Galaxy, then applies that plan if it succeeded and has
  changes; with no changes it only refreshes the state. Only this file run from `main` can
  log in as `terraform-ci`.

Local runs: `terraform plan` (live, with your gcloud login) or the PR mode
`terraform plan -refresh=false -lock=false -var=plan_only=true`.

Don't apply from a laptop. A plan with "to destroy" deletes things in Galaxy: check the PR
plan before merging and before running the apply.

Not managed here: the Airflow connection (`starburst_connection`, Astro UI), service account
passwords, and catalogs whose type the provider can't manage (`tpch`, `federated_*`, …).
