# Reads every teams/*.yaml and turns it into table-level IAM bindings.
# See README.md for the file format and the request/approval process.

locals {
  team_files = fileset("${path.module}/teams", "*.yaml")

  teams = {
    for f in local.team_files :
    trimsuffix(f, ".yaml") => yamldecode(file("${path.module}/teams/${f}"))
  }

  # One row per (team, grant, table).
  grant_rows = flatten([
    for file, team in local.teams : [
      for grant in team.grants : [
        for table in grant.tables : {
          file    = file
          legacy  = try(team.legacy, false)
          members = team.members
          role    = grant.role
          reason  = try(grant.reason, "")
          sens    = try(grant.sensitivity, "")
          signoff = try(grant.signed_off_by, "")
          table   = table
        }
      ]
    ]
  ])

  # Individuals who already held grants before this process existed, per legacy file. This is
  # closed: a new `legacy: true` file, or a new user in an existing one, fails the plan until
  # it is added here, which is itself reviewed as an access decision.
  grandfathered_users = {
    legacy_alexandru_bogdan = ["user:alexandru.bogdan@progresspartners.net"]
    legacy_eugenia_dorn     = ["user:eugenia.dorn@progresspartners.net"]
  }

  # IAM bindings are authoritative per (table, role), so two teams on the same table and
  # role must be merged into one binding. Otherwise they would overwrite each other.
  bindings = {
    for key in distinct([for r in local.grant_rows : "${r.table}|${r.role}"]) :
    key => {
      table = split("|", key)[0]
      role  = split("|", key)[1]
      members = sort(distinct(flatten([
        for r in local.grant_rows : r.members if "${r.table}|${r.role}" == key
      ])))
    }
  }
}

# Fails the plan (so the PR check goes red) on a malformed or policy-breaking team file.
resource "terraform_data" "validate_config" {
  input = local.grant_rows

  lifecycle {
    precondition {
      condition     = alltrue([for r in local.grant_rows : contains(var.allowed_roles, r.role)])
      error_message = "A team file grants a role that is not in var.allowed_roles (${join(", ", var.allowed_roles)})."
    }

    precondition {
      condition     = alltrue([for r in local.grant_rows : can(regex("^[a-z0-9_]+\\.[A-Za-z0-9_]+$", r.table))])
      error_message = "Tables must be written as dataset.table (no project, no wildcards)."
    }

    precondition {
      condition     = alltrue([for r in local.grant_rows : length(trimspace(r.reason)) >= 10])
      error_message = "Every grant needs a business `reason` (at least 10 characters)."
    }

    # `sensitivity` and `signed_off_by` are optional; a non-"none" sensitivity needs a sign-off.
    precondition {
      condition = alltrue([
        for r in local.grant_rows : r.sens == "" || r.sens == "none" || length(trimspace(r.signoff)) > 0
      ])
      error_message = "A grant with a sensitivity other than \"none\" needs `signed_off_by:` (GitHub handle or name of the data owner)."
    }

    # Access goes to groups or service accounts. Individual users are tolerated only if they
    # are listed in local.grandfathered_users for that legacy file.
    precondition {
      condition = alltrue([
        for r in local.grant_rows : alltrue([
          for m in r.members :
          can(regex("^(group|serviceAccount):[^@\\s]+@[^@\\s]+$", m)) || (r.legacy && contains(try(local.grandfathered_users[r.file], []), m))
        ])
      ])
      error_message = "Members must be group:<email> or serviceAccount:<email>. user:<email> is only allowed for the grandfathered users in local.grandfathered_users."
    }

    precondition {
      condition     = alltrue([for r in local.grant_rows : length(r.members) > 0])
      error_message = "A team file has an empty `members` list."
    }
  }
}

resource "google_bigquery_table_iam_binding" "table_access" {
  for_each = local.bindings

  project    = var.gcp_project_id
  dataset_id = split(".", each.value.table)[0]
  table_id   = split(".", each.value.table)[1]
  role       = each.value.role
  members    = each.value.members

  depends_on = [terraform_data.validate_config]
}
