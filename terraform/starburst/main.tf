terraform {
  required_version = ">= 1.11"

  required_providers {
    galaxy = {
      source  = "starburstdata/galaxy"
      version = "= 0.40.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 8.7"
    }
  }

  backend "gcs" {
    bucket = "data-499514-terraform-state"
    prefix = "starburst"
  }
}

provider "google" {
  project = var.gcp_project_id
}

locals {
  placeholder = "plan-only"
}

# Secrets are read from Secret Manager as ephemeral values: never stored in state.
ephemeral "google_secret_manager_secret_version" "galaxy_client_id" {
  count   = var.plan_only ? 0 : 1
  secret  = "starburst-galaxy-client-id"
  version = "latest"
}

ephemeral "google_secret_manager_secret_version" "galaxy_client_secret" {
  count   = var.plan_only ? 0 : 1
  secret  = "starburst-galaxy-client-secret"
  version = "latest"
}

provider "galaxy" {
  domain        = "https://progresspartners.galaxy.starburst.io"
  client_id     = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.galaxy_client_id[0].secret_data
  client_secret = var.plan_only ? local.placeholder : ephemeral.google_secret_manager_secret_version.galaxy_client_secret[0].secret_data
}
