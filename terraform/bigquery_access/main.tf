terraform {
  required_version = ">= 1.11"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.7"
    }
  }

  backend "gcs" {
    bucket = "data-499514-terraform-state"
    prefix = "bigquery-access"
  }
}

provider "google" {
  project = var.gcp_project_id
}
