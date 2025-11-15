terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.26.0"
    }
  }
  backend "gcs" {
    # parameters for the GCS backend set during the init command
    # terraform init -backend-config=backend.hcl
  }
}

provider "google" {
  # if env/target is dev then use dev credentials
  credentials = "./service-account-key-prod.json"
  project     = var.env == "prod" ? "hexa-data-report-etl-prod" : "hexa-data-report-etl-dev"
  region      = "australia-southeast1"
}
