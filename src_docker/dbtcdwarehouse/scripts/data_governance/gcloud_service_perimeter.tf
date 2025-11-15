# resource "google_access_context_manager_service_perimeter" "bigquery-service-perimeter" {
#   parent = "accessPolicies/${google_access_context_manager_access_policy.hexa-data-access-policy.name}"
#   name   = "accessPolicies/${google_access_context_manager_access_policy.hexa-data-access-policy.name}/servicePerimeters/restrict_bigquery_access"
#   title  = "restrict_bigquery_access"


#   status {
#     restricted_services = ["bigquery.googleapis.com"]
#     resources = ["projects/${var.gcloud_project_dev}"]
#     access_levels = ["accessPolicies/${google_access_context_manager_access_policy.hexa-data-access-policy.name}/accessLevels/bigquery_access_level"]

#   }

#   use_explicit_dry_run_spec = true

# }

# resource "google_access_context_manager_access_policy" "hexa-data-access-policy" {
#   parent = "organizations/${var.gcloud_organization_id}"
#   title  = "hexa-data-access-policy-scoped"
#   scopes = ["projects/${var.gcloud_project_dev}"]
# }

# resource "google_access_context_manager_access_level" "bigquery-access-level" {
#   parent = "accessPolicies/${google_access_context_manager_access_policy.hexa-data-access-policy.name}"
#   name   = "accessPolicies/${google_access_context_manager_access_policy.hexa-data-access-policy.name}/accessLevels/bigquery_access_level"
#   title  = "bigquery_access_level"

#   basic {
#     conditions {
#       ip_subnetworks = ["${var.vpn_ip_subnet1}"]
#       members = ["serviceAccount:${var.service_account1}"]

#     }
#   }

# }
