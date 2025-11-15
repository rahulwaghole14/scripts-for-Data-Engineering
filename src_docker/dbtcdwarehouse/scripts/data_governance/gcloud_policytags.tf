resource "google_data_catalog_policy_tag" "pii_masked" {
  taxonomy = google_data_catalog_taxonomy.data_governance.id
  display_name = "pii_masked"
  description = "A policy tag category used for high sensitivity data"
}
