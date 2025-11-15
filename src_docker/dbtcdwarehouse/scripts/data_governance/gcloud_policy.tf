resource "google_bigquery_datapolicy_data_policy" "data_policy_hashed" {
  location         = "australia-southeast1"
  data_policy_id   = "data_policy_hashed"
  policy_tag       = google_data_catalog_policy_tag.pii_masked.name
  data_policy_type = "DATA_MASKING_POLICY"
  data_masking_policy {
    predefined_expression = "SHA256"
  }
}
