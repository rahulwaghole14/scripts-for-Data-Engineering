# Create a single taxonomy for sensitivity levels
resource "google_data_catalog_taxonomy" "data_governance" {
  display_name = "${var.env}_data_governance"
  description  = "Taxonomy for high sensitivity levels of personally identifiable information (PII)"
  region       = "australia-southeast1"
}
