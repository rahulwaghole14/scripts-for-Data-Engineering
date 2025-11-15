
resource "google_bigquery_connection" "connection" {
    connection_id = var.gcloud_connection_id
    project = var.gcloud_project_name_dev
    location = "australia-southeast1"
    cloud_resource {}
}

output "service_account_email" {
  value = google_bigquery_connection.connection.cloud_resource[0].service_account_id
}
