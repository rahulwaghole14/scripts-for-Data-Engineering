variable "env" {
  description = "Deployment environment (prod or dev)"
  type        = string
  default     = "dev"
}

variable "gcloud_organization_id" {
  description = "The GCP organization ID"
  type        = string
  sensitive = true
}

variable "gcloud_project_dev" {
  description = "The GCP project ID"
  type        = string
  sensitive = true
}

variable "service_account1" {
  description = "The GCP service account1"
  type        = string
  sensitive = true
}

variable "vpn_ip_subnet1" {
  description = "The VPN IP subnet1"
  type        = string
  sensitive = true
}

variable "gcloud_connection_id" {
  description = "The GCP connection ID"
  type        = string
  sensitive = true
}

variable "gcloud_project_name_dev" {
  description = "The GCP project name"
  type        = string
  sensitive = true
}
