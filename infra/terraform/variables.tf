variable "aws_region" {
  default = "eu-west-1"
  type    = string
}

variable "landing_bucket" {
  default = "reaas-home-project-landing"
  type    = string
}
variable "data_bucket" {
  default = "reaas-home-project-data"
  type    = string
}

variable "minio_endpoint" {
  description = "MinIO server URL"
  type        = string
  default     = "http://localhost:9000"
}

variable "minio_access_key" {
  description = "MinIO access key"
  type        = string
  default     = ""
}

variable "minio_secret_key" {
  description = "MinIO secret key"
  type        = string
  default     = ""
  sensitive   = true
}

variable "use_minio" {
  default = true
  type    = bool
}