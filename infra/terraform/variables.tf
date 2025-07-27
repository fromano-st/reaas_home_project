variable "aws_region" {
  default = "eu-west-1"
}

variable "landing_bucket" {
  default = "reaas-home-project-landing"
}
variable "data_bucket" {
  default = "reaas-home-project-data"
}

variable "minio_endpoint" {
  description = "MinIO server URL"
  type        = string
  default     = "http://localhost:9000"
}

variable "minio_access_key" {
  description = "MinIO access key"
  type        = string
  default     = "minioadmin"
}

variable "minio_secret_key" {
  description = "MinIO secret key"
  type        = string
  default     = "minioadmin"
}

variable "use_minio"{
  default = true
  type    = bool
}