provider "aws" {
  region = var.aws_region

  skip_credentials_validation = var.use_minio
  skip_metadata_api_check     = var.use_minio
  skip_requesting_account_id  = var.use_minio

  access_key = var.minio_access_key
  secret_key = var.minio_secret_key

  s3_use_path_style = var.use_minio

  endpoints {
    s3 = var.use_minio ? var.minio_endpoint : null
  }
}


resource "aws_s3_bucket" "landing_bucket" {
  bucket = var.landing_bucket
  #acl    = "private"

  # MinIO requires force_destroy
  force_destroy = var.use_minio
}
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.data_bucket
  #acl    = "private"

  # MinIO requires force_destroy
  force_destroy = var.use_minio
}



