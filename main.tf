terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure Terraform to talk to MinIO instead of real AWS
provider "aws" {
  access_key                  = "admin"
  secret_key                  = "password123"
  region                      = "us-east-1"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  endpoints {
    s3 = "http://localhost:9000"
  }
}

# Bronze Layer: Raw incoming streaming data
resource "aws_s3_bucket" "bronze" {
  bucket = "raw-transactions"
}

# Silver Layer: Cleansed, deduplicated tables
resource "aws_s3_bucket" "silver" {
  bucket = "cleansed-transactions"
}

# Gold Layer: Final risk aggregations for BI
resource "aws_s3_bucket" "gold" {
  bucket = "risk-aggregates"
}