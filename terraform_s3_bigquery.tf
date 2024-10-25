# Terraform configuration to set up AWS S3 bucket and Google BigQuery dataset

# Provider configuration
provider "aws" {
  region = "us-west-2" # Update region as needed
}

provider "google" {
  project = "your-gcp-project-id" # Update with your Google Cloud project ID
  region  = "us-central1" # Update region as needed
}

# AWS S3 Bucket
resource "aws_s3_bucket" "renewable_insight_bucket" {
  bucket = "renewable-insight-data-bucket"
  acl    = "private"

  versioning {
    enabled = true
  }

  tags = {
    Name        = "renewable-insight-bucket"
    Environment = "Development"
  }
}

# Google BigQuery Dataset
resource "google_bigquery_dataset" "renewable_insight_dataset" {
  dataset_id  = "renewable_insight_data"
  location    = "US" # Update location as needed

  labels = {
    environment = "development"
  }
}

# Outputs
output "s3_bucket_name" {
  value = aws_s3_bucket.renewable_insight_bucket.bucket
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.renewable_insight_dataset.dataset_id
}
