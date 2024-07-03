resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "ingestion-bucket-${var.unique_number}"
  tags = {
    Name = "gooseberry"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = "processed-bucket-${var.unique_number}"
  tags = {
    Name = "gooseberry"
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket" "lambda_bucket" {
  bucket = "lambda-bucket-${var.unique_number}"
  tags = {
    Name = "gooseberry"
  }
  lifecycle {
    prevent_destroy = true
  }
}