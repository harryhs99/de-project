# Ingestion cloudwatch log group NOTE /lambda/ fixed it all!!!!!!
resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/${var.ingestion_lambda}"
}

# Processed cloudwatch log group 
resource "aws_cloudwatch_log_group" "processed_log_group" {
  name = "/aws/lambda/${var.processed_lambda}"
}

# Warehouse cloudwatch log group 
resource "aws_cloudwatch_log_group" "warehouse_log_group" {
  name = "/aws/lambda/${var.warehouse_lambda}"
}
