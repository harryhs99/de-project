variable "unique_number" {
  type    = string
  default = "1158020804995033"
}
variable "ingestion_lambda" {
  type    = string
  default = "tote-s3-ingestion-lambda"
}
variable "processed_lambda" {
  type    = string
  default = "s3-ingestion-s3-processed-lambda"
}
variable "warehouse_lambda" {
  type    = string
  default = "s3-warehouse-lambda"
}