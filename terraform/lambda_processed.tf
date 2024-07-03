# STEP 1: zip the lambda functions (3 when done)
data "archive_file" "processed_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda_functions/processed_lambda.py"
  output_path = "${path.module}/../src/lambda_functions/processed_lambda.zip"
}
# STEP 2: zip the lambda layer that can be used by all 3 lambdas
data "archive_file" "parquet_layer" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_functions/parquet_layer"
  output_path = "${path.module}/../src/lambda_functions/parquet_layer.zip"
}
# STEP 3: save the zipped lambda functions in the s3 lambda bucket
resource "aws_s3_object" "processed_lambda" {
  bucket = aws_s3_bucket.lambda_bucket.id
  key    = "lambda/processed_lambda.zip"
  source = data.archive_file.processed_lambda.output_path
#   acl    = "private"
}
# STEP 4: save the zipped lambda layer (dependencies) in the s3 lambda bucket
resource "aws_s3_object" "parquet_layer" {
  bucket = aws_s3_bucket.lambda_bucket.id
  key    = "lambda_dependencies/parquet_layer.zip"
  source = data.archive_file.parquet_layer.output_path
}
# STEP 5:
resource "aws_lambda_layer_version" "parquet_layer" {
  layer_name = "parquet_layer"
  s3_bucket  = "lambda-bucket-1158020804995033"
  s3_key     = "lambda_dependencies/parquet_layer.zip"
  compatible_runtimes = ["python3.9"]
}
resource "aws_lambda_function" "processed_lambda" {
  function_name = var.processed_lambda
  role          = aws_iam_role.processed_lambda_role.arn
  handler       = "processed_lambda.lambda_handler"
  runtime       = "python3.9"
  s3_bucket     = "lambda-bucket-1158020804995033"
  s3_key        = "lambda/processed_lambda.zip"
  timeout       = 300
  layers        = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:8",aws_lambda_layer_version.parquet_layer.arn] 
}


# copied from terraform tasks....
resource "aws_lambda_permission" "allow_ingestion_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.processed_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "ingestion_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.processed_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_ingestion_s3]
}