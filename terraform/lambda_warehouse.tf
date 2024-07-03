# STEP 1: zip the lambda functions (3 when done)
data "archive_file" "warehouse_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda_functions/warehouse_lambda.py"
  output_path = "${path.module}/../src/lambda_functions/warehouse_lambda.zip"
}

# STEP 2: save the zipped lambda functions in the s3 lambda bucket
resource "aws_s3_object" "warehouse_lambda" {
  bucket = aws_s3_bucket.lambda_bucket.id
  key    = "lambda/warehouse_lambda.zip"
  source = data.archive_file.warehouse_lambda.output_path
}

resource "aws_lambda_function" "warehouse_lambda" {
  function_name = var.warehouse_lambda
  role          = aws_iam_role.warehouse_lambda_role.arn
  handler       = "warehouse_lambda.lambda_handler"
  runtime       = "python3.9"
  s3_bucket     = "lambda-bucket-1158020804995033"
  s3_key        = "lambda/warehouse_lambda.zip"
  timeout       = 900
  layers        = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:8"] 
}
# copied from terraform tasks....
resource "aws_lambda_permission" "allow_processed_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.warehouse_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.processed_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "processed_bucket_notification" {
  bucket = aws_s3_bucket.processed_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.warehouse_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_processed_s3]
}
