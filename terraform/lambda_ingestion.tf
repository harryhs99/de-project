
# STEP 1: zip the lambda functions (3 when done)
data "archive_file" "ingestion_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda_functions/ingestion_lambda.py"
  output_path = "${path.module}/../src/lambda_functions/ingestion_lambda.zip"
}
# STEP 2: zip the lambda layer that can be used by all 3 lambdas
data "archive_file" "lambda_layer" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_functions/lambda_layer"
  output_path = "${path.module}/../src/lambda_functions/lambda_layer.zip"
}
# STEP 2b: zip the utils layer that can be used by ingestion lambda
# data "archive_file" "utils_layer" {
#   type        = "zip"
#   source_dir  = "${path.module}/../src/lambda_functions/utils_layer"
#   output_path = "${path.module}/../src/lambda_functions/utils_layer.zip"
# }
# STEP 3: save the zipped lambda functions in the s3 lambda bucket
resource "aws_s3_object" "ingestion_lambda" {
  bucket = aws_s3_bucket.lambda_bucket.id
  key    = "lambda/ingestion_lambda.zip"
  source = data.archive_file.ingestion_lambda.output_path
  # acl    = "private"
}

# STEP 4: save the zipped lambda layer (dependencies) in the s3 lambda bucket
resource "aws_s3_object" "lambda_layer" {
  bucket = aws_s3_bucket.lambda_bucket.id
  key    = "lambda_dependencies/lambda_layer.zip"
  source = data.archive_file.lambda_layer.output_path
}



# STEP 5: create the lambda layer from the zip in the s3 bucket
# resource "aws_lambda_layer_version" "lambda_layer" {
#   layer_name = "dependencies"
#   s3_bucket  = aws_s3_bucket.lambda_bucket.bucket
#   s3_key     = "lambda_dependencies/lambda_layer.zip"
#   compatible_runtimes = ["python3.9"]
# }
# STEP 5: create the lambda layer from the zip in the s3 bucket
resource "aws_lambda_layer_version" "lambda_layer" {
  layer_name = "lambda_layer"
  s3_bucket  = "lambda-bucket-1158020804995033"
  s3_key     = "lambda_dependencies/lambda_layer.zip"
  compatible_runtimes = ["python3.9"]
}
# STEP 5b: create the lambda layer from the zip in the s3 bucket
# resource "aws_lambda_layer_version" "utils_layer" {
#   layer_name = "utils_layer"
#   s3_bucket  = "lambda-bucket-1158020804995033"
#   s3_key     = "lambda_dependencies/utils_layer.zip"
#   compatible_runtimes = ["python3.9"]
# }

# STEP 6: create the lambdas from the saved files and grant roles and layers
resource "aws_lambda_function" "ingestion_lambda" {
  function_name = var.ingestion_lambda
  role          = aws_iam_role.ingestion_lambda_role.arn
  handler       = "ingestion_lambda.lambda_handler"
  runtime       = "python3.9"
  s3_bucket     = aws_s3_bucket.lambda_bucket.bucket
  s3_key        = "lambda/ingestion_lambda.zip"
  timeout       = 300
  layers        = [aws_lambda_layer_version.lambda_layer.arn] 
}


