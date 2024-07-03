# processed Lambda Role
resource "aws_iam_role" "processed_lambda_role" {
  name               = "role-${var.processed_lambda}-${var.unique_number}"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

# processed lambda s3 policy document
data "aws_iam_policy_document" "s3_processed_document" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
    ]
  }
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.processed_bucket.arn}/*",
    ]
  }
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.lambda_bucket.arn}/*",
    ]
  }
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.processed_log_group.name}:*"
    ]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}",
      "${aws_s3_bucket.processed_bucket.arn}"
    ]
  }
}

# Processed lambda s3 policy
resource "aws_iam_policy" "s3_processed_policy" {
  name   = "s3-policy-${var.processed_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.s3_processed_document.json
}

# Processed lambda Attach policy to the lambda role
resource "aws_iam_role_policy_attachment" "processed_lambda_s3_policy_attachment" {
  role       = aws_iam_role.processed_lambda_role.name
  policy_arn = aws_iam_policy.s3_processed_policy.arn
}

# Processed cloudwatch document
data "aws_iam_policy_document" "cw_processed_document" {
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.processed_log_group.name}:*"
    ]
  }
}

# Processedcloudwatch policy
resource "aws_iam_policy" "cw_processed_policy" {
  name   = "cw-policy-${var.processed_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.cw_processed_document.json
}

# Processed attach cloudwatch policy to lambda role
resource "aws_iam_role_policy_attachment" "processed_cw_policy_attachment" {
  role       = aws_iam_role.processed_lambda_role.name
  policy_arn = aws_iam_policy.cw_processed_policy.arn
}








