
# Ingestion Lambda Role
resource "aws_iam_role" "ingestion_lambda_role" {
  name               = "role-${var.ingestion_lambda}-${var.unique_number}"
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

# Ingestion lambda s3 policy document NOTE added sectrets permissions as well to lambda role
data "aws_iam_policy_document" "s3_ingestion_document" {
  statement {
    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
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
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.ingestion_log_group.name}:*"
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


# Ingestion lambda s3 policy
resource "aws_iam_policy" "s3_ingestion_policy" {
  name   = "s3-policy-${var.ingestion_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.s3_ingestion_document.json
}

# Ingestion lambda Attach policy to the lambda role
resource "aws_iam_role_policy_attachment" "ingestion_lambda_s3_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.s3_ingestion_policy.arn
}

# Ingestion cloudwatch document
data "aws_iam_policy_document" "cw_ingestion_document" {
  statement {
    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.ingestion_log_group.name}:*"
    ]
  }
}

# Ingestion cloudwatch policy
resource "aws_iam_policy" "cw_ingestion_policy" {
  name   = "cw-policy-${var.ingestion_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.cw_ingestion_document.json
}

# Ingestion attach cloudwatch policy to lambda role
resource "aws_iam_role_policy_attachment" "ingestion_cw_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.cw_ingestion_policy.arn
}

data "aws_iam_policy_document" "secrets_manager_document" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:eu-west-2:027026634773:secret:totesql-djxthH"]
  }
}

resource "aws_iam_policy" "secrets_manager_policy" {
  name   = "secrets-manager-policy-${var.ingestion_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.secrets_manager_document.json
}

resource "aws_iam_role_policy_attachment" "ingestion_secrets_manager_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_policy.arn
}

