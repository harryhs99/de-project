# warehouse Lambda Role
resource "aws_iam_role" "warehouse_lambda_role" {
  name               = "role-${var.warehouse_lambda}-${var.unique_number}"
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
# warehouse lambda s3 policy document
data "aws_iam_policy_document" "s3_warehouse_document" {

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
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:${resource.aws_cloudwatch_log_group.warehouse_log_group.name}:*"
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
# warehouse lambda s3 policy
resource "aws_iam_policy" "s3_warehouse_policy" {
  name   = "s3-policy-${var.warehouse_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.s3_warehouse_document.json
}
# warehouse lambda Attach policy to the lambda role
resource "aws_iam_role_policy_attachment" "warehouse_lambda_s3_policy_attachment" {
  role       = aws_iam_role.warehouse_lambda_role.name
  policy_arn = aws_iam_policy.s3_warehouse_policy.arn
}

# warehouse cloudwatch document
data "aws_iam_policy_document" "cw_warehouse_document" {
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
# warehouse cloudwatch policy
resource "aws_iam_policy" "cw_warehouse_policy" {
  name   = "cw-policy-${var.warehouse_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.cw_warehouse_document.json
}

# warehouse attach cloudwatch policy to lambda role
resource "aws_iam_role_policy_attachment" "warehouse_cw_policy_attachment" {
  role       = aws_iam_role.warehouse_lambda_role.name
  policy_arn = aws_iam_policy.cw_warehouse_policy.arn
}

data "aws_iam_policy_document" "warehouse_secrets_manager_document" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:eu-west-2:027026634773:secret:warehouseSQL-9nO1jI"]
  }
}

resource "aws_iam_policy" "warehouse_secrets_manager_policy" {
  name   = "secrets-manager-policy-${var.warehouse_lambda}-${var.unique_number}"
  policy = data.aws_iam_policy_document.warehouse_secrets_manager_document.json
}

resource "aws_iam_role_policy_attachment" "warehouse_secrets_manager_policy_attachment" {
  role       = aws_iam_role.warehouse_lambda_role.name
  policy_arn = aws_iam_policy.warehouse_secrets_manager_policy.arn
}

