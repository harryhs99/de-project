# Cloudwatch trigger 1 minute
resource "aws_cloudwatch_event_rule" "ingestion_event_rule" {
  name                = "ingestion-event-rule-${var.ingestion_lambda}-${var.unique_number}"
  schedule_expression = "rate(10 minutes)"
}
# attach the rule to the lambda
resource "aws_cloudwatch_event_target" "ingestion_event_target" {
  rule = aws_cloudwatch_event_rule.ingestion_event_rule.name
  arn  = aws_lambda_function.ingestion_lambda.arn
}
# allow EventBridge to invoke the lambda every minuite
resource "aws_lambda_permission" "allow_scheduler" {
  statement_id   = "AllowExecutionFromEventBridge"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.ingestion_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.ingestion_event_rule.arn
  source_account = data.aws_caller_identity.current.account_id
}


