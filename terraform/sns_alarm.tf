# CRITICAL Errors for database connection -trigger 4 events/hr
resource "aws_sns_topic" "critical_db_connection_error" {
    name = "critical_event"
}

resource "aws_sns_topic_subscription" "error_subscription" {
  topic_arn = aws_sns_topic.critical_db_connection_error.arn
  protocol  = "email"
  endpoint  = "david.luke.de-202307@northcoders.net"  
  
}

resource "aws_cloudwatch_log_metric_filter" "logging_error_filter" {
    name = "critical_event_error"
    pattern = "[CRITICAL]"
    log_group_name = aws_cloudwatch_log_group.warehouse_log_group.name

    metric_transformation {
        name = "ErrorFilter"
        namespace = "postgressSQL_Connection_Errors"
        value = "1"
        }
}

resource "aws_cloudwatch_log_metric_filter" "lambda1_logging_error_filter" {
    name = "critical_event_error"
    pattern = "[CRITICAL]"
    log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name

    metric_transformation {
        name = "ErrorFilter"
        namespace = "postgressSQL_Connection_Errors"
        value = "1"
        }
}

resource "aws_cloudwatch_metric_alarm" "logging_error_alarm" {
    alarm_name = "critical_error_alarm"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = "1"
    metric_name = "ErrorFilter"
    namespace = "postgressSQL_Connection_Errors"
    period = "3600"
    statistic = "Sum"
    threshold = "4"
    alarm_description = "Critical Error"
    alarm_actions = [aws_sns_topic.critical_db_connection_error.arn]
}

# ERRORS for aws S3 buckests errors - trigger 45 events/8hr (1728000s)

resource "aws_sns_topic" "s3_error_topic" {
    name = "s3_error_event"
}

resource "aws_sns_topic_subscription" "s3_error_subscription" {
  topic_arn = aws_sns_topic.s3_error_topic.arn
  protocol  = "email"
  endpoint  = "david.luke.de-202307@northcoders.net"  
}

resource "aws_cloudwatch_log_metric_filter" "s3_ingestion_error_filter" {
    name           = "s3_error_event"
    pattern        = "[ERROR]"
    log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name

    metric_transformation {
        name      = "S3ErrorFilter"
        namespace = "S3_Errors"
        value     = "1"
    }
}

resource "aws_cloudwatch_log_metric_filter" "s3_processed_error_filter" {
    name           = "s3_error_event"
    pattern        = "[ERROR]"
    log_group_name = aws_cloudwatch_log_group.processed_log_group.name

    metric_transformation {
        name      = "S3ErrorFilter"
        namespace = "S3_Errors"
        value     = "1"
    }
}

resource "aws_cloudwatch_metric_alarm" "s3_error_alarm" {
    alarm_name          = "s3_error_alarm"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods  = "1"
    metric_name         = "S3ErrorFilter"
    namespace           = "S3_Errors"
    period              = "28800" 
    statistic           = "Sum"
    threshold           = "45"
    alarm_description   = "S3 Error"
    alarm_actions       = [aws_sns_topic.s3_error_topic.arn]
}