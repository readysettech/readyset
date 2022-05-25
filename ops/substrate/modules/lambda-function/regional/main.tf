# managed by Substrate; do not edit by hand

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = 1
}

resource "aws_lambda_function" "function" {
  depends_on = [aws_cloudwatch_log_group.lambda]
  environment {
    variables = merge(
      { "PREVENT_EMPTY_ENVIRONMENT" = "lambda:CreateFunction fails when given an empty Environment" },
      var.environment_variables,
    )
  }
  filename         = var.filename
  function_name    = var.name
  handler          = var.progname != "" ? var.progname : var.name
  memory_size      = 128 # default
  role             = var.role_arn
  runtime          = "go1.x"
  source_code_hash = filebase64sha256(var.filename)
  tags = {
    Name = var.name
  }
  timeout = 60
  vpc_config {
    security_group_ids = var.security_group_ids
    subnet_ids         = var.subnet_ids
  }
}

resource "aws_lambda_permission" "permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.function.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = var.apigateway_execution_arn
}
