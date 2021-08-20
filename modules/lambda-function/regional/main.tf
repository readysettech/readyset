# managed by Substrate; do not edit by hand

data "archive_file" "zip" {
  output_path = var.filename
  source_file = "${data.external.dirname.result.dirname}/${var.name}"
  type        = "zip"
}

data "external" "dirname" {
  program = ["/bin/sh", "-c", "echo \"{\\\"dirname\\\":\\\"$(which substrate-create-admin-account | xargs dirname)\\\"}\""]
}


resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = 1
}

resource "aws_lambda_function" "function" {
  depends_on       = [aws_cloudwatch_log_group.lambda]
  filename         = data.archive_file.zip.output_path
  function_name    = var.name
  handler          = var.name
  memory_size      = 128 # default
  role             = var.role_arn
  runtime          = "go1.x"
  source_code_hash = data.archive_file.zip.output_base64sha256
  tags = {
    Name = var.name
  }
  timeout = 60
}

resource "aws_lambda_permission" "permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.function.function_name
  principal     = "apigateway.amazonaws.com"
  #source_arn    = var.apigateway_execution_arn
}
