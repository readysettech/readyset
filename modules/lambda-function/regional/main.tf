# managed by Substrate; do not edit by hand

data "archive_file" "zip" {
  output_path = var.filename
  source_file = "${data.external.dirname.result.dirname}/${var.name}"
  type        = "zip"
}

data "external" "dirname" {
  program = ["/bin/sh", "-c", "echo \"{\\\"dirname\\\":\\\"$(which substrate-create-admin-account | xargs dirname)\\\"}\""]
}

# TODO remove this data source on or after the 2021.03 release.
data "external" "gobin" {
  program = ["/bin/sh", "-c", "echo \"{\\\"GOBIN\\\":\\\"$GOBIN\\\"}\""]
}


resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = 1
  tags = {
    Manager = "Terraform"
  }
}

resource "aws_lambda_function" "function" {
  depends_on       = [aws_cloudwatch_log_group.lambda]
  filename         = var.filename
  function_name    = var.name
  handler          = var.name
  memory_size      = 128 # default
  role             = var.role_arn
  runtime          = "go1.x"
  source_code_hash = filebase64sha256(var.filename)
  tags = {
    Manager = "Terraform"
    Name    = var.name
  }
  timeout = 60
}

resource "aws_lambda_permission" "permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.function.function_name
  principal     = "apigateway.amazonaws.com"
  #source_arn    = var.apigateway_execution_arn
}
