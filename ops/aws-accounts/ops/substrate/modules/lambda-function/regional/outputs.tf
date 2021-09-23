# managed by Substrate; do not edit by hand

output "function_arn" {
  value = aws_lambda_function.function.arn
}

output "invoke_arn" {
  value = aws_lambda_function.function.invoke_arn
}

output "source_code_hash" {
  value = aws_lambda_function.function.source_code_hash
}
