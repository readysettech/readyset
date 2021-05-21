# managed by Substrate; do not edit by hand

output "tags" {
  value = {
    domain      = data.external.tags.result.Domain
    environment = data.external.tags.result.Environment
    quality     = data.external.tags.result.Quality
  }
}
