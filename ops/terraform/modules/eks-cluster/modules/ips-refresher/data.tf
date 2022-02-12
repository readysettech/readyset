data "utils_deep_merge_yaml" "values-file" {
  input = [
    local.default_values,
    local.cluster_values,
    local.irsa_values,
  ]
}
