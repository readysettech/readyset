data "utils_deep_merge_yaml" "values-file" {
  input = [
    local.base_ingress_config,
    local.ng_ingress_routing_rules
  ]
}
