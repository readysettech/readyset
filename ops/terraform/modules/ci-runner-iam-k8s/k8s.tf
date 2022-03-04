# Kubernetes role with all required in-cluster RBAC allowances
resource "kubernetes_role" "this" {
  for_each = var.k8s_role_grants
  metadata {
    name      = var.k8s_role_name
    namespace = each.key
    labels = {
      name = var.k8s_role_name
    }
  }
  dynamic "rule" {
    for_each = each.value
    content {
      api_groups = rule.value["api_groups"]
      resources  = rule.value["resources"]
      verbs      = rule.value["verbs"]
    }
  }
}

resource "kubernetes_role_binding" "this-rb" {
  for_each = var.k8s_role_grants
  metadata {
    name      = format("%s-rb", var.k8s_role_name)
    namespace = each.key
  }

  subject {
    kind      = "Group"
    name      = format("%s-%s", var.k8s_role_name, each.key)
    namespace = each.key
  }

  # Bind Role
  role_ref {
    kind      = "Role"
    name      = var.k8s_role_name
    api_group = "rbac.authorization.k8s.io"
  }
}
