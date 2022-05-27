resource "auth0_client" "readyset-auth" {
  name                = "Readyset"
  description         = "Readyset Customer Authentication"
  app_type            = "non_interactive"
  callbacks           = [var.callback_url]
  allowed_logout_urls = [var.logout_url]
  oidc_conformant     = true
  is_first_party      = true
  logo_uri            = var.readyset_logo_uri

  jwt_configuration {
    alg = "RS256"
  }
}

resource "auth0_branding" "readyset_branding" {
  logo_url    = var.readyset_logo_uri
  favicon_url = var.readyset_logo_uri
}

resource "auth0_connection" "readyset-app-admin" {
  name     = "readyset-app-user-db"
  strategy = "auth0"
  options {
    password_policy        = "good"
    brute_force_protection = true
  }
  enabled_clients = [auth0_client.readyset-auth.id, var.auth0_client_id]
}

resource "auth0_resource_server" "readyset-app-resource-server" {
  name                                            = "Readyset App Resource Server"
  identifier                                      = var.auth0_api_identifier
  skip_consent_for_verifiable_first_party_clients = true
  token_dialect                                   = "access_token_authz"
  enforce_policies                                = true

  scopes {
    value       = "read:images:all"
    description = "Read all Docker images."
  }

  scopes {
    value       = "read:users:self"
    description = "Read information for your user."
  }

  scopes {
    value       = "read:users:all"
    description = "Read all user information."
  }
}

resource "auth0_client_grant" "readyset-app-client-grant" {
  client_id = auth0_client.readyset-auth.id
  audience  = auth0_resource_server.readyset-app-resource-server.identifier
  scope     = ["read:images:all", "read:users:all", "read:users:self"]
}

resource "auth0_role" "readyset-app-admin-role" {
  name        = "admin_user"
  description = "Administrator"
  permissions {
    resource_server_identifier = auth0_resource_server.readyset-app-resource-server.identifier
    name                       = "read:images:all"
  }

  permissions {
    resource_server_identifier = auth0_resource_server.readyset-app-resource-server.identifier
    name                       = "read:users:all"
  }
}

resource "auth0_role" "readyset-app-free-tier-user-role" {
  name        = "single_node_user"
  description = "Free tier customer"
  permissions {
    resource_server_identifier = auth0_resource_server.readyset-app-resource-server.identifier
    name                       = "read:images:all"
  }

  permissions {
    resource_server_identifier = auth0_resource_server.readyset-app-resource-server.identifier
    name                       = "read:users:self"
  }
}

resource "auth0_role" "readyset-app-audit-role" {
  name        = "audit_user"
  description = "User info auditor"
  permissions {
    resource_server_identifier = auth0_resource_server.readyset-app-resource-server.identifier
    name                       = "read:users:all"
  }
}

resource "auth0_user" "readyset-app-admin-user" {
  connection_name = auth0_connection.readyset-app-admin.name
  user_id         = "superadmin"
  email           = "superadmin@readyset.io"
  email_verified  = true
  password        = var.auth0_admin_user_password
  roles           = [auth0_role.readyset-app-admin-role.id]
}

resource "auth0_user" "readyset-app-auditor-user" {
  connection_name = auth0_connection.readyset-app-admin.name
  user_id         = "auditor"
  email           = "auditor@readyset.io"
  email_verified  = true
  password        = var.auth0_auditor_user_password
  roles           = [auth0_role.readyset-app-admin-role.id]
}

resource "auth0_rule" "readyset-app-free-tier-rule" {
  name = "free-tier-role-assignment"
  script = templatefile("${path.module}/free_tier_customer_rule.js", {
    TERRAFORM_ROLE_ID : auth0_role.readyset-app-free-tier-user-role.id
  })
  enabled = true
}