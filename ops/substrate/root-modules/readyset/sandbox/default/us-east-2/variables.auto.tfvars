# General
aws_region  = "us-east-2"
environment = "sandbox"
quality     = "default"
# Auth0 Frontend
auth0_frontend_ami_id          = "ami-05d708522ca6cbdfb"
auth0_client_id                = "eOBJRWf5nKfCAXdbvtIpHEtlesileuKo"
auth0_domain                   = "https://readyset.us.auth0.com"
auth0_audience                 = "https://console.dev.readyset.io/"
auth0_rs_app_client_secret_arn = "arn:aws:secretsmanager:us-east-2:069491470376:secret:auth0/readyset-app-PkslT0"
auth0_frontend_domain          = "console.dev"
auth0_frontend_issuer_base_url = "https://console.dev.readyset.io"
auth0_frontend_key_name        = "ops-20220608"
auth0_frontend_logout_uri      = "/"
auth0_frontend_num_replicas    = 1
auth0_frontend_redirect_uri    = "/api/callback"