aws_region = "us-east-2"
resource_tags = {
  environment = "build"
}
environment = "build"

# Build/default VPC ID
vpc_id = "vpc-0adb26542fc16ab14"

# EKS cluster module inputs
cluster_name            = "rs-build-us-east-2"
cluster_version         = "1.21"
cluster_vpn_cidr_blocks = ["10.3.128.0/18"]
alb_acm_cert_arn        = "arn:aws:acm:us-east-2:305232526136:certificate/d75aa6a7-8d9b-4ba7-8443-5874aea3e2f7"
kubernetes_namespaces   = ["build"]
ns_ingress_routing_rules = {
  build = [{
    fqdn        = "echo.build.readyset.name",
    service     = "echo-service",
    servicePort = "80",
    },
    // Benchmark prometheus
    {
      fqdn        = "benchmark-prometheus.build.readyset.name",
      service     = "prom-stack-kube-prometheus-prometheus",
      servicePort = "9090",
    },
    // Benchmark Push Gateway
    {
      fqdn        = "benchmark-pushgw.build.readyset.name",
      service     = "prom-pushgw-prometheus-pushgateway",
      servicePort = "9091",
    },
    // Benchmark Grafana
    {
      fqdn        = "benchmark-grafana.build.readyset.name",
      service     = "prom-stack-grafana",
      servicePort = "80",
    }
  ]
}

# Workers
cluster_autoscaler_enabled = true
self_managed_node_group_defaults = {
  block_device_mappings = {
    xvda = {
      device_name = "/dev/xvda"
      ebs = {
        volume_size           = 100
        volume_type           = "gp2"
        delete_on_termination = true
      }
    }
  }
  instance_refresh = {
    strategy = "Rolling",
    preferences = {
      checkpoint_delay       = 600,
      checkpoint_percentages = [35, 70, 100],
      instance_warmup        = 240,
      min_healthy_percentage = 50,
    }
    triggers = ["tag"]
  }
  propagate_tags = [{
    key                 = "aws-node-termination-handler/managed"
    value               = true
    propagate_at_launch = true
    },
    {
      key                 = "k8s.io/cluster-autoscaler/enabled"
      propagate_at_launch = true
      value               = true
    },
    {
      # If copying, don't forget to change this to new cluster
      key                 = "k8s.io/cluster-autoscaler/rs-build-us-east-2"
      propagate_at_launch = true
      value               = "owned"
  }]
}
self_managed_node_group_configs = {
  build-k8s-general = {
    max_size             = 5,
    desired_size         = 1,
    instance_type        = "m5.large",
    bootstrap_extra_args = "--kubelet-extra-args '--node-labels=readyset.io/worker=general'"
  }
}

# ExternalDNS
external_dns_internal_enabled    = true
external_dns_private_zone_domain = "readyset.name"

# Benchmarking Prom/Grafana Stack
benchmark_prom_grafana_enabled = true
benchmark_prom_pushgw_enabled  = true
