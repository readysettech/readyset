global
    daemon
    maxconn 256
defaults
    mode http
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms
frontend readyset_frontend
    bind *:5432
    mode tcp
    option tcplog
    default_backend readyset_backend
backend readyset_backend
    mode tcp
    balance roundrobin
    ${servers}
