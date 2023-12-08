FROM grafana/grafana:10.2.2
COPY dashboards /var/lib/grafana/dashboards/
COPY provisioning /etc/grafana/provisioning/
COPY --chmod=666 provisioning/datasources/default.yml /etc/grafana/provisioning/datasources/
COPY config /etc/grafana/
COPY --chmod=666 config/grafana.ini /etc/grafana/
COPY config-then-run.sh /

# Run our script
# It does things, then calls the Grafana entrypoint
ENTRYPOINT /config-then-run.sh
