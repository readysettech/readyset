apiVersion: v1
kind: ConfigMap
metadata:
  name: aws-auth
  namespace: kube-system
data:
  mapRoles: |
    ${indent(4, mapRoles)}
  mapUsers: |
    ${indent(4, mapUsers)}
  mapAccounts: |
    ${indent(4, mapAccounts)}
