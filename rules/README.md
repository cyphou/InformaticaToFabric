# Rule Definition Schema — Sprint 76
#
# Each rule file contains a list of conversion rules under the 'rules' key.
# See schema.yaml for the full specification.
#
# Example:
#   rules:
#     - name: "Convert NVL"
#       category: sql
#       db_type: oracle
#       match: "\\bNVL\\s*\\("
#       action: replace
#       output: "COALESCE("
#       priority: 50
#       enabled: true
