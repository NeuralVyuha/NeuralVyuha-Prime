from typing import Dict, Set

# Permission Constants
PERM_ALERT_INGEST = "alert:ingest"
PERM_ALERT_READ = "alert:read"
PERM_ALERT_WRITE = "alert:write"
PERM_CASE_READ = "case:read"
PERM_CASE_WRITE = "case:write"
PERM_SEARCH_EXECUTE = "search:execute"
PERM_RULE_MANAGE = "rule:manage"
PERM_RULE_SIMULATE = "rule:simulate"
PERM_ADMIN_VIEW        = "admin:view"
PERM_GRAPH_READ        = "graph:read"
PERM_ADMIN_INTEGRATION = "admin:integration"   # Manage Cortex/MISP nodes (admin only)
PERM_NODE_STATUS_READ  = "node:status:read"    # View node health (admin + analyst)

# Role Definitions
ROLE_ADMIN = "ROLE_ADMIN"
ROLE_ANALYST = "ROLE_ANALYST"
ROLE_INGEST = "ROLE_INGEST"
ROLE_READ_ONLY = "ROLE_READ_ONLY"

# Role to Permission Mapping
ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    ROLE_ADMIN: {
        PERM_ALERT_INGEST,
        PERM_ALERT_READ,
        PERM_ALERT_WRITE,
        PERM_CASE_READ,
        PERM_CASE_WRITE,
        PERM_SEARCH_EXECUTE,
        PERM_RULE_MANAGE,
        PERM_RULE_SIMULATE,
        PERM_ADMIN_VIEW,
        PERM_GRAPH_READ,
        PERM_ADMIN_INTEGRATION,
        PERM_NODE_STATUS_READ,
    },
    ROLE_ANALYST: {
        PERM_ALERT_READ,
        PERM_ALERT_WRITE,
        PERM_CASE_READ,
        PERM_CASE_WRITE,
        PERM_SEARCH_EXECUTE,
        PERM_RULE_SIMULATE,
        PERM_GRAPH_READ,
        PERM_NODE_STATUS_READ,  # View-only integration status
    },
    ROLE_INGEST: {
        PERM_ALERT_INGEST
    },
    ROLE_READ_ONLY: {
        PERM_ALERT_READ,
        PERM_CASE_READ,
        PERM_SEARCH_EXECUTE
    }
}

def resolve_permissions(roles: list[str]) -> Set[str]:
    """
    Given a list of roles, return the union of all permissions.
    """
    permissions = set()
    for role in roles:
        # Handle cases where roles might be prefixed or not, or case-insensitive if needed
        # For now, we assume strict matching to the keys in ROLE_PERMISSIONS
        if role in ROLE_PERMISSIONS:
            permissions.update(ROLE_PERMISSIONS[role])
    return permissions
