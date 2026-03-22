"""
Security Check — OneLake Security Sync Health  (SEC-014)
==========================================================
Validates that OneLake security roles have been correctly
synchronised to the SQL analytics endpoint by inspecting
``sys.database_principals`` for ``ols_`` prefixed roles.

Checks
------
* No ``ols_`` roles found (sync may not have run)
* ``ols_`` roles present without corresponding OneLake roles (stale)
* OneLake roles missing corresponding ``ols_`` sync (incomplete sync)
* Summary of sync state

.. note::

   This check is gated behind ``edition == "LakeWarehouse"`` and
   queries the SQL endpoint via ``synapsesql()``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from pyspark.sql import SparkSession

from ....core.warehouse_reader import read_warehouse_query
from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_LOW,
    LEVEL_MEDIUM,
    LEVEL_HIGH,
    CATEGORY_ONELAKE_SECURITY_SYNC,
)


# -- SQL ----------------------------------------------------------------

_OLS_ROLES_QUERY = """
    SELECT
        dp.name           AS role_name,
        dp.principal_id,
        dp.create_date,
        dp.modify_date
    FROM sys.database_principals AS dp
    WHERE dp.type = 'R'
      AND dp.name LIKE 'ols[_]%'
"""


# -- Public API ---------------------------------------------------------

def check_onelake_security_sync(
    spark: SparkSession,
    warehouse_name: str,
    config: SecurityCheckConfig,
    onelake_role_names: Optional[List[str]] = None,
) -> List[Finding]:
    """Validate OneLake security sync state in the SQL endpoint.

    Parameters
    ----------
    spark : SparkSession
        An active PySpark session.
    warehouse_name : str
        Warehouse / SQL endpoint name.
    config : SecurityCheckConfig
        Advisor configuration.
    onelake_role_names : list[str] | None
        Names of OneLake data access roles fetched in SEC-012.
        Used for cross-referencing. ``None`` if SEC-012 was skipped.

    Returns
    -------
    list[Finding]
        Findings related to OneLake security sync health.
    """
    findings: List[Finding] = []

    # ── Query ols_ roles from SQL endpoint ───────────────────────
    try:
        df = read_warehouse_query(
            spark, warehouse_name, _OLS_ROLES_QUERY,
            workspace_id=config.workspace_id,
            warehouse_id=config.warehouse_id or config.sql_endpoint_id,
        )
        rows = df.collect()
    except Exception as exc:
        findings.append(Finding(
            level=LEVEL_LOW,
            category=CATEGORY_ONELAKE_SECURITY_SYNC,
            check_name="security_sync_query_failed",
            object_name=warehouse_name,
            message="Unable to query security sync roles.",
            detail=f"Query error: {exc}",
            recommendation=(
                "Ensure the SQL endpoint is accessible and you "
                "have permissions to query sys.database_principals."
            ),
        ))
        return findings

    ols_roles: Dict[str, Any] = {}
    for row in rows:
        role_name = row["role_name"]
        ols_roles[role_name] = {
            "principal_id": row["principal_id"],
            "create_date": str(row["create_date"]),
            "modify_date": str(row["modify_date"]),
        }

    # ── No ols_ roles at all ─────────────────────────────────────
    if not ols_roles:
        if onelake_role_names:
            # OneLake roles exist but sync hasn't happened
            findings.append(Finding(
                level=LEVEL_HIGH,
                category=CATEGORY_ONELAKE_SECURITY_SYNC,
                check_name="security_sync_missing",
                object_name=warehouse_name,
                message=(
                    "OneLake data access roles exist but no "
                    "corresponding sync roles (ols_*) were found "
                    "in the SQL endpoint."
                ),
                detail=(
                    f"OneLake roles: {', '.join(onelake_role_names)}. "
                    f"Expected ols_ prefixed roles in "
                    f"sys.database_principals but found none."
                ),
                recommendation=(
                    "Security sync may not have completed. Allow "
                    "up to 5 minutes for role definition changes "
                    "to propagate. If the issue persists, check "
                    "the SQL analytics endpoint access mode and "
                    "Security section for sync errors."
                ),
            ))
        else:
            findings.append(Finding(
                level=LEVEL_INFO,
                category=CATEGORY_ONELAKE_SECURITY_SYNC,
                check_name="no_ols_roles",
                object_name=warehouse_name,
                message=(
                    "No OneLake security sync roles (ols_*) found "
                    "in the SQL endpoint."
                ),
                detail=(
                    "This is expected if OneLake security has not "
                    "been enabled or if the SQL analytics endpoint "
                    "is using delegated identity mode."
                ),
            ))
        return findings

    # ── Summary ──────────────────────────────────────────────────
    ols_role_names = sorted(ols_roles.keys())
    findings.append(Finding(
        level=LEVEL_INFO,
        category=CATEGORY_ONELAKE_SECURITY_SYNC,
        check_name="security_sync_summary",
        object_name=warehouse_name,
        message=(
            f"Found {len(ols_roles)} security sync role(s) "
            f"in the SQL endpoint."
        ),
        detail=f"Sync roles: {', '.join(ols_role_names)}",
    ))

    # ── Cross-reference with OneLake roles ───────────────────────
    if onelake_role_names is not None:
        # Build case-insensitive lookup sets.
        # SQL sync roles are lower-cased (e.g. ols_defaultreader)
        # while API role names preserve original casing (e.g. DefaultReader).
        ol_names_lower = {n.lower() for n in onelake_role_names}

        # Check for stale ols_ roles without matching OneLake role
        for ols_name in ols_role_names:
            # Strip ols_ prefix to get the base name
            base_name = ols_name[4:]  # remove "ols_"
            # Case-insensitive match against OneLake role names
            matched = base_name.lower() in ol_names_lower
            # Also check for cross-lakehouse pattern: {guid}_{roleName}
            if not matched and "_" in base_name:
                # Could be ols_{guid}_{roleName} from a shortcut
                parts = base_name.split("_", 1)
                if len(parts) == 2:
                    # The second part might match an OneLake role
                    # from another lakehouse — this is expected
                    matched = True  # Don't flag cross-lakehouse roles

            if not matched:
                findings.append(Finding(
                    level=LEVEL_MEDIUM,
                    category=CATEGORY_ONELAKE_SECURITY_SYNC,
                    check_name="stale_sync_role",
                    object_name=ols_name,
                    message=(
                        f"Sync role '{ols_name}' has no matching "
                        f"OneLake data access role."
                    ),
                    detail=(
                        "This role may be stale if the corresponding "
                        "OneLake role was deleted. Stale sync roles "
                        "should be removed by the security sync "
                        "service automatically."
                    ),
                    recommendation=(
                        "If this role persists, review the OneLake "
                        "security configuration and re-trigger sync. "
                        "Check the Security folder in the SQL endpoint "
                        "Explorer for sync errors."
                    ),
                ))

        # Check for OneLake roles missing their ols_ counterpart
        ols_names_lower = {n.lower() for n in ols_role_names}
        for ol_name in onelake_role_names:
            expected = f"ols_{ol_name}".lower()
            found = any(
                ols_lower == expected
                or ols_lower.endswith(f"_{ol_name.lower()}")
                for ols_lower in ols_names_lower
            )
            if not found:
                findings.append(Finding(
                    level=LEVEL_MEDIUM,
                    category=CATEGORY_ONELAKE_SECURITY_SYNC,
                    check_name="missing_sync_role",
                    object_name=ol_name,
                    message=(
                        f"OneLake role '{ol_name}' has no "
                        f"corresponding sync role in the SQL endpoint."
                    ),
                    detail=(
                        f"Expected to find 'ols_{ol_name}' in "
                        f"sys.database_principals. The security sync "
                        f"may be incomplete or still in progress."
                    ),
                    recommendation=(
                        "Allow up to 5 minutes for sync to complete. "
                        "Verify that all role members have Fabric Read "
                        "permission on the lakehouse. Check Security "
                        "→ DB Roles (custom) in the SQL endpoint for "
                        "sync errors."
                    ),
                ))

    return findings
