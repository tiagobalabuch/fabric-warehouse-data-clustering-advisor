"""
Fabric Warehouse Advisor — Performance Check Advisor
=======================================================
Analyses common performance pitfalls in Microsoft Fabric Warehouses
including data-type anti-patterns, caching configuration, V-Order state,
and statistics health.

Quick start::

    from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

    config = PerformanceCheckConfig(warehouse_name="MyWarehouse")
    advisor = PerformanceCheckAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
"""

from .advisor import PerformanceCheckAdvisor, PerformanceCheckResult
from .config import PerformanceCheckConfig
from .findings import Finding, CheckSummary

__all__ = [
    "PerformanceCheckAdvisor",
    "PerformanceCheckConfig",
    "PerformanceCheckResult",
    "Finding",
    "CheckSummary",
]
