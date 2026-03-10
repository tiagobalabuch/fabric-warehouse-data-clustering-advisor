"""
Fabric Warehouse Advisor — Data Clustering Advisor
=====================================================
Assesses and recommends which tables and columns should use
**Data Clustering** in Microsoft Fabric Warehouse.

Quick start::

    from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

    config = DataClusteringAdvisorConfig(warehouse_name="MyWarehouse")
    advisor = DataClusteringAdvisor(spark, config)
    result = advisor.run()

    print(result.text_report)
"""

from .advisor import DataClusteringAdvisor, AdvisorResult
from .config import DataClusteringAdvisorConfig
from .scoring import ColumnScore, TableRecommendation
from .data_type_support import DataTypeAssessment, assess_data_type

__all__ = [
    "DataClusteringAdvisor",
    "DataClusteringAdvisorConfig",
    "AdvisorResult",
    "ColumnScore",
    "TableRecommendation",
    "DataTypeAssessment",
    "assess_data_type",
]
