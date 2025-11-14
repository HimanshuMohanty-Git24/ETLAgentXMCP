"""
Tools package for Medallion ETL system.
Provides utilities for Databricks, GitHub, and context analysis.
"""

from .databricks_tools import (
    get_table_schema,
    validate_sql_syntax,
    check_table_exists,
    get_table_stats,
    list_catalogs_schemas,
    execute_sql_query
)

from .github_tools import (
    create_github_pr,
    check_pr_status,
    merge_pr,
    create_branch,
    commit_and_push
)

from .medallion_tools import (
    create_bronze_table,
    execute_silver_transformation,
    execute_gold_transformation,
    optimize_table,
    analyze_table
)

from .context_tools import (
    analyze_layer_output,
    extract_schema_metadata,
    compute_data_quality_score,
    generate_lineage_graph
)

__all__ = [
    # Databricks tools
    "get_table_schema",
    "validate_sql_syntax",
    "check_table_exists",
    "get_table_stats",
    "list_catalogs_schemas",
    "execute_sql_query",
    
    # GitHub tools
    "create_github_pr",
    "check_pr_status",
    "merge_pr",
    "create_branch",
    "commit_and_push",
    
    # Medallion tools
    "create_bronze_table",
    "execute_silver_transformation",
    "execute_gold_transformation",
    "optimize_table",
    "analyze_table",
    
    # Context tools
    "analyze_layer_output",
    "extract_schema_metadata",
    "compute_data_quality_score",
    "generate_lineage_graph",
]
