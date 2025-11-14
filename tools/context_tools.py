"""
Context Analysis Tools for layer output enrichment.

Provides tools for analyzing completed layer outputs to extract schema,
sample data, quality metrics, and other context for next layer planning.

Author: Data Engineering Team
Date: 2025-11-14
"""

from typing import Dict, Any, List
from langchain_core.tools import tool
from .databricks_tools import execute_sql_query, get_table_schema


@tool
def analyze_layer_output(
    table_name: str,
    layer_name: str,
    sample_size: int = 10
) -> Dict[str, Any]:
    """
    Perform comprehensive analysis of a completed layer's output.
    
    Extracts:
    - Schema with column names and types
    - Sample data rows
    - Row count
    - Data quality metrics (nulls, completeness)
    - Column statistics
    
    Args:
        table_name: Full table name (catalog.schema.table)
        layer_name: Layer name (bronze/silver/gold)
        sample_size: Number of sample rows to retrieve
        
    Returns:
        Dictionary with comprehensive layer analysis
    """
    try:
        print(f"[Context] Analyzing {layer_name.upper()} layer: {table_name}")
        
        # Parse table name
        parts = table_name.split(".")
        if len(parts) != 3:
            return {
                "status": "error",
                "error": "Invalid table name format. Expected catalog.schema.table"
            }
        
        catalog, schema, table = parts
        
        # Get schema information
        schema_info = get_table_schema(catalog=catalog, schema=schema, table=table)
        
        if schema_info["status"] != "success":
            return {
                "status": "error",
                "error": f"Failed to get schema: {schema_info.get('error')}"
            }
        
        # Get row count
        count_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        count_result = execute_sql_query(query=count_query)
        
        total_rows = 0
        if count_result["status"] == "success" and count_result["rows"]:
            total_rows = count_result["rows"][0][0]
        
        # Get sample data
        sample_query = f"SELECT * FROM {table_name} LIMIT {sample_size}"
        sample_result = execute_sql_query(query=sample_query)
        
        sample_data = []
        if sample_result["status"] == "success":
            # Convert rows to list of dicts
            columns = sample_result.get("columns", [])
            for row in sample_result.get("rows", []):
                sample_data.append(dict(zip(columns, row)))
        
        # Compute data quality metrics
        quality_metrics = compute_data_quality_score(
            table_name=table_name,
            columns=[col["name"] for col in schema_info["columns"]]
        )
        
        print(f"[Context] ✓ Analysis complete: {total_rows:,} rows, {len(schema_info['columns'])} columns")
        
        return {
            "status": "success",
            "layer": layer_name,
            "table_name": table_name,
            "schema": schema_info,
            "row_count": total_rows,
            "sample_data": sample_data,
            "sample_size": len(sample_data),
            "data_quality": quality_metrics
        }
    except Exception as e:
        return {
            "status": "error",
            "layer": layer_name,
            "table_name": table_name,
            "error": str(e)
        }


@tool
def extract_schema_metadata(table_name: str) -> Dict[str, Any]:
    """
    Extract detailed schema metadata from a table.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        
    Returns:
        Dictionary with schema metadata
    """
    try:
        parts = table_name.split(".")
        if len(parts) != 3:
            return {
                "status": "error",
                "error": "Invalid table name format"
            }
        
        catalog, schema, table = parts
        schema_info = get_table_schema(catalog=catalog, schema=schema, table=table)
        
        if schema_info["status"] == "success":
            # Extract column types and build metadata
            column_types = {}
            nullable_columns = []
            
            for col in schema_info["columns"]:
                column_types[col["name"]] = col["type"]
                if col.get("nullable", True):
                    nullable_columns.append(col["name"])
            
            return {
                "status": "success",
                "table_name": table_name,
                "columns": schema_info["columns"],
                "column_count": len(schema_info["columns"]),
                "column_types": column_types,
                "nullable_columns": nullable_columns
            }
        else:
            return schema_info
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "error": str(e)
        }


@tool
def compute_data_quality_score(table_name: str, columns: List[str]) -> Dict[str, Any]:
    """
    Compute data quality metrics for a table.
    
    Calculates:
    - Completeness ratio (non-null percentage)
    - Records with nulls
    - Null counts per column
    
    Args:
        table_name: Full table name (catalog.schema.table)
        columns: List of column names to analyze
        
    Returns:
        Dictionary with quality metrics
    """
    try:
        if not columns:
            return {
                "status": "error",
                "error": "No columns provided for quality analysis"
            }
        
        # Build query to count nulls per column
        null_checks = []
        for col in columns[:20]:  # Limit to first 20 columns
            null_checks.append(f"SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) as `{col}_nulls`")
        
        quality_query = f"""
        SELECT
            COUNT(*) as total_rows,
            {', '.join(null_checks)}
        FROM {table_name}
        """
        
        result = execute_sql_query(query=quality_query)
        
        if result["status"] != "success":
            return {
                "status": "error",
                "error": result.get("error")
            }
        
        if not result["rows"]:
            return {
                "status": "error",
                "error": "No data returned from quality query"
            }
        
        row = result["rows"][0]
        total_rows = row[0]
        
        if total_rows == 0:
            return {
                "status": "success",
                "completeness_ratio": 1.0,
                "total_rows": 0,
                "records_with_nulls": 0,
                "null_counts": {}
            }
        
        # Calculate null counts
        null_counts = {}
        total_nulls = 0
        
        for i, col in enumerate(columns[:20]):
            null_count = row[i + 1] if i + 1 < len(row) else 0
            null_counts[col] = null_count
            total_nulls += null_count
        
        # Calculate completeness
        total_cells = total_rows * min(len(columns), 20)
        completeness_ratio = 1.0 - (total_nulls / total_cells) if total_cells > 0 else 1.0
        
        # Count records with at least one null
        records_with_nulls_query = f"""
        SELECT COUNT(*) as records_with_nulls
        FROM {table_name}
        WHERE {' OR '.join([f'`{col}` IS NULL' for col in columns[:20]])}
        """
        
        records_result = execute_sql_query(query=records_with_nulls_query)
        records_with_nulls = 0
        
        if records_result["status"] == "success" and records_result["rows"]:
            records_with_nulls = records_result["rows"][0][0]
        
        return {
            "status": "success",
            "completeness_ratio": round(completeness_ratio, 4),
            "total_rows": total_rows,
            "records_with_nulls": records_with_nulls,
            "null_counts": null_counts,
            "columns_analyzed": min(len(columns), 20)
        }
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "error": str(e)
        }


@tool
def generate_lineage_graph(table_name: str) -> Dict[str, Any]:
    """
    Generate data lineage information for a table.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        
    Returns:
        Dictionary with lineage information
    """
    try:
        # For now, return basic lineage info
        # In production, this would query Unity Catalog's lineage API
        parts = table_name.split(".")
        if len(parts) != 3:
            return {
                "status": "error",
                "error": "Invalid table name format"
            }
        
        catalog, schema, table = parts
        layer = "unknown"
        
        if "bronze" in table.lower():
            layer = "bronze"
        elif "silver" in table.lower():
            layer = "silver"
        elif "gold" in table.lower():
            layer = "gold"
        
        return {
            "status": "success",
            "table_name": table_name,
            "layer": layer,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "lineage": {
                "upstream": [],
                "downstream": []
            }
        }
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "error": str(e)
        }
