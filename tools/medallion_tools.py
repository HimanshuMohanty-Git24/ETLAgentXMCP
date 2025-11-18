"""
Medallion Architecture Tools for Bronze/Silver/Gold transformations.

Provides layer-specific transformation execution functions that run
SQL queries on Databricks SQL Warehouse.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from typing import Dict, Any, List
from langchain_core.tools import tool
from .databricks_tools import execute_sql_query


@tool
def create_bronze_table(
    source_table: str,
    target_table: str,
    sql_queries: List[str]
) -> Dict[str, Any]:
    """
    Create Bronze layer table with raw data ingestion.
    
    Executes SQL queries to create Bronze table with:
    - Raw data from source
    - Audit columns (ingestion_timestamp, source_file, row_id)
    - Delta Lake format with Change Data Feed
    
    Args:
        source_table: Source table name (catalog.schema.table)
        target_table: Target Bronze table name
        sql_queries: List of SQL queries to execute
        
    Returns:
        Dictionary with execution results
    """
    try:
        results = []
        total_rows = 0
        
        for i, query in enumerate(sql_queries):
            print(f"[Bronze] Executing query {i+1}/{len(sql_queries)}...")
            result = execute_sql_query(query=query, max_wait_seconds=300)
            results.append(result)
            
            if result["status"] != "success":
                return {
                    "status": "error",
                    "layer": "bronze",
                    "source_table": source_table,
                    "target_table": target_table,
                    "error": f"Query {i+1} failed: {result.get('error')}",
                    "failed_query_index": i,
                    "results": results
                }
        
        # Get row count
        count_query = f"SELECT COUNT(*) FROM {target_table}"
        count_result = execute_sql_query(query=count_query)
        
        if count_result["status"] == "success" and count_result["rows"]:
            total_rows = count_result["rows"][0][0]
        
        print(f"[Bronze] [OK] Table created: {target_table} ({total_rows:,} rows)")
        
        return {
            "status": "success",
            "layer": "bronze",
            "source_table": source_table,
            "target_table": target_table,
            "rows_processed": total_rows,
            "queries_executed": len(sql_queries),
            "results": results
        }
    except Exception as e:
        return {
            "status": "error",
            "layer": "bronze",
            "source_table": source_table,
            "target_table": target_table,
            "error": str(e)
        }


@tool
def execute_silver_transformation(
    source_table: str,
    target_table: str,
    sql_queries: List[str]
) -> Dict[str, Any]:
    """
    Execute Silver layer transformation with data cleaning and validation.
    
    Executes SQL queries for Silver layer:
    - Deduplication
    - Data type conversions
    - Null handling
    - Data quality scoring
    - Validation rules
    
    Args:
        source_table: Source Bronze table name
        target_table: Target Silver table name
        sql_queries: List of SQL queries to execute
        
    Returns:
        Dictionary with execution results
    """
    try:
        results = []
        total_rows = 0
        
        for i, query in enumerate(sql_queries):
            print(f"[Silver] Executing query {i+1}/{len(sql_queries)}...")
            result = execute_sql_query(query=query, max_wait_seconds=300)
            results.append(result)
            
            if result["status"] != "success":
                return {
                    "status": "error",
                    "layer": "silver",
                    "source_table": source_table,
                    "target_table": target_table,
                    "error": f"Query {i+1} failed: {result.get('error')}",
                    "failed_query_index": i,
                    "results": results
                }
        
        # Get row count and quality metrics
        count_query = f"SELECT COUNT(*) FROM {target_table}"
        count_result = execute_sql_query(query=count_query)
        
        if count_result["status"] == "success" and count_result["rows"]:
            total_rows = count_result["rows"][0][0]
        
        # Get quality score if column exists
        quality_query = f"""
        SELECT AVG(data_quality_score) as avg_quality
        FROM {target_table}
        WHERE data_quality_score IS NOT NULL
        """
        quality_result = execute_sql_query(query=quality_query)
        
        avg_quality = None
        if quality_result["status"] == "success" and quality_result["rows"]:
            avg_quality = quality_result["rows"][0][0]
        
        print(f"[Silver] [OK] Table created: {target_table} ({total_rows:,} rows)")
        if avg_quality:
            print(f"[Silver] [OK] Average data quality score: {avg_quality:.2f}")
        
        return {
            "status": "success",
            "layer": "silver",
            "source_table": source_table,
            "target_table": target_table,
            "rows_processed": total_rows,
            "avg_quality_score": avg_quality,
            "queries_executed": len(sql_queries),
            "results": results
        }
    except Exception as e:
        return {
            "status": "error",
            "layer": "silver",
            "source_table": source_table,
            "target_table": target_table,
            "error": str(e)
        }


@tool
def execute_gold_transformation(
    source_table: str,
    target_table: str,
    sql_queries: List[str]
) -> Dict[str, Any]:
    """
    Execute Gold layer transformation with business aggregations.
    
    Executes SQL queries for Gold layer:
    - Aggregations (SUM, AVG, COUNT)
    - Business KPIs
    - Dimension/Fact tables
    - Denormalization
    - Business logic application
    
    Args:
        source_table: Source Silver table name
        target_table: Target Gold table name
        sql_queries: List of SQL queries to execute
        
    Returns:
        Dictionary with execution results
    """
    try:
        results = []
        total_rows = 0
        
        for i, query in enumerate(sql_queries):
            print(f"[Gold] Executing query {i+1}/{len(sql_queries)}...")
            result = execute_sql_query(query=query, max_wait_seconds=300)
            results.append(result)
            
            if result["status"] != "success":
                return {
                    "status": "error",
                    "layer": "gold",
                    "source_table": source_table,
                    "target_table": target_table,
                    "error": f"Query {i+1} failed: {result.get('error')}",
                    "failed_query_index": i,
                    "results": results
                }
        
        # Get row count
        count_query = f"SELECT COUNT(*) FROM {target_table}"
        count_result = execute_sql_query(query=count_query)
        
        if count_result["status"] == "success" and count_result["rows"]:
            total_rows = count_result["rows"][0][0]
        
        print(f"[Gold] [OK] Table created: {target_table} ({total_rows:,} rows)")
        
        return {
            "status": "success",
            "layer": "gold",
            "source_table": source_table,
            "target_table": target_table,
            "rows_processed": total_rows,
            "queries_executed": len(sql_queries),
            "results": results
        }
    except Exception as e:
        return {
            "status": "error",
            "layer": "gold",
            "source_table": source_table,
            "target_table": target_table,
            "error": str(e)
        }


@tool
def optimize_table(table_name: str, zorder_columns: List[str] = None) -> Dict[str, Any]:
    """
    Optimize Delta table with OPTIMIZE and optional Z-ORDER.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        zorder_columns: Optional list of columns for Z-ORDER BY
        
    Returns:
        Dictionary with optimization results
    """
    try:
        # Run OPTIMIZE
        optimize_query = f"OPTIMIZE {table_name}"
        if zorder_columns:
            zorder_cols = ", ".join(zorder_columns)
            optimize_query += f" ZORDER BY ({zorder_cols})"
        
        print(f"[Optimize] Running OPTIMIZE on {table_name}...")
        result = execute_sql_query(query=optimize_query, max_wait_seconds=600)
        
        if result["status"] == "success":
            print(f"[Optimize] [OK] Table optimized: {table_name}")
            return {
                "status": "success",
                "table_name": table_name,
                "zorder_columns": zorder_columns,
                "result": result
            }
        else:
            return {
                "status": "error",
                "table_name": table_name,
                "error": result.get("error")
            }
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "error": str(e)
        }


@tool
def analyze_table(table_name: str) -> Dict[str, Any]:
    """
    Run ANALYZE TABLE to compute statistics.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        
    Returns:
        Dictionary with analysis results
    """
    try:
        analyze_query = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS"
        
        print(f"[Analyze] Computing statistics for {table_name}...")
        result = execute_sql_query(query=analyze_query, max_wait_seconds=300)
        
        if result["status"] == "success":
            print(f"[Analyze] [OK] Statistics computed: {table_name}")
            return {
                "status": "success",
                "table_name": table_name,
                "result": result
            }
        else:
            return {
                "status": "error",
                "table_name": table_name,
                "error": result.get("error")
            }
    except Exception as e:
        return {
            "status": "error",
            "table_name": table_name,
            "error": str(e)
        }
