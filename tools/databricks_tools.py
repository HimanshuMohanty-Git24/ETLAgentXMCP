"""
Databricks Tools for SQL Warehouse and Unity Catalog operations.

Provides tools for schema inspection, SQL validation, and query execution
using the Databricks SDK and SQL Warehouse.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from typing import Dict, List, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from langchain_core.tools import tool
import time


# Initialize Databricks Workspace Client
w = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")


@tool
def get_table_schema(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Get detailed schema information for a Unity Catalog table.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        Dictionary containing schema details including columns, types, and metadata
    """
    try:
        full_name = f"{catalog}.{schema}.{table}"
        table_info = w.tables.get(full_name=full_name)
        
        columns = []
        if table_info.columns:
            for col in table_info.columns:
                columns.append({
                    "name": col.name,
                    "type": col.type_name.value if col.type_name else "UNKNOWN",
                    "nullable": col.nullable if col.nullable is not None else True,
                    "comment": col.comment or ""
                })
        
        return {
            "status": "success",
            "full_name": full_name,
            "table_type": table_info.table_type.value if table_info.table_type else "UNKNOWN",
            "columns": columns,
            "column_count": len(columns),
            "storage_location": table_info.storage_location,
            "owner": table_info.owner,
            "comment": table_info.comment or ""
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "full_name": f"{catalog}.{schema}.{table}"
        }


@tool
def validate_sql_syntax(code: str, dialect: str = "spark") -> Dict[str, Any]:
    """
    Validate SQL syntax using Databricks SQL Warehouse.
    
    Args:
        code: SQL code to validate
        dialect: SQL dialect (default: spark)
        
    Returns:
        Dictionary with validation results
    """
    try:
        # Use EXPLAIN to validate syntax without execution
        validation_query = f"EXPLAIN {code}"
        
        statement = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=validation_query,
            wait_timeout="30s"
        )
        
        if statement.status.state == StatementState.SUCCEEDED:
            return {
                "valid": True,
                "message": "SQL syntax is valid",
                "dialect": dialect
            }
        else:
            return {
                "valid": False,
                "error": statement.status.error.message if statement.status.error else "Unknown error",
                "dialect": dialect
            }
    except Exception as e:
        error_msg = str(e)
        return {
            "valid": False,
            "error": error_msg,
            "dialect": dialect
        }


@tool
def check_table_exists(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Check if a table exists in Unity Catalog.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        Dictionary with existence check result
    """
    try:
        full_name = f"{catalog}.{schema}.{table}"
        table_info = w.tables.get(full_name=full_name)
        
        return {
            "exists": True,
            "full_name": full_name,
            "table_type": table_info.table_type.value if table_info.table_type else "UNKNOWN"
        }
    except Exception as e:
        return {
            "exists": False,
            "full_name": f"{catalog}.{schema}.{table}",
            "error": str(e)
        }


@tool
def get_table_stats(catalog: str, schema: str, table: str) -> Dict[str, Any]:
    """
    Get statistics for a Unity Catalog table.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        Dictionary with table statistics
    """
    try:
        full_name = f"{catalog}.{schema}.{table}"
        
        # Get row count
        count_query = f"SELECT COUNT(*) as row_count FROM {full_name}"
        result = execute_sql_query(query=count_query)
        
        row_count = 0
        if result["status"] == "success" and result["rows"]:
            row_count = result["rows"][0][0]
        
        # Get table size
        describe_query = f"DESCRIBE DETAIL {full_name}"
        detail_result = execute_sql_query(query=describe_query)
        
        size_bytes = 0
        num_files = 0
        if detail_result["status"] == "success" and detail_result["rows"]:
            # Parse DESCRIBE DETAIL output
            for row in detail_result["rows"]:
                if len(row) > 5:
                    size_bytes = row[5] if row[5] else 0
                if len(row) > 6:
                    num_files = row[6] if row[6] else 0
        
        return {
            "status": "success",
            "full_name": full_name,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 2),
            "num_files": num_files
        }
    except Exception as e:
        return {
            "status": "error",
            "full_name": f"{catalog}.{schema}.{table}",
            "error": str(e)
        }


@tool
def list_catalogs_schemas() -> Dict[str, Any]:
    """
    List available catalogs and schemas in Unity Catalog.
    
    Returns:
        Dictionary with available catalogs and schemas
    """
    try:
        catalogs = []
        for catalog in w.catalogs.list():
            schemas = []
            try:
                for schema in w.schemas.list(catalog_name=catalog.name):
                    schemas.append(schema.name)
            except Exception:
                pass  # Skip if cannot list schemas
            
            catalogs.append({
                "name": catalog.name,
                "schemas": schemas
            })
        
        return {
            "status": "success",
            "catalogs": catalogs,
            "catalog_count": len(catalogs)
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@tool
def execute_sql_query(query: str, max_wait_seconds: int = 300) -> Dict[str, Any]:
    """
    Execute SQL query on Databricks SQL Warehouse.
    
    Args:
        query: SQL query to execute
        max_wait_seconds: Maximum time to wait for execution (default: 300)
        
    Returns:
        Dictionary with execution results
    """
    try:
        statement = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout=f"{max_wait_seconds}s"
        )
        
        if statement.status.state == StatementState.SUCCEEDED:
            rows = []
            columns = []
            
            if statement.result and statement.result.data_array:
                rows = statement.result.data_array
            
            if statement.manifest and statement.manifest.schema:
                columns = [col.name for col in statement.manifest.schema.columns]
            
            return {
                "status": "success",
                "state": statement.status.state.value,
                "rows": rows,
                "columns": columns,
                "row_count": len(rows),
                "statement_id": statement.statement_id
            }
        else:
            error_msg = statement.status.error.message if statement.status.error else "Unknown error"
            return {
                "status": "error",
                "state": statement.status.state.value,
                "error": error_msg,
                "statement_id": statement.statement_id
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
