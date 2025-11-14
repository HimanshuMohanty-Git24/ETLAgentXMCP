"""
Executor Agent for running transformations on Databricks.

Executes approved code on Databricks SQL Warehouse after PR merge.
Does NOT use LLM - direct Databricks SDK integration.

Author: Data Engineering Team
Date: 2025-11-14
"""

from state import ETLState
from tools.medallion_tools import (
    create_bronze_table,
    execute_silver_transformation,
    execute_gold_transformation
)
from tools.github_tools import check_pr_status
import time


class ExecutorAgent:
    """
    Executes Medallion transformations on Databricks after PR approval.
    
    Waits for PR merge, then executes transformation code on Databricks
    SQL Warehouse and captures execution results.
    """
    
    def __init__(self):
        """Initialize Executor Agent."""
        print("✓ ExecutorAgent initialized")
    
    async def __call__(self, state: ETLState) -> ETLState:
        """
        Execute approved transformation code on Databricks.
        
        Flow:
        1. Check if PR is merged
        2. If not merged, return and wait
        3. If merged, execute layer-specific transformation
        4. Capture execution results and metrics
        5. Update state with results
        
        Args:
            state: Current ETL pipeline state
        
        Returns:
            Updated state with execution results
        """
        current_layer = state["current_layer"]
        
        # Check PR merge status
        print(f"ℹ Checking PR #{state['current_pr_number']} merge status...")
        
        pr_status = check_pr_status.invoke({"pr_number": state["current_pr_number"]})
        
        if not pr_status.get("merged", False):
            # PR not merged yet - cannot execute
            state["execution_status"] = "awaiting_pr_merge"
            state["workflow_status"] = "awaiting_merge"
            
            print(f"⏳ PR #{state['current_pr_number']} not merged yet. Execution paused.")
            print(f"   PR State: {pr_status.get('state', 'unknown')}")
            print(f"   URL: {pr_status.get('url', state['current_pr_url'])}")
            
            # In production, this would wait or trigger on webhook
            # For demo, we'll simulate waiting
            print("   💡 In production: Webhook triggers execution after merge")
            print("   💡 For testing: Manually merge PR and re-run")
            
            return state
        
        # PR is merged - proceed with execution
        state["pr_merged"] = True
        state["current_pr_merged"] = True
        
        print(f"✓ PR #{state['current_pr_number']} merged! Executing transformation...")
        
        # Determine target table name
        catalog, schema, table = state["source_table"].split(".")
        target_table = f"{catalog}.{schema}_{current_layer}.{table}_{current_layer}"
        state["current_table_output"] = target_table
        
        try:
            # Execute layer-specific transformation
            if current_layer == "bronze":
                result = self._execute_bronze(state, target_table)
            elif current_layer == "silver":
                result = self._execute_silver(state, target_table)
            else:  # gold
                result = self._execute_gold(state, target_table)
            
            # Update state with results
            state["execution_status"] = result["status"]
            state["rows_processed"] = result.get("rows_processed", 0)
            state["data_quality_metrics"] = result.get("data_quality_metrics", {})
            state["current_agent"] = "executor"
            state["workflow_status"] = f"{current_layer}_executed"
            
            if result["status"] == "success":
                print(f"✓ {current_layer.upper()} transformation executed successfully")
                print(f"  Target: {target_table}")
                print(f"  Rows: {result.get('rows_processed', 0):,}")
            else:
                print(f"✗ {current_layer.upper()} transformation failed")
                state["error_log"].append(f"Execution failed: {result.get('error', 'Unknown error')}")
        
        except Exception as e:
            error_msg = f"Exception during {current_layer} execution: {str(e)}"
            state["execution_status"] = "failed"
            state["error_log"].append(error_msg)
            print(f"✗ {error_msg}")
        
        return state
    
    def _execute_bronze(self, state: ETLState, target_table: str) -> dict:
        """Execute Bronze layer transformation."""
        print("  📦 Creating Bronze layer...")
        
        return create_bronze_table.invoke({
            "source_table": state["source_table"],
            "bronze_table": target_table,
            "add_metadata": True
        })
    
    def _execute_silver(self, state: ETLState, target_table: str) -> dict:
        """Execute Silver layer transformation."""
        print("  🥈 Creating Silver layer...")
        
        bronze = state.get("bronze_context")
        source_table = bronze["table_name"] if bronze else state["source_table"]
        
        # Use first SQL query as transformation
        transformation_sql = state["sql_queries"][0] if state["sql_queries"] else state["pyspark_code"]
        
        return execute_silver_transformation.invoke({
            "bronze_table": source_table,
            "silver_table": target_table,
            "transformation_sql": transformation_sql
        })
    
    def _execute_gold(self, state: ETLState, target_table: str) -> dict:
        """Execute Gold layer transformation."""
        print("  🥇 Creating Gold layer...")
        
        silver = state.get("silver_context")
        source_table = silver["table_name"] if silver else state["source_table"]
        
        # Use first SQL query as transformation
        transformation_sql = state["sql_queries"][0] if state["sql_queries"] else state["pyspark_code"]
        
        return execute_gold_transformation.invoke({
            "silver_table": source_table,
            "gold_table": target_table,
            "transformation_sql": transformation_sql,
            "partition_by": "date"
        })
