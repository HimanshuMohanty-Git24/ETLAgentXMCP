"""
State management for multi-layer Medallion ETL pipeline.

Defines the ETLState and LayerContext types that flow through
LangGraph and all agents.

Author: Data Engineering Team
Date: 2025-11-14
"""

from typing import TypedDict, Annotated, List, Dict, Any, Literal, Optional
from langchain_core.messages import BaseMessage
import operator


class LayerContext(TypedDict):
    """
    Rich context from a completed transformation layer.

    Attributes:
        layer_name: "bronze" | "silver" | "gold"
        table_name: Fully qualified table name (catalog.schema.table)
        schema: Schema info (parsed columns, counts, etc.)
        sample_data: Sample records from the layer output
        row_count: Total rows in the layer output table
        data_quality_metrics: Data quality summary (nulls, completeness, etc.)
        transformation_summary: Human-readable summary of what the layer did
        execution_timestamp: ISO timestamp of when this layer completed
        pr_url: GitHub PR URL for this layer's code
        pr_merged: Whether the PR was merged before execution
    """
    layer_name: Literal["bronze", "silver", "gold"]
    table_name: str
    schema: Dict[str, Any]
    sample_data: List[Dict[str, Any]]
    row_count: int
    data_quality_metrics: Dict[str, Any]
    transformation_summary: str
    execution_timestamp: str
    pr_url: str
    pr_merged: bool


class ETLState(TypedDict):
    """
    Complete state for the multi-layer Medallion ETL workflow.

    This is the single source of truth that:
    - LangGraph nodes read/modify
    - Agents use to pass plans, code, review results, PR data, and metrics
    """

    # ---------------------------------------------------------------------
    # USER INPUT & CONFIG
    # ---------------------------------------------------------------------
    user_query: str              # Natural language request from user
    source_table: str            # catalog.schema.table of the initial data
    full_pipeline: bool          # True = Bronze->Silver->Gold, False = single layer
    transformation_rules: str    # Contents of rules.txt

    # ---------------------------------------------------------------------
    # CURRENT LAYER TRACKING
    # ---------------------------------------------------------------------
    current_layer: Literal["bronze", "silver", "gold"]
    target_layers: List[str]     # Ordered list: ["bronze", "silver", "gold"]

    # ---------------------------------------------------------------------
    # LAYER CONTEXT HISTORY (SET BY ContextEnrichmentAgent)
    # ---------------------------------------------------------------------
    bronze_context: Optional[LayerContext]
    silver_context: Optional[LayerContext]
    gold_context: Optional[LayerContext]

    # ---------------------------------------------------------------------
    # CURRENT TRANSFORMATION (RESET EACH LAYER)
    # ---------------------------------------------------------------------
    transformation_plan: str     # PlannerAgent output
    test_plan: str               # PlannerAgent test strategy
    pyspark_code: str            # CodeGenAgent optional PySpark
    test_code: str               # CodeGenAgent pytest tests
    sql_queries: List[str]       # CodeGenAgent main SQL statements

    # ---------------------------------------------------------------------
    # CODE REVIEW (PER LAYER)
    # ---------------------------------------------------------------------
    review_status: str           # "APPROVED" | "NEEDS_REVISION" | "REJECTED"
    review_comments: List[str]   # ReviewerAgent comments
    code_quality_score: float    # 0–100 score

    # ---------------------------------------------------------------------
    # PR MANAGEMENT (PER LAYER)
    # ---------------------------------------------------------------------
    current_pr_url: str
    current_pr_number: int
    current_pr_merged: bool
    pr_history: List[Dict[str, Any]]  # list of {layer, pr_number, pr_url, branch_name, quality_score, ...}

    # ---------------------------------------------------------------------
    # EXECUTION RESULTS (PER LAYER)
    # ---------------------------------------------------------------------
    execution_status: str        # "success" | "failed" | "awaiting_pr_merge" | etc.
    current_table_output: str    # output table name for this layer
    rows_processed: int          # rows written in this layer
    data_quality_metrics: Dict[str, Any]

    # ---------------------------------------------------------------------
    # OVERALL WORKFLOW STATUS
    # ---------------------------------------------------------------------
    layers_completed: List[str]  # e.g. ["bronze", "silver"]
    layers_remaining: List[str]  # e.g. ["gold"]
    executive_summary: str       # Final summary for user

    # LangGraph message history (if you use it)
    messages: Annotated[List[BaseMessage], operator.add]

    current_agent: str           # Which agent ran last
    workflow_status: str         # free-form status string
    error_log: List[str]         # accumulated errors

    pipeline_start_time: str     # ISO timestamp
    pipeline_end_time: str       # ISO timestamp
