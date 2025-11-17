"""
MCP Server for Multi-Layer Medallion ETL System.

Uses Databricks-hosted Claude Sonnet 4.5 for all agents.
Provides tools for VS Code Copilot integration.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables FIRST
load_dotenv()

# Validate critical environment variables
required_vars = [
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_WAREHOUSE_ID",
    "DATABRICKS_MODEL_ENDPOINT",
    "GITHUB_TOKEN",
    "GITHUB_REPO_OWNER",
    "GITHUB_REPO_NAME",
    "GIT_LOCAL_PATH"
]

missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("Missing required environment variables:")
    for var in missing_vars:
        print(f"   - {var}")
    print("\nPlease check your .env file and ensure all variables are set.")
    sys.exit(1)

from fastmcp import FastMCP, Context
from graph_workflow import MEDALLION_PIPELINE
from state import ETLState
from datetime import datetime
from typing import Optional

# Initialize MCP server
mcp = FastMCP(
    name="Medallion ETL with Databricks Claude",
    version="3.0.0",
    description="Multi-layer Medallion ETL system using Databricks-hosted Claude Sonnet 4.5"
)


@mcp.tool()
async def run_full_medallion_pipeline(
    user_query: str,
    source_table: str,
    ctx: Optional[Context] = None
) -> str:
    """
    Execute complete Bronze → Silver → Gold pipeline with context enrichment.
    
    This intelligent agentic system:
    1. Plans and transforms Bronze layer → creates PR #1 → waits for merge → executes
    2. Analyzes Bronze output and enriches context with actual schema/data
    3. Uses Bronze context to intelligently plan Silver transformation
    4. Transforms Silver layer → creates PR #2 → waits for merge → executes
    5. Analyzes Silver output and enriches context
    6. Uses Silver context to plan Gold transformation
    7. Transforms Gold layer → creates PR #3 → waits for merge → executes
    8. Returns comprehensive summary with all 3 PRs and execution metrics
    
    Each layer transformation is informed by ACTUAL, FRESH data from the
    previous completed layer, creating a truly adaptive pipeline.
    
    All agents use Databricks-hosted Claude Sonnet 4.5 via Foundation Model API.
    
    Args:
        user_query: Business transformation request in natural language
                   Example: "Clean and deduplicate weather data, create daily aggregations"
        source_table: Source table in Unity Catalog format (catalog.schema.table)
                     Example: "samples.accuweather.forecast_daily_calendar_imperial"
        ctx: MCP context for logging (optional)
    
    Returns:
        Comprehensive summary with PR links, execution metrics, and next steps
    
    Example:
        >>> result = await run_full_medallion_pipeline(
        ...     "Remove duplicates and nulls, then aggregate by city",
        ...     "samples.accuweather.weather_data"
        ... )
    """
    
    # Initialize pipeline state
    initial_state: ETLState = {
        "user_query": user_query,
        "source_table": source_table,
        "full_pipeline": True,
        "transformation_rules": "",
        "current_layer": "bronze",
        "target_layers": ["bronze", "silver", "gold"],
        "bronze_context": None,
        "silver_context": None,
        "gold_context": None,
        "transformation_plan": "",
        "test_plan": "",
        "pyspark_code": "",
        "test_code": "",
        "sql_queries": [],
        "review_status": "",
        "review_comments": [],
        "code_quality_score": 0.0,
        "current_pr_url": "",
        "current_pr_number": 0,
        "current_pr_merged": False,
        "pr_history": [],
        "execution_status": "",
        "current_table_output": "",
        "rows_processed": 0,
        "data_quality_metrics": {},
        "layers_completed": [],
        "layers_remaining": ["bronze", "silver", "gold"],
        "executive_summary": "",
        "messages": [],
        "current_agent": "",
        "workflow_status": "initializing",
        "error_log": [],
        "pipeline_start_time": datetime.now().isoformat(),
        "pipeline_end_time": ""
    }
    
    # Load transformation rules from file
    try:
        with open("rules.txt", "r") as f:
            initial_state["transformation_rules"] = f.read()
    except FileNotFoundError:
        print("⚠ rules.txt not found, using default rules")
        initial_state["transformation_rules"] = "# Default transformation rules"
    
    if ctx:
        await ctx.info(f"Starting full Medallion pipeline (Bronze -> Silver -> Gold)")
        await ctx.info(f"Source: {source_table}")
        await ctx.info(f"LLM: Databricks {os.getenv('DATABRICKS_MODEL_ENDPOINT')}")
    
    print("\n" + "="*70)
    print("MEDALLION ETL PIPELINE STARTING")
    print("="*70)
    print(f"Source: {source_table}")
    print(f"Request: {user_query}")
    print(f"LLM: {os.getenv('DATABRICKS_MODEL_ENDPOINT')}")
    print("="*70 + "\n")
    
    # Execute multi-layer workflow
    try:
        final_state = await MEDALLION_PIPELINE.ainvoke(initial_state)
        final_state["pipeline_end_time"] = datetime.now().isoformat()
        
        if ctx:
            await ctx.info(f"Pipeline completed: {len(final_state['layers_completed'])} layers processed")
    
    except Exception as e:
        error_msg = f"Pipeline execution failed: {str(e)}"
        print(f"\n{error_msg}\n")
        if ctx:
            await ctx.error(error_msg)
        
        return f"""# Pipeline Execution Failed

Error: {str(e)}

Please check:
- Databricks connectivity (DATABRICKS_HOST, DATABRICKS_TOKEN)
- SQL Warehouse availability (DATABRICKS_WAREHOUSE_ID)
- Foundation Model API access (DATABRICKS_MODEL_ENDPOINT)
- GitHub credentials (GITHUB_TOKEN)

See logs for detailed error information.
"""
    
    # Format comprehensive summary
    summary = _format_pipeline_summary(final_state)
    
    print("\n" + "="*70)
    print("PIPELINE EXECUTION COMPLETE")
    print("="*70 + "\n")
    
    return summary


@mcp.tool()
async def check_transformation_status(pr_number: int) -> str:
    """
    Check the status of a transformation by GitHub PR number.
    
    Provides current status of a layer transformation including:
    - PR state (open, closed, merged)
    - Merge status
    - Link to PR
    
    Args:
        pr_number: GitHub PR number from pipeline execution
    
    Returns:
        Formatted status summary
    """
    from tools.github_tools import check_pr_status
    
    status = check_pr_status.invoke({"pr_number": pr_number})
    
    return f"""# Transformation Status: PR #{pr_number}

**State**: {status.get('state', 'unknown').upper()}
**Merged**: {'Yes' if status.get('merged') else 'Not yet'}
**Mergeable**: {status.get('mergeable', 'unknown')}
**URL**: {status.get('url', 'N/A')}

{'Code has been merged and executed!' if status.get('merged') else 'Awaiting review and merge. Code will execute automatically after merge.'}
"""


@mcp.tool()
async def view_transformation_rules() -> str:
    """
    View current business transformation rules from rules.txt.
    
    Returns the complete set of transformation rules that guide
    the agentic system's planning and code generation.
    
    Returns:
        Contents of rules.txt with formatting
    """
    try:
        with open("rules.txt", "r") as f:
            rules = f.read()
        
        return f"""# Current Transformation Rules

{rules}

---
**Edit rules.txt to modify transformation logic system-wide**
**Changes take effect on next pipeline execution**
"""
    except FileNotFoundError:
        return "rules.txt not found. Please create this file in the project root."


def _format_pipeline_summary(state: ETLState) -> str:
    """Format comprehensive pipeline summary for user."""
    
    # Calculate duration
    from datetime import datetime
    start = datetime.fromisoformat(state["pipeline_start_time"])
    end = datetime.fromisoformat(state["pipeline_end_time"])
    duration = end - start
    
    # Format layer summaries
    layer_summaries = []
    for layer in ["bronze", "silver", "gold"]:
        context_key = f"{layer}_context"
        context = state.get(context_key)
        if context:
            layer_summaries.append(_format_layer_summary(layer, context))
    
    # Format PR history
    pr_list = _format_pr_history(state["pr_history"])
    
    return f"""# Multi-Layer Medallion Pipeline Complete

## Executive Summary

{state['executive_summary']}

## Pipeline Overview

- **Duration**: {duration.total_seconds():.1f} seconds
- **Layers Processed**: {', '.join([l.upper() for l in state['layers_completed']])}
- **Total PRs Created**: {len(state['pr_history'])}
- **Overall Status**: {state['workflow_status']}
- **LLM Used**: Databricks {os.getenv('DATABRICKS_MODEL_ENDPOINT')}

## Layer Details

{''.join(layer_summaries)}

## Pull Requests

{pr_list}

## Errors (if any)

{_format_errors(state['error_log'])}

## Next Steps

{'All transformations executed successfully! Data is available in all layers.' if not state['layers_remaining'] and not state['error_log'] else ''}
{'Action Required: Review and merge PRs in order (Bronze -> Silver -> Gold)' if state['pr_history'] and any(not pr.get('merged', False) for pr in state['pr_history']) else ''}
{'Attention Needed: Review errors above and re-run failed layers' if state['error_log'] else ''}

---
*Generated by Medallion ETL Agentic System v3.0*
*Powered by Databricks Claude Sonnet 4.5*
"""


def _format_layer_summary(layer: str, context: dict) -> str:
    """Format individual layer summary."""
    
    return f"""
### {layer.upper()} Layer

- **Table**: `{context['table_name']}`
- **Rows**: {context['row_count']:,}
- **Columns**: {context['schema']['column_count']}
- **Quality**: {context['data_quality_metrics'].get('completeness_ratio', 0)*100:.1f}% complete
- **PR**: {context['pr_url']}
- **Status**: {'Merged & Executed' if context['pr_merged'] else 'Awaiting Merge'}
"""


def _format_pr_history(pr_history: list) -> str:
    """Format PR history list."""
    if not pr_history:
        return "No PRs created (pipeline did not complete)"
    
    lines = []
    for i, pr in enumerate(pr_history, 1):
        status = "Merged" if pr.get('merged', False) else "Pending"
        quality = pr.get('quality_score', 0)
        lines.append(f"{i}. **{pr['layer'].upper()}** Layer - PR #{pr['pr_number']} ({status}, Quality: {quality}/100)")
        lines.append(f"   - URL: {pr['pr_url']}")
        lines.append(f"   - Branch: `{pr.get('branch_name', 'N/A')}`")
        lines.append("")
    
    return "\n".join(lines)


def _format_errors(errors: list) -> str:
    """Format error list."""
    if not errors:
        return "No errors encountered"
    
    return "\n".join([f"- {err}" for err in errors])


if __name__ == "__main__":
    print("\n" + "="*70)
    print("MEDALLION ETL MCP SERVER v3.0")
    print("="*70)
    print(f"Using Databricks-hosted {os.getenv('DATABRICKS_MODEL_ENDPOINT')}")
    print(f"Workspace: {os.getenv('DATABRICKS_HOST')}")
    print(f"SQL Warehouse: {os.getenv('DATABRICKS_WAREHOUSE_ID')}")
    print(f"GitHub Repo: {os.getenv('GITHUB_REPO_OWNER')}/{os.getenv('GITHUB_REPO_NAME')}")
    print("="*70)
    print("Loaded transformation rules from rules.txt")
    print("Ready for VS Code Copilot integration")
    print("="*70 + "\n")
    
    mcp.run()
