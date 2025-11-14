"""
Context Enrichment Agent.

Analyzes completed layer and enriches state for next transformation.
Does NOT use LLM - direct Databricks analysis.

Author: Data Engineering Team
Date: 2025-11-14
"""

from state import ETLState, LayerContext
from tools.context_tools import analyze_layer_output
from datetime import datetime


class ContextEnrichmentAgent:
    """
    Analyzes completed layer and enriches state for next transformation.
    
    After each layer execution, this agent:
    1. Deeply analyzes the output table
    2. Extracts schema, statistics, quality metrics
    3. Stores enriched context in state
    4. Prepares insights for next layer planning
    
    This ensures each subsequent layer has FRESH, ACTUAL context
    from the completed previous layer.
    """
    
    def __init__(self):
        """Initialize Context Enrichment Agent."""
        print("✓ ContextEnrichmentAgent initialized")
    
    async def __call__(self, state: ETLState) -> ETLState:
        """
        Enrich state with completed layer context.
        
        Performs deep analysis of completed layer to extract:
        - Complete schema with column types
        - Sample data for context understanding
        - Data quality metrics and statistics
        - Row counts and completeness ratios
        
        This context feeds into the next layer's planning and code generation.
        
        Args:
            state: Current ETL pipeline state
        
        Returns:
            Updated state with enriched layer context
        """
        current_layer = state["current_layer"]
        output_table = state["current_table_output"]
        
        print(f"📊 Analyzing {current_layer.upper()} layer output: {output_table}")
        
        # Deep analysis of completed layer
        analysis_result = analyze_layer_output.invoke({
            "table_name": output_table,
            "layer_name": current_layer,
            "sample_size": 10  # Get 10 sample records
        })
        
        if analysis_result["status"] == "success":
            # Create rich context object
            layer_context: LayerContext = {
                "layer_name": current_layer,
                "table_name": output_table,
                "schema": analysis_result["schema"],
                "sample_data": analysis_result["sample_data"],
                "row_count": analysis_result["row_count"],
                "data_quality_metrics": analysis_result["data_quality"],
                "transformation_summary": state.get("transformation_plan", ""),
                "execution_timestamp": datetime.now().isoformat(),
                "pr_url": state["current_pr_url"],
                "pr_merged": state["current_pr_merged"]
            }
            
            # Store in appropriate context slot
            if current_layer == "bronze":
                state["bronze_context"] = layer_context
                print(f"  ✓ Bronze context captured: {layer_context['row_count']:,} rows, {layer_context['schema']['column_count']} columns")
            elif current_layer == "silver":
                state["silver_context"] = layer_context
                print(f"  ✓ Silver context captured: {layer_context['row_count']:,} rows, {layer_context['data_quality_metrics']['completeness_ratio']*100:.1f}% complete")
            elif current_layer == "gold":
                state["gold_context"] = layer_context
                print(f"  ✓ Gold context captured: {layer_context['row_count']:,} rows")
            
            # Mark layer as completed
            if current_layer not in state["layers_completed"]:
                state["layers_completed"].append(current_layer)
            
            # Update remaining layers
            if current_layer in state["layers_remaining"]:
                state["layers_remaining"].remove(current_layer)
            
            state["current_agent"] = "context_enrichment"
            
            print(f"  ✓ Context enrichment complete for {current_layer.upper()}")
            print(f"  ℹ Remaining layers: {', '.join(state['layers_remaining']) if state['layers_remaining'] else 'None'}")
        
        else:
            error_msg = f"Context enrichment failed: {analysis_result.get('error', 'Unknown error')}"
            state["error_log"].append(error_msg)
            print(f"  ✗ {error_msg}")
        
        return state
