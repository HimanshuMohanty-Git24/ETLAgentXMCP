"""
LangGraph workflow orchestration for multi-layer Medallion pipeline.

Implements conditional routing, context enrichment, and sequential layer processing.
Each layer creates its own PR and enriches context for the next layer.

Author: Data Engineering Team
Date: 2025-11-14
"""

from langgraph.graph import StateGraph, START, END
from state import ETLState
from agents import (
    PlannerAgent,
    CodeGenAgent,
    ReviewerAgent,
    PRCreatorAgent,
    ExecutorAgent,
    ContextEnrichmentAgent,
    SummaryAgent
)


def create_medallion_pipeline_graph():
    """
    Create multi-stage Medallion pipeline with context enrichment.
    
    Workflow for full pipeline (Bronze → Silver → Gold):
    
    For Each Layer:
    ┌─────────────────────────────────────────────────────────────┐
    │ 1. Plan (using context from previous layer if available)   │
    │ 2. Generate Code (context-aware SQL/PySpark)               │
    │ 3. Review (quality check + syntax validation)              │
    │ 4. Create PR (if approved, else skip to summary)           │
    │ 5. Execute (wait for PR merge, then run on Databricks)     │
    │ 6. Enrich Context (analyze output for next layer)          │
    └─────────────────────────────────────────────────────────────┘
    
    After all layers: Generate Executive Summary
    
    Key Features:
    - Conditional routing based on review status
    - Context enrichment after each layer completion
    - Sequential processing with fresh context
    - Separate PR for each layer transformation
    
    Returns:
        Compiled LangGraph workflow
    """
    
    # Initialize graph with ETL state
    graph = StateGraph(ETLState)
    
    # =========================================================================
    # ADD AGENT NODES
    # =========================================================================
    
    graph.add_node("planner", PlannerAgent())
    graph.add_node("codegen", CodeGenAgent())
    graph.add_node("reviewer", ReviewerAgent())
    graph.add_node("pr_creator", PRCreatorAgent())
    graph.add_node("executor", ExecutorAgent())
    graph.add_node("context_enrichment", ContextEnrichmentAgent())
    graph.add_node("summary", SummaryAgent())
    
    # =========================================================================
    # DEFINE WORKFLOW EDGES
    # =========================================================================
    
    # Start with planning
    graph.add_edge(START, "planner")
    
    # Linear flow through code generation and review
    graph.add_edge("planner", "codegen")
    graph.add_edge("codegen", "reviewer")
    
    # =========================================================================
    # CONDITIONAL EDGE: Create PR only if code approved
    # =========================================================================
    
    def should_create_pr(state: ETLState) -> str:
        """
        Decide whether to create PR or skip to summary.
        
        If code is approved, create PR and proceed with execution.
        If code needs revision or is rejected, skip to summary.
        
        Args:
            state: Current pipeline state
        
        Returns:
            Next node name: "pr_creator" or "summary"
        """
        if state["review_status"] == "APPROVED":
            return "pr_creator"
        else:
            print(f"Code not approved (status: {state['review_status']}), skipping to summary")
            return "summary"
    
    graph.add_conditional_edges("reviewer", should_create_pr)
    
    # Continue to execution after PR creation
    graph.add_edge("pr_creator", "executor")
    
    # Enrich context after execution
    graph.add_edge("executor", "context_enrichment")
    
    # =========================================================================
    # CONDITIONAL EDGE: Process next layer or finish
    # =========================================================================
    
    def should_process_next_layer(state: ETLState) -> str:
        """
        Decide whether to process next layer or generate summary.
        
        If more layers remain, update current_layer and loop back to planning
        with enriched context from completed layer.
        
        If all layers complete, proceed to final summary.
        
        Args:
            state: Current pipeline state with updated layers_remaining
        
        Returns:
            Next node name: "planner" (for next layer) or "summary" (finish)
        """
        if state["layers_remaining"]:
            # More layers to process
            next_layer = state["layers_remaining"][0]
            state["current_layer"] = next_layer
            
            print(f"\n{'='*70}")
            print(f"Moving to next layer: {next_layer.upper()}")
            print(f"   Completed: {', '.join(state['layers_completed'])}")
            print(f"   Remaining: {', '.join(state['layers_remaining'])}")
            print(f"{'='*70}\n")
            
            return "planner"  # Loop back with fresh context
        else:
            # All layers complete
            print(f"\n{'='*70}")
            print(f"All layers completed: {', '.join(state['layers_completed'])}")
            print(f"Generating executive summary...")
            print(f"{'='*70}\n")
            
            return "summary"  # Finish pipeline
    
    graph.add_conditional_edges("context_enrichment", should_process_next_layer)
    
    # End after summary
    graph.add_edge("summary", END)
    
    # =========================================================================
    # COMPILE AND RETURN
    # =========================================================================
    
    compiled_graph = graph.compile()
    
    print("Medallion pipeline graph compiled successfully")
    print("  Flow: Planner  CodeGen  Reviewer  PR  Executor  Context  (loop or Summary)")
    
    return compiled_graph


# Create singleton instance
MEDALLION_PIPELINE = create_medallion_pipeline_graph()
