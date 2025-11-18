"""
Planner Agent using Groq-hosted LLMs.

This agent analyzes user requirements and creates detailed transformation plans
using Groq API for intelligent planning based on enriched context from previous layers.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from groq_llm import groq_chat_complete
from langchain_core.messages import HumanMessage, SystemMessage
from state import ETLState
from tools.context_tools import generate_transformation_recommendations
import json


class PlannerAgent:
    """
    Context-aware planner for Medallion transformations.
    
    Uses Groq-hosted LLMs for intelligent planning based on enriched context
    from previously completed layers.
    """
    
    def __init__(self):
        """
        Initialize PlannerAgent with Groq LLM.
        
        Configuration is read from environment variables:
        - GROQ_API_KEY: Groq API authentication
        - GROQ_MODEL_PLANNER: Model to use (default: llama-3.3-70b-versatile)
        """
        model = os.getenv("GROQ_MODEL_PLANNER", "llama-3.3-70b-versatile")
        print(f"[OK] PlannerAgent initialized with Groq model: {model}")
    
    async def __call__(self, state: ETLState) -> ETLState:
        """
        Generate layer-specific transformation plan.
        
        Uses enriched context from previously completed layers to create
        intelligent, data-aware transformation plans.
        
        Args:
            state: Current ETL pipeline state
        
        Returns:
            Updated state with transformation_plan and test_plan populated
        """
        current_layer = state["current_layer"]
        
        # Build context-aware prompt with actual data from previous layers
        context_info = self._build_context_section(state, current_layer)
        layer_guidance = self._get_layer_guidance(current_layer)
        
        system_prompt = f"""You are an expert data engineer specializing in Databricks Medallion architecture transformations.

{layer_guidance}

**CRITICAL INSTRUCTIONS**:
1. Use the enriched context from previously completed layers to inform your plan
2. The context includes ACTUAL schema, data quality metrics, and sample data
3. Reference specific column names and data patterns you see in the context
4. Consider data quality issues identified in previous layers
5. Plan transformations that build incrementally on previous work

Output your response as a JSON object with this EXACT structure:
{{
    "transformation_plan": "Detailed step-by-step transformation plan with specific column references",
    "test_plan": "Comprehensive testing strategy covering edge cases",
    "key_considerations": ["consideration1", "consideration2", "consideration3"],
    "expected_improvements": "What this layer achieves over the previous layer"
}}

Be specific and reference actual column names from the context provided."""
        
        user_prompt = f"""Plan the {current_layer.upper()} layer transformation:

USER REQUEST:
{state['user_query']}

{context_info}

BUSINESS RULES (from rules.txt):
{state['transformation_rules'][:2000]}  # Truncated for context window

Create a detailed, context-aware transformation plan that builds on the actual data structure you see above."""
        
        # Prepare messages for Claude
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        try:
            # Call Groq LLM
            content = await groq_chat_complete(
                messages=messages,
                model_env_key="GROQ_MODEL_PLANNER",
                default_model="llama-3.3-70b-versatile",
            )
            
            # Parse JSON response
            plan_data = json.loads(content)
            
            # Update state with plan
            state["transformation_plan"] = plan_data["transformation_plan"]
            state["test_plan"] = plan_data.get("test_plan", "Generate comprehensive unit tests")
            
            print(f"[OK] {current_layer.upper()} layer plan created")
            
        except json.JSONDecodeError:
            # Fallback if response is not valid JSON
            print(f"[WARNING] JSON parse failed, using raw response for {current_layer}")
            state["transformation_plan"] = content
            state["test_plan"] = "Generate comprehensive unit tests covering edge cases and data quality"
        
        except Exception as e:
            print(f"[ERROR] Planning failed for {current_layer}: {str(e)}")
            state["error_log"].append(f"Planning error: {str(e)}")
            state["transformation_plan"] = f"Error in planning: {str(e)}"
            state["test_plan"] = "Unable to generate test plan"
        
        # Update workflow tracking
        state["current_agent"] = "planner"
        state["workflow_status"] = f"{current_layer}_planned"
        
        return state
    
    def _build_context_section(self, state: ETLState, current_layer: str) -> str:
        """
        Build rich context section from completed layers.
        
        This provides the agent with actual schema and data from previous
        layers, enabling intelligent, data-aware planning.
        
        Args:
            state: Current pipeline state
            current_layer: Layer being planned (bronze/silver/gold)
        
        Returns:
            Formatted context string with schema and sample data
        """
        if current_layer == "bronze":
            # First layer - use source table info
            return f"""
SOURCE TABLE: {state['source_table']}

This is the raw data ingestion layer (Bronze).
You will create a table that ingests this source data with audit metadata.
"""
        
        elif current_layer == "silver":
            # Use bronze context if available
            bronze = state.get("bronze_context")
            if bronze:
                return f"""
COMPLETED BRONZE LAYER ANALYSIS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Table: {bronze['table_name']}
Total Rows: {bronze['row_count']:,}
Columns: {bronze['schema']['column_count']}
Data Quality: {bronze['data_quality_metrics']['completeness_ratio']*100:.1f}% complete
Null Records: {bronze['data_quality_metrics']['records_with_nulls']:,}

BRONZE SCHEMA (actual columns from completed layer):
{json.dumps(bronze['schema']['columns'][:15], indent=2)}

BRONZE SAMPLE DATA (first 3 actual records):
{json.dumps(bronze['sample_data'][:3], indent=2)}

**IMPORTANT**: Use these ACTUAL column names and data types in your Silver transformation plan!
The data above is REAL - reference specific columns and patterns you observe.
"""
            return "Bronze context unavailable - proceeding with limited information"
        
        else:  # gold
            # Use silver context if available
            silver = state.get("silver_context")
            if silver:
                return f"""
COMPLETED SILVER LAYER ANALYSIS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Table: {silver['table_name']}
Total Rows: {silver['row_count']:,}
Columns: {silver['schema']['column_count']}
Data Quality: {silver['data_quality_metrics']['completeness_ratio']*100:.1f}% complete

SILVER SCHEMA (cleaned and validated):
{json.dumps(silver['schema']['columns'][:15], indent=2)}

SILVER SAMPLE DATA (first 3 records):
{json.dumps(silver['sample_data'][:3], indent=2)}

**IMPORTANT**: This is CLEANED, VALIDATED data ready for aggregation.
Use these columns to create business metrics and aggregations in Gold layer.
Reference specific metrics and columns you see above.
"""
            return "Silver context unavailable - proceeding with limited information"
    
    def _get_layer_guidance(self, layer: str) -> str:
        """
        Get layer-specific transformation guidance.
        
        Args:
            layer: bronze, silver, or gold
        
        Returns:
            Detailed guidance for the specific layer
        """
        guidance = {
            "bronze": """
BRONZE LAYER: Raw data ingestion with audit trail

Your role: Plan how to ingest raw data with minimal transformation.

Key Requirements:
- Ingest ALL data as-is (no filtering, no cleaning)
- Add audit columns: ingestion_timestamp, source_file, row_id
- Enable Delta Lake features: Change Data Feed, Auto Optimize
- Create foundation for data lineage tracking
- Store in catalog.schema_bronze.table_bronze naming convention
""",
            "silver": """
SILVER LAYER: Cleaned and validated data

Your role: Transform Bronze data into clean, reliable, analysis-ready format.

Use the ACTUAL Bronze schema and sample data provided above to plan:
- Deduplication strategy based on observed duplicates
- Null handling for columns with missing values
- Data type corrections for mismatched types
- Validation rules for out-of-range values
- Quality scoring based on observed data patterns
- Standardization of formats (dates, strings, codes)

Reference specific Bronze columns in your plan.
""",
            "gold": """
GOLD LAYER: Business-ready analytics and aggregations

Your role: Create aggregated, KPI-driven datasets for business users.

Use the ACTUAL Silver schema and sample data provided above to plan:
- Aggregations (daily, weekly, monthly by relevant dimensions)
- Derived business metrics and KPIs
- Dimension and fact table structure
- Surrogate key strategy for joins
- Partitioning and optimization strategy

Reference specific Silver columns and propose specific aggregations.
"""
        }
        
        return guidance.get(layer, "")
