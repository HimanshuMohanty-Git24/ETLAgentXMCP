"""
Code Generation Agent using Databricks-hosted Claude Sonnet 4.5.

Generates production-ready PySpark/SQL code for Medallion transformations
using context from completed layers.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage
from state import ETLState
import json


class CodeGenAgent:
    """
    Context-enriched code generation for each layer.
    
    Uses Databricks-hosted Claude Sonnet 4.5 to generate SQL transformations
    based on actual schema and data from previous layers.
    """
    
    def __init__(self):
        """Initialize CodeGenAgent with Databricks Foundation Model API."""
        endpoint = os.getenv("DATABRICKS_MODEL_ENDPOINT", "databricks-claude-sonnet-4-5")
        temperature = float(os.getenv("MODEL_TEMPERATURE", "0.2"))
        max_tokens = int(os.getenv("MODEL_MAX_TOKENS", "4096"))
        
        self.llm = ChatDatabricks(
            endpoint=endpoint,
            temperature=temperature,  # Lower temperature for more deterministic code
            max_tokens=max_tokens,
        )
        
        print(f"✓ CodeGenAgent initialized with {endpoint}")
    
    async def __call__(self, state: ETLState) -> ETLState:
        """
        Generate PySpark/SQL code using enriched context.
        
        Creates executable SQL code for Databricks SQL Warehouse based on
        the transformation plan and actual data from previous layers.
        
        Args:
            state: Current ETL pipeline state
        
        Returns:
            Updated state with sql_queries, pyspark_code, and test_code
        """
        current_layer = state["current_layer"]
        
        # Determine source table from context
        source_table = self._get_source_table(state, current_layer)
        
        # Get context details from completed layers
        context_details = self._get_context_details(state, current_layer)
        
        # Build target table name
        catalog, schema, table = state["source_table"].split(".")
        target_table = f"{catalog}.{schema}_{current_layer}.{table}_{current_layer}"
        
        system_prompt = f"""You are an expert PySpark and SQL developer for Databricks.

Generate production-ready SQL code for {current_layer.upper()} layer transformation.

**CRITICAL REQUIREMENTS**:
1. Use the ACTUAL schema and column names from the context provided
2. Generate executable SQL for Databricks SQL Warehouse
3. Use CREATE OR REPLACE TABLE statements with Delta Lake
4. Include table properties: enableChangeDataFeed, autoOptimize
5. Add OPTIMIZE and ZORDER statements for performance
6. Include comprehensive pytest test suite
7. Reference actual column names - do not invent columns

Output JSON with this EXACT structure:
{{
    "sql_queries": [
        "CREATE OR REPLACE TABLE ... -- Main transformation",
        "OPTIMIZE ... ZORDER BY ... -- Performance optimization",
        "-- Additional setup queries if needed"
    ],
    "pyspark_code": "# Optional PySpark code if complex logic needed\\n# Leave empty if SQL is sufficient",
    "test_code": "# Complete pytest test suite\\nimport pytest\\n...",
    "validation_queries": [
        "SELECT COUNT(*) ... -- Row count validation",
        "SELECT ... -- Data quality checks"
    ]
}}

Generate clean, commented, production-ready code."""
        
        user_prompt = f"""Generate {current_layer.upper()} layer SQL transformation:

SOURCE TABLE: {source_table}
TARGET TABLE: {target_table}

{context_details}

TRANSFORMATION PLAN:
{state['transformation_plan']}

TEST STRATEGY:
{state['test_plan']}

REQUIREMENTS:
- Use actual column names from context above
- Create Delta table with Change Data Feed enabled
- Add data quality validations
- Include error handling
- Optimize for performance

Generate complete, executable SQL code now."""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        try:
            # Call Databricks-hosted Claude Sonnet 4.5
            response = await self.llm.ainvoke(messages)
            
            # Parse JSON response
            code_data = json.loads(response.content)
            
            state["sql_queries"] = code_data.get("sql_queries", [])
            state["pyspark_code"] = code_data.get("pyspark_code", "")
            state["test_code"] = code_data.get("test_code", "")
            
            # Store target table
            state["current_table_output"] = target_table
            
            print(f"✓ {current_layer.upper()} layer code generated ({len(state['sql_queries'])} queries)")
            
        except json.JSONDecodeError:
            # Fallback: extract SQL from markdown
            print(f"⚠ JSON parse failed, extracting SQL from markdown for {current_layer}")
            content = response.content
            sql_queries = []
            
            # Look for fenced SQL code blocks and extract their contents
            if "```" in content:
                for block in content.split("```sql")[1:]:
                    # take the portion up to the next closing fence
                    query = block.split("```")[0]
                    if query and query.strip():
                        sql_queries.append(query.strip())
            
            state["sql_queries"] = sql_queries
            state["pyspark_code"] = content
            state["test_code"] = "# Tests to be generated manually"
            state["current_table_output"] = target_table
        
        except Exception as e:
            print(f"✗ Code generation failed for {current_layer}: {str(e)}")
            state["error_log"].append(f"CodeGen error: {str(e)}")
            state["sql_queries"] = []
            state["pyspark_code"] = ""
        
        state["current_agent"] = "codegen"
        state["workflow_status"] = f"{current_layer}_coded"
        
        return state
    
    def _get_source_table(self, state: ETLState, layer: str) -> str:
        """Determine source table based on current layer and context."""
        if layer == "bronze":
            return state["source_table"]
        elif layer == "silver":
            bronze = state.get("bronze_context")
            return bronze["table_name"] if bronze else state["source_table"]
        else:  # gold
            silver = state.get("silver_context")
            return silver["table_name"] if silver else state["source_table"]
    
    def _get_context_details(self, state: ETLState, layer: str) -> str:
        """Get relevant context for code generation."""
        if layer == "bronze":
            return f"""
SOURCE DATA:
You are ingesting from: {state['source_table']}
This is raw data - create Bronze layer with audit columns.
"""
        
        elif layer == "silver":
            bronze = state.get("bronze_context")
            if bronze:
                return f"""
BRONZE LAYER OUTPUT (use this as source for Silver):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Schema with ACTUAL column names:
{json.dumps(bronze['schema']['columns'][:20], indent=2)}

Sample data showing actual values and patterns:
{json.dumps(bronze['sample_data'][:2], indent=2)}

Data Quality:
- Total rows: {bronze['row_count']:,}
- Completeness: {bronze['data_quality_metrics']['completeness_ratio']*100:.1f}%
- Records with nulls: {bronze['data_quality_metrics']['records_with_nulls']:,}

**Use these ACTUAL column names in your SQL - do not invent new columns!**
"""
        
        else:  # gold
            silver = state.get("silver_context")
            if silver:
                return f"""
SILVER LAYER OUTPUT (use this as source for Gold):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Schema with ACTUAL column names (cleaned and validated):
{json.dumps(silver['schema']['columns'][:20], indent=2)}

Sample data for aggregation planning:
{json.dumps(silver['sample_data'][:2], indent=2)}

Data Quality:
- Total rows: {silver['row_count']:,}
- Completeness: {silver['data_quality_metrics']['completeness_ratio']*100:.1f}%

**This is clean, validated data ready for aggregation and business logic!**
"""
        
        return "Context not available - use source table directly"