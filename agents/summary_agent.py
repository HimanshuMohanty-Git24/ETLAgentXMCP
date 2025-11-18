"""
Summary Agent using Databricks-hosted Claude Sonnet 4.5.

Creates executive summaries of pipeline execution for business users.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage
from state import ETLState


class SummaryAgent:
    """
    Generates executive summaries for business stakeholders.
    
    Uses Databricks-hosted Claude Sonnet 4.5 to create clear,
    non-technical summaries of pipeline execution results.
    """
    
    def __init__(self):
        """Initialize SummaryAgent with Databricks Foundation Model API."""
        endpoint = os.getenv("DATABRICKS_MODEL_ENDPOINT", "databricks-claude-sonnet-4-5")
        temperature = float(os.getenv("MODEL_TEMPERATURE", "0.4"))  # Slightly higher for natural language
        max_tokens = int(os.getenv("MODEL_MAX_TOKENS", "2048"))
        
        self.llm = ChatDatabricks(
            endpoint=endpoint,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        
        print(f"SummaryAgent initialized with {endpoint}")
    
    async def __call__(self, state: ETLState) -> ETLState:
        """
        Create executive summary of pipeline execution.
        
        Generates a clear, business-friendly summary including:
        - What was accomplished
        - Data quality improvements
        - PR links for review
        - Next steps
        
        Args:
            state: Final ETL pipeline state
        
        Returns:
            Updated state with executive_summary populated
        """
        system_prompt = """You are a technical writer creating executive summaries for business stakeholders.

Create a clear, professional summary of the ETL pipeline execution that:
- Explains what was accomplished in plain English
- Highlights key metrics and improvements
- Notes any issues or areas needing attention
- Provides clear next steps

Write in a professional but accessible tone. Avoid jargon where possible,
and explain technical terms when necessary.

Keep the summary concise (200-300 words) but informative."""
        
        # Build comprehensive context
        layers_info = []
        for layer in state["layers_completed"]:
            context_key = f"{layer}_context"
            context = state.get(context_key)
            if context:
                layers_info.append(f"""
{layer.upper()} Layer:
- Table: {context['table_name']}
- Rows: {context['row_count']:,}
- Quality: {context['data_quality_metrics'].get('completeness_ratio', 0)*100:.1f}%
- PR: {context['pr_url']}
""")
        
        user_prompt = f"""Generate executive summary for this ETL pipeline execution:

USER REQUEST: {state['user_query']}
SOURCE TABLE: {state['source_table']}

LAYERS PROCESSED: {', '.join(state['layers_completed'])}

LAYER DETAILS:
{''.join(layers_info)}

OVERALL STATUS: {state['workflow_status']}

QUALITY METRICS:
- Average Code Quality: {state['code_quality_score']}/100
- Total PRs Created: {len(state['pr_history'])}
- Execution Status: {state.get('execution_status', 'Pending')}

ERRORS (if any): {', '.join(state['error_log']) if state['error_log'] else 'None'}

Create a professional executive summary suitable for business stakeholders."""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        try:
            # Call Databricks-hosted Claude Sonnet 4.5
            response = await self.llm.ainvoke(messages)
            
            state["executive_summary"] = response.content
            
            print(f"Executive summary generated")
            
        except Exception as e:
            print(f"Summary generation failed: {str(e)}")
            state["error_log"].append(f"Summary error: {str(e)}")
            state["executive_summary"] = f"""
Pipeline execution completed for {state['user_query']}.
Processed {len(state['layers_completed'])} layers: {', '.join(state['layers_completed'])}.
Review PRs and check execution logs for details.
"""
        
        state["current_agent"] = "summary"
        state["workflow_status"] = "completed"
        
        return state
