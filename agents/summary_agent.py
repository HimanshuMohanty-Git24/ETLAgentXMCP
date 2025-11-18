"""
Summary Agent using Groq-hosted LLMs.

Creates executive summaries of pipeline execution for business users.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from groq_llm import groq_chat_complete
from langchain_core.messages import HumanMessage, SystemMessage
from state import ETLState


class SummaryAgent:
    """
    Generates executive summaries for business stakeholders.
    
    Uses Groq-hosted LLMs to create clear,
    non-technical summaries of pipeline execution results.
    """
    
    def __init__(self):
        """Initialize SummaryAgent with Groq LLM."""
        model = os.getenv("GROQ_MODEL_SUMMARY", "llama-3.1-8b-instant")
        print(f"[OK] SummaryAgent initialized with Groq model: {model}")
    
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
            # Call Groq LLM
            content = await groq_chat_complete(
                messages=messages,
                model_env_key="GROQ_MODEL_SUMMARY",
                default_model="llama-3.1-8b-instant",
            )
            
            state["executive_summary"] = content
            
            print(f"[OK] Executive summary generated")
            
        except Exception as e:
            print(f"[ERROR] Summary generation failed: {str(e)}")
            state["error_log"].append(f"Summary error: {str(e)}")
            state["executive_summary"] = f"""
Pipeline execution completed for {state['user_query']}.
Processed {len(state['layers_completed'])} layers: {', '.join(state['layers_completed'])}.
Review PRs and check execution logs for details.
"""
        
        state["current_agent"] = "summary"
        state["workflow_status"] = "completed"
        
        return state
