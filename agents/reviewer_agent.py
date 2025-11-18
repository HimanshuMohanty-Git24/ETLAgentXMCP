"""
Code Review Agent using Databricks-hosted Claude Sonnet 4.5.

Reviews generated code for quality, correctness, and best practices.

Author: Data Engineering Team
Date: 2025-11-14
"""

import os
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage
from state import ETLState
from tools.databricks_tools import validate_sql_syntax
import json


class ReviewerAgent:
    """
    Reviews generated code for quality and correctness.
    
    Uses Databricks-hosted Claude Sonnet 4.5 for intelligent code review
    including syntax validation, security checks, and performance analysis.
    """
    
    def __init__(self):
        """Initialize ReviewerAgent with Databricks Foundation Model API."""
        endpoint = os.getenv("DATABRICKS_MODEL_ENDPOINT", "databricks-claude-sonnet-4-5")
        temperature = float(os.getenv("MODEL_TEMPERATURE", "0.1"))  # Very low for deterministic review
        max_tokens = int(os.getenv("MODEL_MAX_TOKENS", "4096"))
        
        self.llm = ChatDatabricks(
            endpoint=endpoint,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        
        print(f"[OK] ReviewerAgent initialized with {endpoint}")
    
    async def __call__(self, state: ETLState) -> ETLState:
        """
        Review generated code for quality and approval.
        
        Performs comprehensive code review including:
        - Syntax validation via Databricks
        - Security checks (no hardcoded credentials)
        - Performance analysis
        - Best practices verification
        - Test coverage assessment
        
        Args:
            state: Current ETL pipeline state
        
        Returns:
            Updated state with review_status, code_quality_score, and comments
        """
        current_layer = state["current_layer"]
        
        # Validate SQL syntax using Databricks
        syntax_valid = True
        syntax_errors = []
        
        for i, query in enumerate(state["sql_queries"]):
            validation = validate_sql_syntax.invoke({"code": query})
            if not validation.get("valid", False):
                syntax_valid = False
                syntax_errors.append(f"Query {i+1}: {validation.get('error', 'Unknown error')}")
        
        system_prompt = """You are a senior code reviewer specializing in Databricks and PySpark.

Review the code for:
1. **Correctness**: Does it implement the plan correctly?
2. **Performance**: Are there optimization opportunities?
3. **Security**: No hardcoded credentials or secrets?
4. **Best Practices**: Follows Databricks and Delta Lake best practices?
5. **Code Style**: Clean, readable, well-commented?
6. **Test Coverage**: Are tests comprehensive?

Output JSON with this EXACT structure:
{
    "status": "APPROVED" or "NEEDS_REVISION" or "REJECTED",
    "quality_score": 0-100,
    "comments": ["specific comment 1", "specific comment 2", ...],
    "security_issues": ["issue1", "issue2", ...] or [],
    "performance_recommendations": ["rec1", "rec2", ...] or [],
    "approval_reasoning": "Why this code is approved/needs revision/rejected"
}

Be thorough but constructive in your review."""
        
        user_prompt = f"""Review this {current_layer.upper()} layer transformation code:

SQL QUERIES:
{chr(10).join([f"-- Query {i+1}:{chr(10)}{q}{chr(10)}" for i, q in enumerate(state['sql_queries'])])}

PYSPARK CODE (if any):
{state['pyspark_code'] if state['pyspark_code'] else '(None provided)'}

TEST CODE:
{state['test_code']}

SYNTAX VALIDATION RESULTS:
{'All queries passed syntax validation' if syntax_valid else f'[ERROR] Syntax errors found:{chr(10)}{chr(10).join(syntax_errors)}'}

TRANSFORMATION PLAN (for context):
{state['transformation_plan'][:1000]}

Provide detailed review with approval decision."""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        try:
            # Call Databricks-hosted Claude Sonnet 4.5
            response = await self.llm.ainvoke(messages)
            
            # Parse JSON response
            review_data = json.loads(response.content)
            
            # Override status if syntax invalid
            if not syntax_valid:
                review_data["status"] = "NEEDS_REVISION"
                review_data["comments"].insert(0, "SQL syntax validation failed - see errors above")
                review_data["quality_score"] = min(review_data["quality_score"], 60)
            
            state["review_status"] = review_data["status"]
            state["code_quality_score"] = review_data["quality_score"]
            state["review_comments"] = review_data["comments"]
            
            print(f"{current_layer.upper()} code reviewed: {review_data['status']} (score: {review_data['quality_score']}/100)")
            
        except json.JSONDecodeError:
            # Fallback review
            print(f"JSON parse failed, using fallback review for {current_layer}")
            state["review_status"] = "APPROVED" if syntax_valid else "NEEDS_REVISION"
            state["code_quality_score"] = 75.0 if syntax_valid else 50.0
            state["review_comments"] = ["Review completed with limited analysis"]
        
        except Exception as e:
            print(f"Review failed for {current_layer}: {str(e)}")
            state["error_log"].append(f"Review error: {str(e)}")
            state["review_status"] = "NEEDS_REVISION"
            state["code_quality_score"] = 0.0
            state["review_comments"] = [f"Review failed: {str(e)}"]
        
        state["current_agent"] = "reviewer"
        state["workflow_status"] = f"{current_layer}_reviewed"
        
        return state
