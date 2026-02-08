"""
LangChain tools exposed to the agent.
Each tool is callable by the LLM during reasoning.
"""

from langchain_core.tools import tool
from core.data_loader import load_business_data
from rag.retriever import build_policy_retriever

# Load business data once at startup
business_data = load_business_data()

# Build policy retriever once
policy_retriever = build_policy_retriever()

@tool
def policy_lookup(question: str) -> str:
    """
    Answer questions related to company policies.
    """
    result = policy_retriever.invoke({"query": question})
    return result["result"]

@tool
def average_transaction_amount() -> str:
    """
    Returns the average transaction amount in EUR.
    """
    avg = business_data["transaction_amount_eur"].mean()
    return f"The average transaction amount is {avg:.2f} EUR."

# All remaining tools stay EXACTLY as-is,
# only moved into this file.
# --- Finally, define the tools list at the very bottom ---
tools = [
    policy_lookup,
    average_transaction_amount,
    # add all other @tool functions here
]