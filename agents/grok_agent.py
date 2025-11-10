# full_implementation_maf.py
# Complete Python implementation for the conversational AI agent using Microsoft Agent Framework (MAF)
# This supersedes AutoGen, unifying it with Semantic Kernel. Data from CSVs on GitHub Copilot usage.
# Assumptions:
# - CSVs in current directory, e.g., 'usage_*.csv', 'requests_*.csv'.
# - Env vars: AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_API_VERSION.
# - Install: pip install microsoft-agent-framework fastapi uvicorn pandas sqlite3 streamlit openai

import os
import pandas as pd
import sqlite3
import glob
import threading
import asyncio
import requests
from typing import List, Dict, Any, Optional
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import streamlit as st
from microsoft_agent_framework import ChatAgent, AzureChatClient, McpClient
from microsoft_agent_framework.types import Thread, Message

# Step 1: Data Layer - Load CSVs into in-memory SQLite
def load_data(csv_pattern='*.csv') -> sqlite3.Connection:
    conn = sqlite3.connect(':memory:')
    usage_dfs = []
    requests_dfs = []
    for file in glob.glob(csv_pattern):
        df = pd.read_csv(file)
        if 'usage' in file.lower():
            usage_dfs.append(df)
        elif 'requests' in file.lower():
            requests_dfs.append(df)
    if usage_dfs:
        usage_df = pd.concat(usage_dfs, ignore_index=True)
        usage_df.to_sql('usage', conn, if_exists='replace', index=False)
    if requests_dfs:
        requests_df = pd.concat(requests_dfs, ignore_index=True)
        requests_df.to_sql('requests', conn, if_exists='replace', index=False)
    # Create merged view
    conn.execute("""
        CREATE VIEW IF NOT EXISTS merged_data AS
        SELECT u.developer_id, u.month_year, u.division, u.used_copilot,
               r.request_id, r.model_used, r.lines_suggested, r.lines_accepted, r.chat_type
        FROM usage u
        LEFT JOIN requests r ON u.developer_id = r.developer_id AND u.month_year = r.month_year
    """)
    return conn

# Step 2: Tools Layer - Define analysis functions (exposed via MCP)
class ToolInput(BaseModel):
    division: Optional[str] = None
    month_year: Optional[str] = None
    model: Optional[str] = None
    period: str = 'month'
    n: int = 10
    metric: str = 'accepted_lines'

def get_monthly_adoption(conn: sqlite3.Connection, division: Optional[str] = None, month_year: Optional[str] = None) -> List[Dict[str, Any]]:
    query = """
        SELECT division, month_year, COUNT(DISTINCT developer_id) as total_users,
               SUM(CASE WHEN used_copilot = 1 THEN 1 ELSE 0 END) as copilot_users
        FROM usage
    """
    conditions = []
    params = []
    if division:
        conditions.append("division = ?")
        params.append(division)
    if month_year:
        conditions.append("month_year = ?")
        params.append(month_year)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY division, month_year"
    df = pd.read_sql(query, conn, params=params)
    if not df.empty:
        df['adoption_rate'] = (df['copilot_users'] / df['total_users'] * 100).round(2)
    return df.to_dict('records')

def get_request_stats(conn: sqlite3.Connection, model: Optional[str] = None, period: str = 'month') -> List[Dict[str, Any]]:
    if period == 'quarter':
        group_by = "strftime('%Y-%m', month_year) || '-Q' || CAST(((strftime('%m', month_year) - 1)/3 + 1) AS TEXT)"
    else:
        group_by = 'month_year'
    query = f"""
        SELECT {group_by} as period, model_used,
               SUM(lines_suggested) as total_suggested,
               SUM(lines_accepted) as total_accepted,
               COUNT(request_id) as total_requests
        FROM requests
    """
    params = []
    if model:
        query += " WHERE model_used = ?"
        params.append(model)
    query += f" GROUP BY period, model_used"
    df = pd.read_sql(query, conn, params=params)
    if not df.empty:
        df['acceptance_rate'] = (df['total_accepted'] / df['total_suggested'].replace(0, float('nan')) * 100).round(2)
    return df.to_dict('records')

def get_top_users(conn: sqlite3.Connection, n: int = 10, metric: str = 'accepted_lines', period: str = 'all') -> List[Dict[str, Any]]:
    if metric == 'accepted_lines':
        agg = 'SUM(COALESCE(lines_accepted, 0))'
    else:
        agg = 'COUNT(request_id)'
    query = f"""
        SELECT developer_id, division, {agg} as value
        FROM merged_data
    """
    params = []
    if period != 'all':
        query += " WHERE month_year = ?"
        params.append(period)
    query += f" GROUP BY developer_id, division ORDER BY value DESC LIMIT ?"
    params.append(n)
    df = pd.read_sql(query, conn, params=params)
    return df.to_dict('records')

# Step 3: MCP Server - FastAPI endpoints for tools (MAF integrates via McpClient)
app = FastAPI(title="MCP Server for Copilot Analysis")

class ToolRequest(BaseModel):
    tool_name: str
    params: Dict[str, Any]

@app.post("/mcp/tools/execute")
async def execute_tool(request: ToolRequest) -> Dict[str, Any]:
    global db_conn
    tools_map = {
        "get_monthly_adoption": lambda p: get_monthly_adoption(db_conn, **p),
        "get_request_stats": lambda p: get_request_stats(db_conn, **p),
        "get_top_users": lambda p: get_top_users(db_conn, **p)
    }
    if request.tool_name not in tools_map:
        return {"error": "Tool not found"}
    result = tools_map[request.tool_name](request.params)
    return {"result": result, "tool_name": request.tool_name}

def run_mcp_server():
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")

# Step 4: Agent Layer - Using Microsoft Agent Framework
async def create_agent() -> ChatAgent:
    # Azure Chat Client
    client = AzureChatClient(
        model="gpt-4o",
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION")
    )
    
    # MCP Client for tools
    mcp_client = McpClient(server_url="http://127.0.0.1:8000/mcp")
    tools = await mcp_client.get_tools()  # Discovers and wraps MCP tools
    
    agent = ChatAgent(
        name="copilot_analyst",
        chat_client=client,
        tools=tools,
        instructions="""You are a GitHub Copilot usage analyst for company management. 
        Analyze historic data on developer adoption, requests, models used, suggestions, and acceptances. 
        Use tools like get_monthly_adoption, get_request_stats, get_top_users to fetch data. 
        Respond conversationally, explain trends, and suggest insights. Use tables for data presentation."""
    )
    return agent

async def chat_with_agent(agent: ChatAgent, user_message: str, thread: Optional[Thread] = None) -> str:
    if thread is None:
        thread = Thread()
    
    # Create user message
    user_msg = Message(content=user_message, role="user")
    await agent.process_message(thread, [user_msg])
    
    # Get assistant response
    assistant_messages = [msg for msg in thread.messages if msg.role == "assistant"]
    if assistant_messages:
        return assistant_messages[-1].content
    return "No response generated."

# Step 5: Streamlit UI for Conversational Interface
async def main():
    global db_conn
    db_conn = load_data()

    # Start MCP server in background thread
    mcp_thread = threading.Thread(target=run_mcp_server, daemon=True)
    mcp_thread.start()
    await asyncio.sleep(2)  # Wait for server to start

    # Create agent
    agent = await create_agent()

    st.title("GitHub Copilot Usage Analyzer (Microsoft Agent Framework)")

    # Initialize chat history and thread
    if "thread" not in st.session_state:
        st.session_state.thread = Thread()
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # User input
    if prompt := st.chat_input("Ask about Copilot usage (e.g., 'Adoption in Engineering last month')"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            # Run async chat
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            response = loop.run_until_complete(chat_with_agent(agent, prompt, st.session_state.thread))
            loop.close()
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})

if __name__ == "__main__":
    # Run Streamlit with asyncio
    asyncio.run(main())
