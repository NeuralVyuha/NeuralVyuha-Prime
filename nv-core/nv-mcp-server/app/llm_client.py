import os
import json
import logging
import httpx
from typing import TypedDict, Annotated, Sequence
import operator
from langgraph.graph import StateGraph, END
import litellm
from litellm import acompletion
from .tools import AVAILABLE_TOOLS, execute_tool

logger = logging.getLogger(__name__)


# Enable function calling mapping in litellm
litellm.drop_params = True

def add_messages(left: list, right: list):
    return left + right

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    provider: str
    model_name: str

async def agent_node(state: AgentState):
    provider = state["provider"]
    model_name = state["model_name"]
    messages = state["messages"]

    api_key = None
    api_base = None

    # Try to fetch from Node Service (Integration Hub)
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Note: In production, we'd use a service token. 
            # For now, we assume internal network trust or env-based auth.
            node_svc_url = os.environ.get("NODE_SERVICE_URL", "http://node-service:8085")
            resp = await client.get(f"{node_svc_url}/internal/vault/credentials/{provider}")
            if resp.status_code == 200:
                data = resp.json()
                api_key = data.get("api_key")
                if api_key:
                    api_key = api_key.strip()
                if data.get("url"):
                    api_base = data.get("url")
                logger.info(f"llm_client: Successfully fetched {provider} credentials from Integration Hub")
    except Exception as e:
         logger.error(f"llm_client: FAILED to fetch {provider} credentials from {node_svc_url}: {e}")
         # Fallback to env vars (default litellm behavior)
         pass

    if provider == "ollama":
        litellm_model = f"ollama/{model_name}"
        if not api_base:
            api_base = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
    elif provider == "openai":
        litellm_model = model_name
    elif provider == "claude":
        litellm_model = f"anthropic/{model_name}" if not model_name.startswith("claude") else model_name
    elif provider == "gemini":
        if model_name == "gemini-1.5-flash":
            model_name = "gemini-2.5-flash"
        litellm_model = f"gemini/{model_name}"
    elif provider == "moonshot":
        litellm_model = f"moonshot/{model_name}"
    elif provider == "deepseek":
        litellm_model = f"deepseek/{model_name}"
    else:
        raise ValueError(f"Unknown provider: {provider}")

    kwargs = {
        "model": litellm_model,
        "messages": messages,
        "tools": AVAILABLE_TOOLS
    }
    if api_key:
        kwargs["api_key"] = api_key
        logger.info(f"Using fetched API key for {provider} starting with {api_key[:10]}...")
    else:
        logger.warning(f"NO API KEY FOUND FOR {provider}!")
        
    if api_base:
        kwargs["api_base"] = api_base

    try:
        res = await acompletion(**kwargs, drop_params=True)
        new_msg = res.choices[0].message.model_dump()
        return {"messages": [new_msg]}
    except litellm.exceptions.AuthenticationError as e:
        logger.error(f"LITELLM AUTH ERROR for {provider} ({litellm_model}): {e}")
        return {"messages": [{"role": "assistant", "content": f"ERROR: Authentication Failure - Check your API Key for {provider}."}]}
    except Exception as e:
        logger.error(f"LITELLM GENERAL ERROR for {provider} ({litellm_model}): {type(e)} - {e}")
        return {"messages": [{"role": "assistant", "content": f"ERROR: AI Engine Failure ({provider}) - {str(e)}"}]}

async def tool_node(state: AgentState):
    last_message = state["messages"][-1]
    tool_calls = last_message.get("tool_calls", [])
    
    tool_messages = []
    for tool_call in tool_calls:
        try:
            func = tool_call["function"]
            f_name = func["name"]
            args = func["arguments"]
            if isinstance(args, str):
                args = json.loads(args)
                
            result = execute_tool(f_name, args)
            tool_messages.append({
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "name": f_name,
                "content": json.dumps(result)
            })
        except Exception as e:
            tool_messages.append({
                "role": "tool",
                "tool_call_id": tool_call.get("id", "unknown"),
                "name": f_name if 'f_name' in locals() else "unknown",
                "content": json.dumps({"error": str(e)})
            })
            
    return {"messages": tool_messages}

def should_continue(state: AgentState):
    last_message = state["messages"][-1]
    
    # Check if there are tool calls in the last message
    if last_message.get("tool_calls"):
        return "tools"
    
    # Otherwise, stop the graph
    return END

# Build the Graph
workflow = StateGraph(AgentState)
workflow.add_node("agent", agent_node)
workflow.add_node("tools", tool_node)
workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue)
workflow.add_edge("tools", "agent")
app = workflow.compile()


async def chat_with_copilot(user_message: str, provider: str, model_name: str, context: dict = None) -> str:
    system_prompt = "You are NeuralVyuha Threat Copilot, an elite AI cybersecurity analyst. You have access to SIEM tools to hunt for logs and cases. Use them proactively to answer the user."
    if context:
        system_prompt += f"\n\nCurrent Context:\n{json.dumps(context)}"

    initial_state = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        "provider": provider,
        "model_name": model_name
    }

    try:
        final_state = await app.ainvoke(initial_state)
        return final_state["messages"][-1].get("content", "")
    except Exception as e:
        return f"ERROR: AI Agent Execution Failure - {str(e)}"

async def chat_with_copilot_stream(user_message: str, provider: str, model_name: str, context: dict = None):
    system_prompt = "You are NeuralVyuha Threat Copilot, an elite AI cybersecurity analyst. You have access to SIEM tools to hunt for logs and cases. Use them proactively to answer the user."
    if context:
        system_prompt += f"\n\nCurrent Context:\n{json.dumps(context)}"

    initial_state = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        "provider": provider,
        "model_name": model_name
    }

    try:
        async for event in app.astream(initial_state):
            if "agent" in event:
                msg = event["agent"]["messages"][0]
                if msg.get("tool_calls"):
                    for tc in msg["tool_calls"]:
                        fn_name = tc.get("function", {}).get("name", "unknown")
                        payload = {'type': 'thought', 'content': f'Tool Execution Phase: Calling `{fn_name}`...'}
                        yield f"data: {json.dumps(payload)}\n\n"
                elif msg.get("content"):
                    payload = {'type': 'message', 'content': msg['content']}
                    yield f"data: {json.dumps(payload)}\n\n"
            elif "tools" in event:
                tool_msgs = event["tools"]["messages"]
                for tm in tool_msgs:
                    name = tm.get("name", "tool")
                    payload = {'type': 'thought', 'content': f'Result Acquired: Parsed metadata from `{name}`.'}
                    yield f"data: {json.dumps(payload)}\n\n"
                    
        yield "data: [DONE]\n\n"
    except Exception as e:
        payload = {'type': 'error', 'content': f'AI Engine Failure: {str(e)}'}
        yield f"data: {json.dumps(payload)}\n\n"
        yield "data: [DONE]\n\n"
