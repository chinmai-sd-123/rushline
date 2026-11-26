"""The agent as a LangGraph graph, with checkpoints and a human approval interrupt.

    model -> tools -> model ... -> END
                  \\-> wait_for_user (interrupt) -> model

wait_for_user holds nothing but the interrupt: LangGraph re-runs a whole node on resume,
so any side effect in that node would happen twice. The approval request itself is made
in the tools node, which is checkpointed before the pause.
"""

import json
import operator
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from rushline.agent.gateway import Gateway
from rushline.agent.prompts import SYSTEM_PROMPT
from rushline.agent.tools import SCHEMAS, ApiClient, as_message, execute


class AgentState(TypedDict):
    messages: Annotated[list[dict], operator.add]  # OpenAI-format chat messages
    pending_approval: dict | None
    tokens_in: int  # this conversation's usage so far (the budget is per conversation)
    tokens_out: int
    cost_usd: float
    steps: int  # model calls in the current run


def build_graph(
    gateway: Gateway,
    api: ApiClient,
    user_id: int,
    run_id: int,
    max_steps: int,
    conversation_token_budget: int,
    checkpointer,
):
    """Built per run: the API client (with its token) stays in a closure, never in state."""

    async def model(state: AgentState) -> dict:
        used = state.get("tokens_in", 0) + state.get("tokens_out", 0)
        over_budget = used >= conversation_token_budget
        if state.get("steps", 0) >= max_steps or over_budget:
            stop = "I've hit my limit for this request. Please try a simpler request."
            return {"messages": [{"role": "assistant", "content": stop}]}
        reply = await gateway.chat(
            user_id, [{"role": "system", "content": SYSTEM_PROMPT}, *state["messages"]], SCHEMAS
        )
        message: dict = {"role": "assistant", "content": reply.content}
        if reply.tool_calls:
            message["tool_calls"] = [
                {
                    "id": c.id,
                    "type": "function",
                    "function": {"name": c.name, "arguments": json.dumps(c.arguments)},
                }
                for c in reply.tool_calls
            ]
        return {
            "messages": [message],
            "tokens_in": state.get("tokens_in", 0) + reply.tokens_in,
            "tokens_out": state.get("tokens_out", 0) + reply.tokens_out,
            "cost_usd": state.get("cost_usd", 0.0) + reply.cost_usd,
            "steps": state.get("steps", 0) + 1,
        }

    async def tools(state: AgentState) -> dict:
        results, pending = [], None
        for call in state["messages"][-1]["tool_calls"]:
            name = call["function"]["name"]
            try:
                args = json.loads(call["function"]["arguments"] or "{}")
            except json.JSONDecodeError:
                args = {}
            result = await execute(api, name, args, f"run{run_id}-{call['id']}")
            if name == "request_payment_approval" and result["status"] == 201:
                pending = result["body"]
            results.append(
                {"role": "tool", "tool_call_id": call["id"], "content": as_message(result)}
            )
        return {"messages": results, "pending_approval": pending}

    async def wait_for_user(state: AgentState) -> dict:
        approval = state["pending_approval"]
        decision = interrupt(approval)  # the run pauses here until the user decides
        verb = "approved" if decision == "approve" else "rejected"
        note = (
            f"The user {verb} payment approval {approval['approval_id']} "
            f"for hold {approval['hold_id']}."
        )
        return {"messages": [{"role": "system", "content": note}], "pending_approval": None}

    def after_model(state: AgentState) -> str:
        return "tools" if state["messages"][-1].get("tool_calls") else END

    def after_tools(state: AgentState) -> str:
        return "wait_for_user" if state.get("pending_approval") else "model"

    graph = StateGraph(AgentState)
    graph.add_node("model", model)
    graph.add_node("tools", tools)
    graph.add_node("wait_for_user", wait_for_user)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", after_model, ["tools", END])
    graph.add_conditional_edges("tools", after_tools, ["wait_for_user", "model"])
    graph.add_edge("wait_for_user", "model")
    return graph.compile(checkpointer=checkpointer)
