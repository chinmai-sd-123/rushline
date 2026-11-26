"""The plain version: a tool-calling loop with no framework (roadmap: before LangGraph).

Useful to see the mechanics. It can't pause for approval and resume later, so the real
agent is the LangGraph graph in graph.py.
"""

import json

from rushline.agent.gateway import Gateway
from rushline.agent.prompts import SYSTEM_PROMPT
from rushline.agent.tools import SCHEMAS, ApiClient, as_message, execute


async def run_simple(
    gateway: Gateway, api: ApiClient, user_id: int, question: str, max_steps: int = 8
) -> tuple[str, list[dict]]:
    """Returns (answer, messages). Stops at the first approval request."""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question}]
    for _ in range(max_steps):
        reply = await gateway.chat(user_id, messages, SCHEMAS)
        if not reply.tool_calls:
            messages.append({"role": "assistant", "content": reply.content})
            return reply.content or "", messages
        messages.append(
            {
                "role": "assistant",
                "content": reply.content,
                "tool_calls": [
                    {
                        "id": c.id,
                        "type": "function",
                        "function": {"name": c.name, "arguments": json.dumps(c.arguments)},
                    }
                    for c in reply.tool_calls
                ],
            }
        )
        for call in reply.tool_calls:
            result = await execute(api, call.name, call.arguments, call.id)
            messages.append(
                {"role": "tool", "tool_call_id": call.id, "content": as_message(result)}
            )
            if call.name == "request_payment_approval" and result["status"] == 201:
                return "Waiting for your approval to pay.", messages
    return "I stopped after too many steps.", messages
