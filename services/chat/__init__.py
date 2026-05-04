"""Chat / agent services for the company-page sidebar.

Public surface:
- run_agent_stream — streaming agent loop with tool use
- TOOL_DEFINITIONS — OpenRouter-shaped tool schemas exposed to the model
"""

from services.chat.agent import run_agent_stream
from services.chat.tools import TOOL_DEFINITIONS

__all__ = ["run_agent_stream", "TOOL_DEFINITIONS"]
