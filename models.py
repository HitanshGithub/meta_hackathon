"""
Pydantic models for the SQL Data Analyst OpenEnv environment.
Defines Action, Observation, State, and StepResult typed models.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Action
# ---------------------------------------------------------------------------

class SQLAction(BaseModel):
    """Action that an agent can take in the SQL Data Analyst environment."""

    action_type: Literal[
        "execute_query",   # Run a SQL query against the episode database
        "describe_table",  # Get schema + sample rows for a table
        "submit_answer",   # Submit final answer to be graded
        "list_tables",     # List all tables available in this episode
        "noop",            # Do nothing (burn a step)
    ] = Field(description="Type of action to perform.")

    sql_query: Optional[str] = Field(
        default=None,
        description="SQL query string (required for execute_query and describe_table).",
    )

    answer: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Final answer dict submitted to the grader (required for submit_answer). "
            "Schema depends on the active task."
        ),
    )


# ---------------------------------------------------------------------------
# Observation
# ---------------------------------------------------------------------------

class SQLObservation(BaseModel):
    """Observation returned by the environment after each step."""

    task_id: str = Field(description="Identifier of the active task.")
    goal: str = Field(description="Natural-language description of what the agent must accomplish.")
    schema_info: str = Field(description="DDL / schema description of the available tables.")
    data_sample: List[Dict[str, Any]] = Field(
        description="Up to 5 sample rows from the primary table, for orientation."
    )
    last_query_result: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Rows returned by the most recent execute_query action (None if no query yet).",
    )
    last_query_error: Optional[str] = Field(
        default=None,
        description="Error message if the last SQL query failed, otherwise None.",
    )
    last_action_error: Optional[str] = Field(
        default=None,
        description="Error from the last action (malformed action, etc.), otherwise None.",
    )
    step_count: int = Field(description="Number of steps taken so far in this episode.")
    max_steps: int = Field(description="Maximum steps allowed before episode terminates.")
    hints: Optional[List[str]] = Field(
        default=None,
        description="Optional hints unlocked as steps progress.",
    )


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class SQLState(BaseModel):
    """Episode-level state metadata."""

    episode_id: str = Field(description="Unique identifier for this episode.")
    task_id: str = Field(description="Active task identifier.")
    step_count: int = Field(description="Number of steps taken so far.")
    current_score: float = Field(description="Running score in [0.0, 1.0].")
    max_steps: int = Field(description="Maximum steps for this episode.")
    done: bool = Field(description="Whether the episode has ended.")


# ---------------------------------------------------------------------------
# StepResult  (returned by /step endpoint)
# ---------------------------------------------------------------------------

class StepResult(BaseModel):
    """Full result returned by the /step endpoint."""

    observation: SQLObservation
    reward: float = Field(description="Reward for the current step.")
    done: bool = Field(description="True if the episode has ended.")
    info: Dict[str, Any] = Field(default_factory=dict, description="Extra diagnostic info.")


# ---------------------------------------------------------------------------
# ResetResult  (returned by /reset endpoint)
# ---------------------------------------------------------------------------

class ResetResult(BaseModel):
    """Result returned by the /reset endpoint."""

    observation: SQLObservation
    done: bool = False
    info: Dict[str, Any] = Field(default_factory=dict)
