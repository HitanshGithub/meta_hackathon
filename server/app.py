"""
FastAPI server for the SQL Data Analyst OpenEnv environment.
Exposes /reset, /step, /state, /health, and /tasks endpoints.
"""

from __future__ import annotations

import sys
import os

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from models import ResetResult, SQLAction, SQLObservation, SQLState, StepResult
from server.environment import SQLDataAnalystEnv, TASKS

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SQL Data Analyst Environment",
    description=(
        "An OpenEnv-compatible agentic environment where AI agents must analyze "
        "SQLite databases, fix broken queries, detect data anomalies, and repair "
        "data pipelines. Implements the OpenEnv step()/reset()/state() API."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global environment instance (single-session server — sufficient for HF Spaces + evaluation)
_env = SQLDataAnalystEnv()
_last_obs: Optional[SQLObservation] = None
_last_done: bool = False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", summary="Health check")
def health() -> Dict[str, str]:
    """Liveness probe — returns 200 if server is running."""
    return {"status": "ok", "environment": "sql-data-analyst-env", "version": "1.0.0"}


@app.get("/tasks", summary="List available tasks")
def list_tasks() -> Dict[str, Any]:
    """Return metadata for all available tasks."""
    return {
        "tasks": [
            {
                "id": t["id"],
                "difficulty": t["difficulty"],
                "goal": t["goal"],
                "max_steps": t["max_steps"],
            }
            for t in TASKS.values()
        ]
    }


@app.post("/reset", response_model=ResetResult, summary="Reset the environment")
def reset(task_id: Optional[str] = Query(default=None, description="Task ID to load. If omitted, cycles through tasks.")) -> ResetResult:
    """
    Initialize a new episode.
    Returns the initial observation.
    """
    global _last_obs, _last_done
    try:
        obs, info = _env.reset(task_id=task_id)
        _last_obs = obs
        _last_done = False
        return ResetResult(observation=obs, done=False, info=info)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/step", response_model=StepResult, summary="Take a step in the environment")
def step(action: SQLAction) -> StepResult:
    """
    Execute an action and return (observation, reward, done, info).

    Action types:
    - `execute_query`: Run a SQL query. Requires `sql_query`.
    - `describe_table`: Get schema + sample for a table. Set `sql_query` = table name.
    - `list_tables`: List all tables in the episode database.
    - `submit_answer`: Submit final answer to the grader. Requires `answer` dict.
    - `noop`: Do nothing.
    """
    global _last_obs, _last_done
    try:
        obs, reward, done, info = _env.step(action)
        _last_obs = obs
        _last_done = done
        return StepResult(observation=obs, reward=reward, done=done, info=info)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/state", response_model=SQLState, summary="Get current episode state")
def state() -> SQLState:
    """Return the current episode-level state metadata."""
    try:
        return _env.state()
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/observation", response_model=SQLObservation, summary="Get current observation")
def observation() -> SQLObservation:
    """Return the last observation (convenience endpoint)."""
    if _last_obs is None:
        raise HTTPException(status_code=400, detail="No observation yet. Call /reset first.")
    return _last_obs

def main():
    import uvicorn
    uvicorn.run("server.app:app", host="0.0.0.0", port=7860, reload=False)

if __name__ == "__main__":
    main()
