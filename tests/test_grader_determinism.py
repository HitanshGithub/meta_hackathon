from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any, Dict, List, Tuple

from models import SQLAction
from server.environment import SQLDataAnalystEnv


def _load_deterministic_tasks() -> Dict[str, Dict[str, Any]]:
    path = Path(__file__).resolve().parents[1] / "training_queries.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["deterministic"]


def _collect_count_dict(env: SQLDataAnalystEnv, queries: List[Dict[str, str]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in queries:
        key = item["key"]
        sql = item["sql"]
        obs, _, done, _ = env.step(SQLAction(action_type="execute_query", sql_query=sql))
        if done:
            raise AssertionError("Episode ended unexpectedly while collecting count_dict answers.")
        rows = obs.last_query_result or []
        value = 0
        if rows and isinstance(rows[0], dict):
            value = int(rows[0].get("c", 0))
        counts[key] = value
    return counts


def _run_perfect_episode(env: SQLDataAnalystEnv, task_id: str, task_cfg: Dict[str, Any]) -> float:
    env.reset(task_id=task_id)
    task_type = task_cfg["type"]

    if task_type == "fixed":
        obs, _, done, _ = env.step(SQLAction(action_type="execute_query", sql_query=task_cfg["primary_sql"]))
        if done:
            raise AssertionError("Episode ended unexpectedly before submit.")
        payload = obs.last_query_result or []
        _, _, _, info = env.step(
            SQLAction(action_type="submit_answer", answer={task_cfg["submit_key"]: payload})
        )
        return float(info.get("final_score", 0.0))

    if task_type == "count_dict":
        answer = _collect_count_dict(env, task_cfg["queries"])
        _, _, _, info = env.step(SQLAction(action_type="submit_answer", answer=answer))
        return float(info.get("final_score", 0.0))

    raise AssertionError(f"Unsupported task type in test: {task_type}")


class TestGraderDeterminism(unittest.TestCase):
    def test_perfect_answers_score_one_and_are_deterministic(self) -> None:
        tasks = _load_deterministic_tasks()
        env = SQLDataAnalystEnv()
        try:
            first_pass: Dict[str, float] = {}
            second_pass: Dict[str, float] = {}

            for task_id, task_cfg in tasks.items():
                first_pass[task_id] = _run_perfect_episode(env, task_id, task_cfg)
                second_pass[task_id] = _run_perfect_episode(env, task_id, task_cfg)

            for task_id in tasks:
                self.assertGreaterEqual(first_pass[task_id], 0.99, msg=f"{task_id} expected near-perfect score")
                self.assertLessEqual(first_pass[task_id], 1.0, msg=f"{task_id} score must be <= 1.0")
                self.assertEqual(
                    round(first_pass[task_id], 4),
                    round(second_pass[task_id], 4),
                    msg=f"{task_id} grader should be deterministic",
                )
        finally:
            env.close()


if __name__ == "__main__":
    unittest.main()
