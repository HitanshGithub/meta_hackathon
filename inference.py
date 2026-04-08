from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

API_BASE_URL: str = os.environ["API_BASE_URL"]
API_KEY: str = os.environ["API_KEY"]
MODEL_NAME: str = os.environ.get("MODEL_NAME", "meta-llama/Llama-3.1-8B-Instruct:novita")
ENV_BASE_URL: str = os.environ.get("ENV_BASE_URL", "http://localhost:7860")
USE_LLM_AGENT: bool = os.environ.get("USE_LLM_AGENT", "1").strip() in {"1", "true", "True"}
PROXY_PING_REQUIRED: bool = os.environ.get("PROXY_PING_REQUIRED", "1").strip() in {"1", "true", "True"}

MAX_STEPS: int = 26
TASK_IDS: List[str] = [
    "fix_broken_query",
    "inventory_restock_alerts",
    "find_data_anomalies",
    "detect_subscription_issues",
    "repair_data_pipeline",
    "multi_channel_attribution",
]
BENCHMARK: str = "sql-data-analyst-env"
SUCCESS_SCORE_THRESHOLD: float = 0.95
FEW_SHOT_EXAMPLE_COUNT: int = int(os.environ.get("FEW_SHOT_EXAMPLE_COUNT", "5"))
HARD_EXAMPLE_RATIO: float = float(os.environ.get("HARD_EXAMPLE_RATIO", "0.6"))
MEDIUM_EXAMPLE_RATIO: float = float(os.environ.get("MEDIUM_EXAMPLE_RATIO", "0.2"))
DETERMINISTIC_HARD_QUERY_RATE: float = float(os.environ.get("DETERMINISTIC_HARD_QUERY_RATE", "0.35"))
TRAINING_QUERY_PATH: str = os.environ.get("TRAINING_QUERY_PATH", "training_queries.json")

client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)

SYSTEM_PROMPT = """You are an expert SQL data analyst agent.
Respond ONLY with compact JSON: {"action_type":"...","sql_query":"...","answer":{...}}
Allowed action_type: list_tables, describe_table, execute_query, submit_answer, noop
"""
def load_training_queries() -> Dict[str, Any]:
    path = Path(TRAINING_QUERY_PATH)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"Training query file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in training query file: {path} ({exc})") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"Training query file must contain a JSON object: {path}")
    if "few_shot" not in payload or "deterministic" not in payload:
        raise RuntimeError(f"Training query file missing required keys 'few_shot' and 'deterministic': {path}")
    return payload


TRAINING_QUERIES: Dict[str, Any] = load_training_queries()


def _clip(value: str, limit: int = 180) -> str:
    flat = " ".join(value.split())
    if len(flat) <= limit:
        return flat
    return f"{flat[: limit - 3]}..."


def _json_preview(value: Any, limit: int = 180) -> str:
    text = json.dumps(value, separators=(",", ":"), ensure_ascii=True, default=str)
    return _clip(text, limit=limit)


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: Dict[str, Any], reward: float, done: bool, error: Optional[str]) -> None:
    compact_action = json.dumps(action, separators=(",", ":"), ensure_ascii=True, default=str)
    err = "" if not error else _clip(str(error), limit=220)
    print(
        f"[STEP] step={step} action={compact_action} reward={reward:.4f} done={str(done).lower()} error={err}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.4f} rewards={_json_preview([round(r, 4) for r in rewards], limit=260)}",
        flush=True,
    )


def env_reset(task_id: str) -> Dict[str, Any]:
    resp = requests.post(f"{ENV_BASE_URL}/reset", params={"task_id": task_id}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def env_step(action: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(f"{ENV_BASE_URL}/step", json=action, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} /step failed: {resp.text}")
    return resp.json()


def validate_action(action: Dict[str, Any]) -> Dict[str, Any]:
    valid_types = {"execute_query", "describe_table", "submit_answer", "list_tables", "noop"}
    action_type = action.get("action_type")
    if action_type not in valid_types:
        return {"action_type": "noop"}
    cleaned: Dict[str, Any] = {"action_type": action_type}
    if action.get("sql_query") is not None:
        cleaned["sql_query"] = str(action["sql_query"])
    if action.get("answer") is not None:
        cleaned["answer"] = action["answer"]
    return cleaned


def build_sql_examples() -> List[Dict[str, str]]:
    few_shot = TRAINING_QUERIES.get("few_shot", {})
    easy_pool = list(few_shot.get("easy", []))
    medium_pool = list(few_shot.get("medium", []))
    hard_pool = list(few_shot.get("hard", []))
    random.shuffle(easy_pool)
    random.shuffle(medium_pool)
    random.shuffle(hard_pool)

    target_hard = max(1, int(round(FEW_SHOT_EXAMPLE_COUNT * HARD_EXAMPLE_RATIO)))
    target_medium = max(0, int(round(FEW_SHOT_EXAMPLE_COUNT * MEDIUM_EXAMPLE_RATIO)))
    target_hard = min(target_hard, len(hard_pool))
    target_medium = min(target_medium, len(medium_pool))
    target_easy = max(0, FEW_SHOT_EXAMPLE_COUNT - target_hard - target_medium)
    target_easy = min(target_easy, len(easy_pool))

    examples = hard_pool[:target_hard] + medium_pool[:target_medium] + easy_pool[:target_easy]
    if len(examples) < FEW_SHOT_EXAMPLE_COUNT:
        shortfall = FEW_SHOT_EXAMPLE_COUNT - len(examples)
        leftovers = hard_pool[target_hard:] + medium_pool[target_medium:] + easy_pool[target_easy:]
        examples.extend(leftovers[:shortfall])
    random.shuffle(examples)
    return examples


def choose_query(primary_sql: str, harder_variants: List[str]) -> str:
    if harder_variants and random.random() < DETERMINISTIC_HARD_QUERY_RATE:
        return random.choice(harder_variants)
    return primary_sql


def llm_action(observation: Dict[str, Any], history: List[str]) -> Dict[str, Any]:
    user_prompt = json.dumps(
        {
            "task_id": observation.get("task_id"),
            "goal": observation.get("goal"),
            "schema_info": observation.get("schema_info"),
            "last_query_result": observation.get("last_query_result"),
            "last_query_error": observation.get("last_query_error"),
            "last_action_error": observation.get("last_action_error"),
            "history": history[-6:],
            "sql_examples": build_sql_examples(),
        },
        ensure_ascii=True,
    )
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=256,
            stream=False,
        )
        text = (completion.choices[0].message.content or "").strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:].strip()
        return validate_action(json.loads(text))
    except Exception:
        return {"action_type": "noop"}


def ping_llm_proxy() -> None:
    """Ensure at least one request is sent through the configured LLM proxy."""
    try:
        # More robust than a model-specific completion call.
        client.models.list()
        print("[INFO] proxy_ping=ok", flush=True)
    except Exception as exc:
        print(f"[ERROR] proxy_ping_failed={exc}", flush=True)
        if PROXY_PING_REQUIRED:
            raise


def execute_and_log(step_no: int, action: Dict[str, Any], rewards: List[float]) -> Dict[str, Any]:
    action = validate_action(action)
    result = env_step(action)
    reward = float(result.get("reward", 0.0) or 0.0)
    done = bool(result.get("done", False))
    obs = result["observation"]
    error = obs.get("last_action_error") or obs.get("last_query_error")
    log_step(step=step_no, action=action, reward=reward, done=done, error=error)
    rewards.append(reward)
    return result


def run_fixed_query_task(task_id: str, sql: str, submit_key: str) -> float:
    rewards: List[float] = []
    steps = 0
    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)
    env_reset(task_id)

    steps += 1
    result = execute_and_log(steps, {"action_type": "execute_query", "sql_query": sql}, rewards)
    payload = result["observation"].get("last_query_result") or []

    steps += 1
    submit = execute_and_log(steps, {"action_type": "submit_answer", "answer": {submit_key: payload}}, rewards)

    score = float(submit.get("info", {}).get("final_score", 0.0) or 0.0)
    log_end(success=score >= SUCCESS_SCORE_THRESHOLD, steps=steps, score=score, rewards=rewards)
    return score


def run_count_dict_task(task_id: str, queries: List[Tuple[str, str]]) -> float:
    rewards: List[float] = []
    steps = 0
    counts: Dict[str, int] = {}
    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)
    env_reset(task_id)

    for key, sql in queries:
        steps += 1
        result = execute_and_log(steps, {"action_type": "execute_query", "sql_query": sql}, rewards)
        rows = result["observation"].get("last_query_result") or []
        value = 0
        if rows and isinstance(rows[0], dict):
            try:
                value = int(rows[0].get("c", 0))
            except (TypeError, ValueError):
                value = 0
        counts[key] = value

    steps += 1
    submit = execute_and_log(steps, {"action_type": "submit_answer", "answer": counts}, rewards)
    score = float(submit.get("info", {}).get("final_score", 0.0) or 0.0)
    log_end(success=score >= SUCCESS_SCORE_THRESHOLD, steps=steps, score=score, rewards=rewards)
    return score


def run_deterministic(task_id: str) -> float:
    task_cfg = TRAINING_QUERIES.get("deterministic", {}).get(task_id)
    if not isinstance(task_cfg, dict):
        raise ValueError(f"Unsupported task id: {task_id}")

    task_type = task_cfg.get("type")
    if task_type == "fixed":
        primary_sql = str(task_cfg.get("primary_sql", ""))
        hard_variants = [str(x) for x in task_cfg.get("hard_variants", [])]
        submit_key = str(task_cfg.get("submit_key", "rows"))
        if not primary_sql:
            raise ValueError(f"Missing primary_sql for task id: {task_id}")
        sql = choose_query(primary_sql, hard_variants)
        return run_fixed_query_task(task_id, sql, submit_key)

    if task_type == "count_dict":
        raw_queries = task_cfg.get("queries", [])
        parsed_queries: List[Tuple[str, str]] = []
        for item in raw_queries:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "")).strip()
            sql = str(item.get("sql", "")).strip()
            if key and sql:
                parsed_queries.append((key, sql))
        if not parsed_queries:
            raise ValueError(f"No valid count_dict queries for task id: {task_id}")
        return run_count_dict_task(task_id, parsed_queries)

    raise ValueError(f"Unsupported deterministic task type '{task_type}' for task id: {task_id}")


def run_episode_with_llm(task_id: str) -> float:
    rewards: List[float] = []
    history: List[str] = []
    steps_taken = 0

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)
    reset_result = env_reset(task_id)
    result = {"observation": reset_result["observation"], "done": False, "reward": 0.0, "info": {}}

    for step in range(1, MAX_STEPS + 1):
        if result.get("done"):
            break
        action = llm_action(result["observation"], history)
        steps_taken = step
        result = execute_and_log(step, action, rewards)
        history.append(json.dumps(action, ensure_ascii=True))

    score = float(result.get("info", {}).get("final_score", 0.0) or 0.0)
    log_end(success=score >= SUCCESS_SCORE_THRESHOLD, steps=steps_taken, score=score, rewards=rewards)
    return score


def main() -> None:
    global ENV_BASE_URL

    parser = argparse.ArgumentParser(description="SQL Data Analyst Env - Baseline Inference")
    parser.add_argument("--env-url", default=None)
    args = parser.parse_args()

    if args.env_url:
        ENV_BASE_URL = args.env_url.rstrip("/")

    print(f"Connecting to environment at {ENV_BASE_URL} ...", flush=True)
    try:
        health_resp = requests.get(f"{ENV_BASE_URL}/health", timeout=10)
        health_resp.raise_for_status()
        print(f"Health: {health_resp.json()}", flush=True)
    except Exception as exc:
        print(f"ERROR: Cannot reach environment server: {exc}", flush=True)
        sys.exit(1)

    print(f"Model: {MODEL_NAME}", flush=True)
    print(f"API:   {API_BASE_URL}", flush=True)
    print(f"Mode:  {'LLM' if USE_LLM_AGENT else 'deterministic'}", flush=True)
    print(f"Training Queries: {TRAINING_QUERY_PATH}", flush=True)
    try:
        ping_llm_proxy()
    except Exception as exc:
        print(f"ERROR: LLM proxy validation failed: {exc}", flush=True)
        sys.exit(1)

    total_start = time.time()
    scores: Dict[str, float] = {}

    try:
        for task_id in TASK_IDS:
            scores[task_id] = run_episode_with_llm(task_id) if USE_LLM_AGENT else run_deterministic(task_id)
    except Exception as exc:
        print(f"ERROR: Inference failed: {exc}", flush=True)
        sys.exit(1)

    avg = sum(scores.values()) / len(scores)
    elapsed = round(time.time() - total_start, 1)
    print("\n=== RUN SUMMARY ===", flush=True)
    for task_id in TASK_IDS:
        print(f"{task_id}: {scores.get(task_id, 0.0):.4f}", flush=True)
    print(f"average: {avg:.4f}", flush=True)
    print(f"elapsed_seconds: {elapsed:.1f}", flush=True)


if __name__ == "__main__":
    main()
