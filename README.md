# SQL Data Analyst Environment

An OpenEnv-compatible environment where agents solve practical SQL/data-ops workflows over SQLite.

## What Is Included

- OpenEnv API: `/reset`, `/step`, `/state`
- Typed models for action, observation, state, and step/reset responses
- 6 graded tasks across easy/medium/hard
- Deterministic baseline `inference.py` with structured logs: `[START]`, `[STEP]`, `[END]`
- Dockerfile + HF Space deploy support

## Task Set

| Task ID | Difficulty | Objective |
|---|---|---|
| `fix_broken_query` | easy | Repair a broken SQL query and submit top-5 product revenue rows |
| `inventory_restock_alerts` | easy | Detect low-stock products and submit deficit-prioritized restock rows |
| `find_data_anomalies` | medium | Count null emails, duplicate IDs, negative ages, invalid statuses |
| `detect_subscription_issues` | medium | Count expired-active, delinquent, autopay mismatch, duplicate users |
| `repair_data_pipeline` | hard | Fix aggregation pipeline to avoid JOIN double-counting |
| `multi_channel_attribution` | hard | Build first-touch channel attribution from sessions + conversions |

## Action Space

```json
{
  "action_type": "execute_query | describe_table | list_tables | submit_answer | noop",
  "sql_query": "<optional SQL string or table name>",
  "answer": {}
}
```

## Reward Shape

- `+0.05` successful `execute_query`
- `+0.01` / `+0.02` exploration actions (`list_tables`, `describe_table`)
- `-0.02` invalid SQL
- `-0.01` `noop`
- `submit_answer` reward = grader score `[0.0, 1.0]`

## Run Locally

```bash
pip install -r requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 7860
```

Health check:

```bash
curl http://localhost:7860/health
```

## Run Baseline Inference

```bash
export API_BASE_URL="https://router.huggingface.co/v1"
export MODEL_NAME="meta-llama/Llama-3.1-8B-Instruct:novita"
export HF_TOKEN="<your_token>"
export ENV_BASE_URL="http://localhost:7860"
python inference.py
```

Default mode is deterministic for reproducible evaluation. To run LLM control mode:

```bash
USE_LLM_AGENT=1 python inference.py
```

## Docker

```bash
docker build -t sql-data-analyst-env .
docker run -p 7860:7860 sql-data-analyst-env
```

## Validation

```bash
pip install openenv-core
openenv validate
```
