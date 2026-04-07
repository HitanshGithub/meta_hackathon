"""
Core environment logic for the SQL Data Analyst OpenEnv environment.

Six tasks:
  - Easy:   fix_broken_query, inventory_restock_alerts
  - Medium: find_data_anomalies, detect_subscription_issues
  - Hard:   repair_data_pipeline, multi_channel_attribution
"""

from __future__ import annotations

import re
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Tuple

from models import SQLAction, SQLObservation, SQLState

GRADER_VERSION = "2026-04-08.sql-backed.v2"


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

TASKS: Dict[str, Dict[str, Any]] = {
    "fix_broken_query": {
        "id": "fix_broken_query",
        "difficulty": "easy",
        "goal": (
            "The `sales` table contains product sales data. "
            "You have been given a SQL query that has exactly 3 syntax errors. "
            "Fix the query, execute it, and submit the result as a list of rows under the key 'rows'. "
            "The query should return: product_name, total_revenue (SUM of price*quantity), "
            "grouped by product_name, ordered by total_revenue DESC, limited to top 5."
        ),
        "max_steps": 15,
        "hints": [
            "Step 5: Look carefully at the SELECT, GROUP BY, and ORDER BY clauses.",
            "Step 10: The errors involve a misspelled keyword, a missing comma, and a malformed ORDER BY.",
        ],
    },
    "inventory_restock_alerts": {
        "id": "inventory_restock_alerts",
        "difficulty": "easy",
        "goal": (
            "The `inventory` table tracks stock levels and reorder thresholds. "
            "Find all products that need restocking where stock_on_hand <= reorder_level. "
            "Submit a list of rows under key 'rows' with: product_name, stock_on_hand, reorder_level, deficit_units "
            "where deficit_units = reorder_level - stock_on_hand. "
            "Order by deficit_units DESC, then product_name ASC."
        ),
        "max_steps": 15,
        "hints": [
            "Step 4: Use a WHERE clause with <= for stock threshold.",
            "Step 9: Compute deficit_units using subtraction in SELECT.",
        ],
    },
    "find_data_anomalies": {
        "id": "find_data_anomalies",
        "difficulty": "medium",
        "goal": (
            "The `customers` table has intentional data quality issues. "
            "Find ALL anomalies and submit a dict with these keys: "
            "'null_email_count' (int), 'duplicate_customer_id_count' (int), "
            "'negative_age_count' (int), 'invalid_status_count' (int). "
            "Valid statuses are: 'active', 'inactive', 'pending'."
        ),
        "max_steps": 20,
        "hints": [
            "Step 6: Use IS NULL to find missing emails.",
            "Step 12: Use GROUP BY + HAVING COUNT(*) > 1 to find duplicates.",
        ],
    },
    "detect_subscription_issues": {
        "id": "detect_subscription_issues",
        "difficulty": "medium",
        "goal": (
            "The `subscriptions` table contains billing lifecycle data. "
            "Submit a dict with exact counts for these keys: "
            "'expired_active_count', 'delinquent_count', 'autopay_mismatch_count', 'duplicate_user_count'. "
            "Definitions: expired_active = status='active' and end_date < '2024-02-01'; "
            "delinquent = last_payment_days_ago > 30; "
            "autopay_mismatch = status='canceled' and autopay_enabled=1; "
            "duplicate_user = same user_id appears more than once."
        ),
        "max_steps": 22,
        "hints": [
            "Step 7: Use date comparison on end_date for expired active subscriptions.",
            "Step 14: Duplicate users can be counted with GROUP BY user_id HAVING COUNT(*) > 1.",
        ],
    },
    "repair_data_pipeline": {
        "id": "repair_data_pipeline",
        "difficulty": "hard",
        "goal": (
            "You have two tables: `orders` and `order_items`. "
            "The business wants a report: for each category, compute total_orders (count of distinct order_ids), "
            "total_units_sold (SUM of quantity), and avg_order_value (AVG of order total). "
            "A broken pipeline query exists that double-counts due to a bad JOIN. "
            "Fix it and submit the correct result as a list of dicts under key 'report', "
            "each dict with keys: category, total_orders, total_units_sold, avg_order_value (rounded to 2dp). "
            "Order by total_orders DESC."
        ),
        "max_steps": 25,
        "hints": [
            "Step 7: Think about per-order totals before category aggregation.",
            "Step 14: A CTE for order totals can avoid double counting.",
            "Step 20: Use COUNT(DISTINCT order_id) for total_orders.",
        ],
    },
    "multi_channel_attribution": {
        "id": "multi_channel_attribution",
        "difficulty": "hard",
        "goal": (
            "The `sessions` table has marketing touchpoints and `conversions` has user revenue events. "
            "Build first-touch attribution: assign each user's total conversion revenue to the earliest session channel. "
            "Submit under key 'report' rows with: channel, users, total_revenue (rounded to 2dp), "
            "ordered by total_revenue DESC then channel ASC."
        ),
        "max_steps": 26,
        "hints": [
            "Step 8: Use MIN(session_ts) per user to find first touch.",
            "Step 16: Aggregate conversions per user before joining channels.",
            "Step 22: Join first_touch users with user_revenue and group by channel.",
        ],
    },
}


# ---------------------------------------------------------------------------
# Database seeders
# ---------------------------------------------------------------------------

def _seed_fix_broken_query(conn: sqlite3.Connection) -> str:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            sale_date TEXT NOT NULL
        );
        INSERT INTO sales VALUES
            (1, 'Widget A', 19.99, 5, '2024-01-10'),
            (2, 'Widget B', 34.50, 3, '2024-01-11'),
            (3, 'Widget A', 19.99, 8, '2024-01-12'),
            (4, 'Gadget X', 89.00, 2, '2024-01-13'),
            (5, 'Widget B', 34.50, 6, '2024-01-14'),
            (6, 'Gadget X', 89.00, 4, '2024-01-15'),
            (7, 'Gadget Y', 45.00, 7, '2024-01-16'),
            (8, 'Widget A', 19.99, 3, '2024-01-17'),
            (9, 'Gadget Y', 45.00, 5, '2024-01-18'),
            (10, 'Gadget X', 89.00, 1, '2024-01-19'),
            (11, 'Smart Hub', 129.00, 3, '2024-01-20'),
            (12, 'Smart Hub', 129.00, 2, '2024-01-21');
    """)
    return (
        "SELEKT product_name "
        "SUM(price * quantity) AS total_revenue "
        "FROM sales "
        "GROUP BY product_name "
        "ORDR BY total_revenue DESC "
        "LIMIT 5;"
    )


def _seed_inventory_restock_alerts(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS inventory (
            product_name TEXT PRIMARY KEY,
            stock_on_hand INTEGER NOT NULL,
            reorder_level INTEGER NOT NULL,
            lead_time_days INTEGER NOT NULL
        );
        INSERT INTO inventory VALUES
            ('Widget A', 12, 20, 7),
            ('Widget B', 5, 8, 10),
            ('Gadget X', 30, 15, 12),
            ('Gadget Y', 2, 12, 9),
            ('Smart Hub', 8, 8, 14),
            ('Power Cable', 0, 25, 5);
    """)


def _seed_find_data_anomalies(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            status TEXT
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'alice@example.com', 28, 'active'),
            (2, 'Bob', NULL, 35, 'inactive'),
            (3, 'Carol', 'carol@example.com', -5, 'active'),
            (4, 'Dave', 'dave@example.com', 42, 'pending'),
            (5, 'Eve', NULL, 31, 'active'),
            (6, 'Frank', 'frank@example.com', 29, 'BANNED'),
            (3, 'Carol_dup', 'carol2@example.com', 27, 'active'),
            (7, 'Grace', 'grace@example.com', 50, 'inactive'),
            (8, 'Hank', NULL, -12, 'pending'),
            (9, 'Ivy', 'ivy@example.com', 23, 'VIP'),
            (10, 'Jack', 'jack@example.com', 38, 'active'),
            (2, 'Bob_dup', 'bob2@example.com', 36, 'inactive');
    """)


def _seed_detect_subscription_issues(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            end_date TEXT NOT NULL,
            last_payment_days_ago INTEGER NOT NULL,
            autopay_enabled INTEGER NOT NULL
        );
        INSERT INTO subscriptions VALUES
            (1, 101, 'active',   '2024-01-15', 10, 1),
            (2, 102, 'active',   '2024-03-10', 45, 1),
            (3, 103, 'canceled', '2023-12-20', 5,  1),
            (4, 104, 'active',   '2024-01-01', 60, 0),
            (5, 105, 'trial',    '2024-02-20', 0,  0),
            (6, 106, 'canceled', '2024-01-25', 35, 0),
            (7, 102, 'active',   '2024-01-28', 31, 1),
            (8, 107, 'active',   '2024-04-01', 12, 1);
    """)


def _seed_repair_data_pipeline(conn: sqlite3.Connection) -> str:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            product TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );
        INSERT INTO orders VALUES
            (101, 1, '2024-01-10'),
            (102, 2, '2024-01-11'),
            (103, 1, '2024-01-12'),
            (104, 3, '2024-01-13'),
            (105, 2, '2024-01-14');
        INSERT INTO order_items VALUES
            (1, 101, 'Electronics', 'Phone',   2, 299.99),
            (2, 101, 'Electronics', 'Charger', 1, 19.99),
            (3, 102, 'Clothing',    'T-Shirt', 3, 15.00),
            (4, 102, 'Clothing',    'Jeans',   1, 45.00),
            (5, 103, 'Electronics', 'Tablet',  1, 199.99),
            (6, 103, 'Books',       'Python',  2, 29.99),
            (7, 104, 'Books',       'ML Book', 1, 39.99),
            (8, 104, 'Clothing',    'Jacket',  1, 89.99),
            (9, 105, 'Electronics', 'Headphones', 2, 79.99),
            (10,105, 'Books',       'Data Sci',1, 34.99);
    """)
    return (
        "SELECT oi.category, "
        "COUNT(o.order_id) AS total_orders, "
        "SUM(oi.quantity) AS total_units_sold, "
        "AVG(oi.unit_price * oi.quantity) AS avg_order_value "
        "FROM orders o "
        "JOIN order_items oi ON o.order_id = oi.order_id "
        "GROUP BY oi.category "
        "ORDER BY total_orders DESC;"
    )


def _seed_multi_channel_attribution(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            session_ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS conversions (
            conversion_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            revenue REAL NOT NULL,
            conversion_ts TEXT NOT NULL
        );
        INSERT INTO sessions VALUES
            (1, 201, 'Organic',  '2024-01-01 09:00:00'),
            (2, 201, 'Paid',     '2024-01-03 10:00:00'),
            (3, 202, 'Paid',     '2024-01-02 08:30:00'),
            (4, 203, 'Referral', '2024-01-01 14:00:00'),
            (5, 203, 'Organic',  '2024-01-04 12:00:00'),
            (6, 204, 'Email',    '2024-01-02 16:45:00'),
            (7, 205, 'Paid',     '2024-01-05 11:15:00');
        INSERT INTO conversions VALUES
            (1, 201, 120.00, '2024-01-06 13:00:00'),
            (2, 201,  80.00, '2024-01-10 09:00:00'),
            (3, 202, 220.00, '2024-01-08 15:00:00'),
            (4, 203, 150.00, '2024-01-07 10:30:00'),
            (5, 204,  60.00, '2024-01-09 17:00:00');
    """)


# ---------------------------------------------------------------------------
# Graders
# ---------------------------------------------------------------------------

def _grade_fix_broken_query(answer: Optional[Dict[str, Any]], conn: sqlite3.Connection) -> Tuple[float, str]:
    if answer is None or "rows" not in answer:
        return 0.0, "No 'rows' key in submitted answer."

    expected_rows = conn.execute(
        "SELECT product_name, ROUND(SUM(price*quantity),2) AS total_revenue "
        "FROM sales GROUP BY product_name ORDER BY total_revenue DESC LIMIT 5"
    ).fetchall()
    expected = [{"product_name": r[0], "total_revenue": r[1]} for r in expected_rows]

    submitted = answer["rows"]
    if not isinstance(submitted, list) or not submitted:
        return 0.0, "Answer 'rows' must be a non-empty list of dicts."

    n = len(expected)
    per_row_score = 1.0 / n if n > 0 else 0.0
    score = 0.0
    for i, exp in enumerate(expected):
        if i >= len(submitted):
            break
        sub = submitted[i]
        name_ok = str(sub.get("product_name", "")).strip() == exp["product_name"]
        try:
            rev_ok = abs(float(sub.get("total_revenue", -1)) - exp["total_revenue"]) < 0.1
        except (TypeError, ValueError):
            rev_ok = False
        if name_ok and rev_ok:
            score += per_row_score

    matched = round(score / per_row_score) if per_row_score > 0 else 0
    return round(min(score, 1.0), 2), f"Matched {matched}/{n} rows correctly."


def _grade_inventory_restock_alerts(answer: Optional[Dict[str, Any]], conn: sqlite3.Connection) -> Tuple[float, str]:
    if answer is None or "rows" not in answer:
        return 0.0, "No 'rows' key in submitted answer."

    expected_rows = conn.execute(
        "SELECT product_name, stock_on_hand, reorder_level, (reorder_level - stock_on_hand) AS deficit_units "
        "FROM inventory "
        "WHERE stock_on_hand <= reorder_level "
        "ORDER BY deficit_units DESC, product_name ASC"
    ).fetchall()
    expected = [dict(r) for r in expected_rows]

    submitted = answer["rows"]
    if not isinstance(submitted, list) or not submitted:
        return 0.0, "Answer 'rows' must be a non-empty list."

    n = len(expected)
    per_row_score = 1.0 / n if n else 0.0
    score = 0.0

    for i, exp in enumerate(expected):
        if i >= len(submitted):
            break
        sub = submitted[i]
        row_ok = True
        for key in ["product_name", "stock_on_hand", "reorder_level", "deficit_units"]:
            if key == "product_name":
                row_ok = row_ok and str(sub.get(key, "")).strip() == str(exp[key])
            else:
                try:
                    row_ok = row_ok and int(sub.get(key, -999999)) == int(exp[key])
                except (TypeError, ValueError):
                    row_ok = False
        if row_ok:
            score += per_row_score

    return round(min(score, 1.0), 2), f"Matched {round(score / per_row_score) if per_row_score else 0}/{n} restock rows."


def _grade_find_data_anomalies(answer: Optional[Dict[str, Any]], conn: sqlite3.Connection) -> Tuple[float, str]:
    if answer is None:
        return 0.0, "No answer submitted."

    expected = {
        "null_email_count": int(conn.execute("SELECT COUNT(*) FROM customers WHERE email IS NULL").fetchone()[0]),
        "duplicate_customer_id_count": int(
            conn.execute(
                "SELECT COUNT(*) FROM (SELECT customer_id FROM customers GROUP BY customer_id HAVING COUNT(*) > 1)"
            ).fetchone()[0]
        ),
        "negative_age_count": int(conn.execute("SELECT COUNT(*) FROM customers WHERE age < 0").fetchone()[0]),
        "invalid_status_count": int(
            conn.execute(
                "SELECT COUNT(*) FROM customers WHERE LOWER(status) NOT IN ('active','inactive','pending')"
            ).fetchone()[0]
        ),
    }
    score = 0.0
    feedback = []
    for key, exp_val in expected.items():
        sub_val = answer.get(key)
        try:
            if int(sub_val) == exp_val:
                score += 0.25
                feedback.append(f"{key}: correct ({exp_val})")
            else:
                feedback.append(f"{key}: wrong (got {sub_val}, expected {exp_val})")
        except (TypeError, ValueError):
            feedback.append(f"{key}: invalid value '{sub_val}'")

    return round(score, 2), " | ".join(feedback)


def _grade_detect_subscription_issues(answer: Optional[Dict[str, Any]], conn: sqlite3.Connection) -> Tuple[float, str]:
    if answer is None:
        return 0.0, "No answer submitted."

    expected = {
        "expired_active_count": int(
            conn.execute(
                "SELECT COUNT(*) FROM subscriptions WHERE status='active' AND end_date < '2024-02-01'"
            ).fetchone()[0]
        ),
        "delinquent_count": int(
            conn.execute("SELECT COUNT(*) FROM subscriptions WHERE last_payment_days_ago > 30").fetchone()[0]
        ),
        "autopay_mismatch_count": int(
            conn.execute("SELECT COUNT(*) FROM subscriptions WHERE status='canceled' AND autopay_enabled=1").fetchone()[0]
        ),
        "duplicate_user_count": int(
            conn.execute(
                "SELECT COUNT(*) FROM (SELECT user_id FROM subscriptions GROUP BY user_id HAVING COUNT(*) > 1)"
            ).fetchone()[0]
        ),
    }
    score = 0.0
    feedback = []
    for key, exp_val in expected.items():
        sub_val = answer.get(key)
        try:
            if int(sub_val) == exp_val:
                score += 0.25
                feedback.append(f"{key}: correct ({exp_val})")
            else:
                feedback.append(f"{key}: wrong (got {sub_val}, expected {exp_val})")
        except (TypeError, ValueError):
            feedback.append(f"{key}: invalid value '{sub_val}'")

    return round(score, 2), " | ".join(feedback)


def _grade_repair_data_pipeline(answer: Optional[Dict[str, Any]], conn: sqlite3.Connection) -> Tuple[float, str]:
    if answer is None or "report" not in answer:
        return 0.0, "No 'report' key in submitted answer."

    expected_sql = """
        WITH order_totals AS (
            SELECT order_id, SUM(quantity * unit_price) AS order_total
            FROM order_items GROUP BY order_id
        )
        SELECT oi.category,
               COUNT(DISTINCT o.order_id) AS total_orders,
               SUM(oi.quantity) AS total_units_sold,
               ROUND(AVG(ot.order_total), 2) AS avg_order_value
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN order_totals ot ON o.order_id = ot.order_id
        GROUP BY oi.category
        ORDER BY total_orders DESC
    """
    expected_rows = conn.execute(expected_sql).fetchall()
    expected = [
        {
            "category": r[0],
            "total_orders": r[1],
            "total_units_sold": r[2],
            "avg_order_value": round(r[3], 2),
        }
        for r in expected_rows
    ]

    submitted = answer.get("report", [])
    if not isinstance(submitted, list) or len(submitted) == 0:
        return 0.0, "Submitted 'report' must be a non-empty list."

    sub_map = {str(r.get("category", "")): r for r in submitted}
    score = 0.0
    per_cat_score = 1.0 / len(expected)
    feedback = []
    for exp in expected:
        cat = exp["category"]
        sub = sub_map.get(cat)
        if sub is None:
            feedback.append(f"{cat}: missing")
            continue

        ok_orders = int(sub.get("total_orders", -1)) == exp["total_orders"]
        ok_units = int(sub.get("total_units_sold", -1)) == exp["total_units_sold"]
        try:
            ok_avg = abs(round(float(sub.get("avg_order_value", -1)), 2) - exp["avg_order_value"]) < 0.05
        except (TypeError, ValueError):
            ok_avg = False

        score += per_cat_score * (sum([ok_orders, ok_units, ok_avg]) / 3.0)
        feedback.append(f"{cat}: orders={ok_orders} units={ok_units} avg={ok_avg}")

    return round(min(score, 1.0), 2), " | ".join(feedback)


def _grade_multi_channel_attribution(answer: Optional[Dict[str, Any]], conn: sqlite3.Connection) -> Tuple[float, str]:
    if answer is None or "report" not in answer:
        return 0.0, "No 'report' key in submitted answer."

    expected_sql = """
        WITH first_touch AS (
            SELECT s.user_id, s.channel
            FROM sessions s
            JOIN (
                SELECT user_id, MIN(session_ts) AS first_ts
                FROM sessions
                GROUP BY user_id
            ) m ON s.user_id = m.user_id AND s.session_ts = m.first_ts
        ),
        user_revenue AS (
            SELECT user_id, ROUND(SUM(revenue), 2) AS total_revenue
            FROM conversions
            GROUP BY user_id
        )
        SELECT ft.channel,
               COUNT(ft.user_id) AS users,
               ROUND(SUM(COALESCE(ur.total_revenue, 0)), 2) AS total_revenue
        FROM first_touch ft
        LEFT JOIN user_revenue ur ON ft.user_id = ur.user_id
        GROUP BY ft.channel
        ORDER BY total_revenue DESC, ft.channel ASC
    """
    expected_rows = conn.execute(expected_sql).fetchall()
    expected = [dict(r) for r in expected_rows]

    submitted = answer.get("report", [])
    if not isinstance(submitted, list) or not submitted:
        return 0.0, "Submitted 'report' must be a non-empty list."

    sub_map = {str(r.get("channel", "")): r for r in submitted}
    per_row_score = 1.0 / len(expected)
    score = 0.0

    for exp in expected:
        channel = exp["channel"]
        sub = sub_map.get(channel)
        if sub is None:
            continue
        try:
            users_ok = int(sub.get("users", -1)) == int(exp["users"])
            revenue_ok = abs(float(sub.get("total_revenue", -1)) - float(exp["total_revenue"])) < 0.05
        except (TypeError, ValueError):
            users_ok = False
            revenue_ok = False
        score += per_row_score * (sum([users_ok, revenue_ok]) / 2.0)

    return round(min(score, 1.0), 2), "Attribution rows graded with per-channel partial credit."


# ---------------------------------------------------------------------------
# Main Environment Class
# ---------------------------------------------------------------------------

class SQLDataAnalystEnv:
    """
    OpenEnv-compatible SQL Data Analyst environment.
    Each episode is backed by a fresh in-memory SQLite database.
    """

    TASK_ORDER = [
        "fix_broken_query",
        "inventory_restock_alerts",
        "find_data_anomalies",
        "detect_subscription_issues",
        "repair_data_pipeline",
        "multi_channel_attribution",
    ]

    def __init__(self) -> None:
        self._conn: Optional[sqlite3.Connection] = None
        self._state: Optional[SQLState] = None
        self._task_cfg: Optional[Dict[str, Any]] = None
        self._task_context: Dict[str, Any] = {}
        self._obs_cache: Optional[SQLObservation] = None
        self._task_index: int = 0

    def reset(self, task_id: Optional[str] = None) -> Tuple[SQLObservation, Dict[str, Any]]:
        if task_id and task_id in TASKS:
            chosen_task_id = task_id
        else:
            chosen_task_id = self.TASK_ORDER[self._task_index % len(self.TASK_ORDER)]
            self._task_index += 1

        self._task_cfg = TASKS[chosen_task_id]
        self._task_context = {}

        if self._conn:
            self._conn.close()
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row

        if chosen_task_id == "fix_broken_query":
            self._task_context["broken_query"] = _seed_fix_broken_query(self._conn)
        elif chosen_task_id == "inventory_restock_alerts":
            _seed_inventory_restock_alerts(self._conn)
        elif chosen_task_id == "find_data_anomalies":
            _seed_find_data_anomalies(self._conn)
        elif chosen_task_id == "detect_subscription_issues":
            _seed_detect_subscription_issues(self._conn)
        elif chosen_task_id == "repair_data_pipeline":
            self._task_context["broken_query"] = _seed_repair_data_pipeline(self._conn)
        elif chosen_task_id == "multi_channel_attribution":
            _seed_multi_channel_attribution(self._conn)

        self._conn.commit()

        self._state = SQLState(
            episode_id=str(uuid.uuid4()),
            task_id=chosen_task_id,
            step_count=0,
            current_score=0.0,
            max_steps=self._task_cfg["max_steps"],
            done=False,
        )

        obs = self._build_observation()
        self._obs_cache = obs
        info = {"episode_id": self._state.episode_id, "task_id": chosen_task_id}
        return obs, info

    def step(self, action: SQLAction) -> Tuple[SQLObservation, float, bool, Dict[str, Any]]:
        if self._state is None or self._conn is None:
            raise RuntimeError("Call reset() before step().")

        if self._state.done:
            obs = self._build_observation(last_action_error="Episode is already done. Call reset().")
            return obs, 0.0, True, {}

        self._state.step_count += 1
        reward = 0.0
        last_result: Optional[List[Dict]] = None
        last_error: Optional[str] = None
        action_error: Optional[str] = None
        info: Dict[str, Any] = {}

        if action.action_type == "list_tables":
            rows = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            last_result = [{"table_name": r["name"]} for r in rows]
            reward = 0.01

        elif action.action_type == "describe_table":
            if not action.sql_query:
                action_error = "describe_table requires sql_query to contain the table name."
            else:
                table_name = action.sql_query.strip().strip(";")
                exists = self._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
                ).fetchone()
                if not exists:
                    action_error = f"Table '{table_name}' does not exist."
                else:
                    cols = self._conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                    schema_rows = [{"cid": c["cid"], "name": c["name"], "type": c["type"]} for c in cols]
                    sample = self._conn.execute(f"SELECT * FROM '{table_name}' LIMIT 5").fetchall()
                    sample_rows = [dict(r) for r in sample]
                    last_result = [{"schema": schema_rows, "sample": sample_rows}]
                    reward = 0.02

        elif action.action_type == "execute_query":
            if not action.sql_query:
                action_error = "execute_query requires a sql_query."
            else:
                try:
                    cur = self._conn.execute(action.sql_query)
                    rows = cur.fetchall()
                    last_result = [dict(r) for r in rows]
                    reward = 0.05
                except sqlite3.Error as e:
                    last_error = str(e)
                    reward = -0.02

        elif action.action_type == "submit_answer":
            if action.answer is None:
                action_error = "submit_answer requires an 'answer' dict."
            else:
                score, feedback = self._grade(action.answer)
                self._state.current_score = score
                reward = score
                self._state.done = True
                info["grade_feedback"] = feedback
                info["final_score"] = score
                info["grader_version"] = GRADER_VERSION
                info["grader_deterministic"] = True
                info["task_difficulty"] = self._task_cfg["difficulty"]

        elif action.action_type == "noop":
            reward = -0.01

        else:
            action_error = f"Unknown action_type: {action.action_type}"

        if self._state.step_count >= self._state.max_steps and not self._state.done:
            self._state.done = True
            info["termination"] = "max_steps_reached"
            if "final_score" not in info:
                info["final_score"] = self._state.current_score

        obs = self._build_observation(last_result=last_result, last_error=last_error, last_action_error=action_error)
        self._obs_cache = obs
        return obs, reward, self._state.done, info

    def state(self) -> SQLState:
        if self._state is None:
            raise RuntimeError("Call reset() before state().")
        return self._state

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _build_observation(
        self,
        last_result: Optional[List[Dict]] = None,
        last_error: Optional[str] = None,
        last_action_error: Optional[str] = None,
    ) -> SQLObservation:
        assert self._state is not None and self._task_cfg is not None and self._conn is not None

        tables = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        schema_parts = []
        for t in tables:
            tname = t["name"]
            cols = self._conn.execute(f"PRAGMA table_info('{tname}')").fetchall()
            col_strs = ", ".join(f"{c['name']} {c['type']}" for c in cols)
            schema_parts.append(f"TABLE {tname} ({col_strs})")
        schema_info = "\n".join(schema_parts)

        if "broken_query" in self._task_context:
            schema_info += f"\n\nBROKEN QUERY (fix this):\n{self._task_context['broken_query']}"

        primary_table = tables[0]["name"] if tables else None
        data_sample: List[Dict] = []
        if primary_table:
            rows = self._conn.execute(f"SELECT * FROM '{primary_table}' LIMIT 5").fetchall()
            data_sample = [dict(r) for r in rows]

        hints = None
        step_hints = self._task_cfg.get("hints", [])
        unlocked: List[str] = []
        for hint in step_hints:
            match = re.search(r"Step (\d+):", hint)
            if not match:
                continue
            threshold = int(match.group(1))
            if self._state.step_count >= threshold:
                unlocked.append(hint[hint.index(":") + 2 :])
        if unlocked:
            hints = unlocked

        return SQLObservation(
            task_id=self._state.task_id,
            goal=self._task_cfg["goal"],
            schema_info=schema_info,
            data_sample=data_sample,
            last_query_result=last_result,
            last_query_error=last_error,
            last_action_error=last_action_error,
            step_count=self._state.step_count,
            max_steps=self._state.max_steps,
            hints=hints,
        )

    def _grade(self, answer: Dict[str, Any]) -> Tuple[float, str]:
        assert self._state is not None and self._conn is not None
        task_id = self._state.task_id
        if task_id == "fix_broken_query":
            return _grade_fix_broken_query(answer, self._conn)
        if task_id == "inventory_restock_alerts":
            return _grade_inventory_restock_alerts(answer, self._conn)
        if task_id == "find_data_anomalies":
            return _grade_find_data_anomalies(answer, self._conn)
        if task_id == "detect_subscription_issues":
            return _grade_detect_subscription_issues(answer, self._conn)
        if task_id == "repair_data_pipeline":
            return _grade_repair_data_pipeline(answer, self._conn)
        if task_id == "multi_channel_attribution":
            return _grade_multi_channel_attribution(answer, self._conn)
        return 0.0, f"Unknown task: {task_id}"
