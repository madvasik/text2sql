import os
import sqlite3
from pathlib import Path
from typing import List, Tuple, Any, Optional, Dict
import random
from datetime import datetime, timedelta


DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "example.db"


def ensure_database_exists() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        return
    conn = sqlite3.connect(DB_PATH.as_posix())
    try:
        cur = conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                location TEXT NOT NULL,
                budget INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT NOT NULL,
                phone TEXT,
                title TEXT NOT NULL,
                department_id INTEGER NOT NULL,
                manager_id INTEGER,
                hire_date TEXT NOT NULL,
                date_of_birth TEXT NOT NULL,
                salary_band TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (department_id) REFERENCES departments(id),
                FOREIGN KEY (manager_id) REFERENCES employees(id)
            );

            CREATE TABLE salaries (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                currency TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                FOREIGN KEY (employee_id) REFERENCES employees(id)
            );

            CREATE TABLE projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department_id INTEGER NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                budget INTEGER NOT NULL,
                FOREIGN KEY (department_id) REFERENCES departments(id)
            );

            CREATE TABLE employee_projects (
                employee_id INTEGER NOT NULL,
                project_id INTEGER NOT NULL,
                allocation_percent INTEGER NOT NULL,
                PRIMARY KEY (employee_id, project_id),
                FOREIGN KEY (employee_id) REFERENCES employees(id),
                FOREIGN KEY (project_id) REFERENCES projects(id)
            );

            CREATE TABLE performance_reviews (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER NOT NULL,
                review_date TEXT NOT NULL,
                score REAL NOT NULL,
                reviewer TEXT NOT NULL,
                comments TEXT,
                FOREIGN KEY (employee_id) REFERENCES employees(id)
            );
            """
        )

        # Seed data
        random.seed(42)
        now = datetime.utcnow()

        departments = [
            (1, "Engineering", "Prague", 2_000_000, (now - timedelta(days=1500)).date().isoformat()),
            (2, "Sales", "Berlin", 1_200_000, (now - timedelta(days=1400)).date().isoformat()),
            (3, "HR", "Warsaw", 400_000, (now - timedelta(days=1300)).date().isoformat()),
            (4, "Marketing", "Paris", 800_000, (now - timedelta(days=1200)).date().isoformat()),
            (5, "Finance", "London", 1_500_000, (now - timedelta(days=1100)).date().isoformat()),
            (6, "Support", "Dublin", 600_000, (now - timedelta(days=1000)).date().isoformat()),
        ]
        cur.executemany(
            "INSERT INTO departments (id, name, location, budget, created_at) VALUES (?, ?, ?, ?, ?)",
            departments,
        )

        first_names = [
            "Alice","Bob","Carol","David","Eve","Frank","Grace","Heidi","Ivan","Judy","Mallory","Niaj","Olivia","Peggy","Rupert","Sybil","Trent","Victor","Wendy","Yvonne","Zack"
        ]
        last_names = [
            "Johnson","Smith","Nguyen","Lee","Brown","Williams","Jones","Garcia","Miller","Davis","Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore"
        ]
        titles = [
            "Engineer","Senior Engineer","Lead Engineer","Manager","Director","Sales Rep","Sales Manager","HR Specialist","HR Manager","Marketing Specialist","Analyst","Finance Manager","Support Agent","Support Lead"
        ]
        bands = ["A","B","C","D","E"]
        statuses = ["active","on_leave","terminated"]

        employees: List[Tuple[Any, ...]] = []
        employee_count = 250
        start_emp_id = 1
        for i in range(employee_count):
            emp_id = start_emp_id + i
            fn = random.choice(first_names)
            ln = random.choice(last_names)
            email = f"{fn.lower()}.{ln.lower()}_{emp_id}@example.com"
            phone = f"+1-555-{random.randint(100,999):03d}-{random.randint(1000,9999):04d}"
            title = random.choice(titles)
            dept_id = random.randint(1, len(departments))
            hire_date = (now - timedelta(days=random.randint(30, 2000))).date().isoformat()
            dob = (now - timedelta(days=random.randint(9000, 20000))).date().isoformat()
            band = random.choice(bands)
            status = random.choices(statuses, weights=[85,10,5])[0]
            manager_id = None
            if i > 10 and random.random() < 0.7:
                manager_id = random.randint(1, emp_id - 1)
            employees.append((emp_id, fn, ln, email, phone, title, dept_id, manager_id, hire_date, dob, band, status))

        cur.executemany(
            """
            INSERT INTO employees (
                id, first_name, last_name, email, phone, title,
                department_id, manager_id, hire_date, date_of_birth,
                salary_band, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            employees,
        )

        salaries_rows: List[Tuple[Any, ...]] = []
        salary_id = 1
        for (emp_id, *_rest) in employees:
            base = {
                "A": random.randint(50_000, 70_000),
                "B": random.randint(70_001, 90_000),
                "C": random.randint(90_001, 120_000),
                "D": random.randint(120_001, 160_000),
                "E": random.randint(160_001, 220_000),
            }
            band = _rest[-2]
            effective_date = (now - timedelta(days=random.randint(0, 365))).date().isoformat()
            salaries_rows.append((salary_id, emp_id, base[band], "USD", effective_date))
            salary_id += 1
        cur.executemany(
            "INSERT INTO salaries (id, employee_id, amount, currency, effective_date) VALUES (?, ?, ?, ?, ?)",
            salaries_rows,
        )

        projects: List[Tuple[Any, ...]] = []
        project_count = 60
        for pid in range(1, project_count + 1):
            name = f"Project-{pid:03d}"
            dept_id = random.randint(1, len(departments))
            start_date = (now - timedelta(days=random.randint(30, 1500))).date().isoformat()
            if random.random() < 0.7:
                end_date = (now - timedelta(days=random.randint(0, 29))).date().isoformat()
            else:
                end_date = None
            budget = random.randint(50_000, 1_000_000)
            projects.append((pid, name, dept_id, start_date, end_date, budget))
        cur.executemany(
            "INSERT INTO projects (id, name, department_id, start_date, end_date, budget) VALUES (?, ?, ?, ?, ?, ?)",
            projects,
        )

        emp_proj_rows: List[Tuple[int, int, int]] = []
        for (emp_id, *_r) in employees:
            assigned = random.sample(range(1, project_count + 1), k=random.randint(0, 3))
            for proj_id in assigned:
                emp_proj_rows.append((emp_id, proj_id, random.choice([20, 30, 50, 75, 100])))
        if emp_proj_rows:
            cur.executemany(
                "INSERT INTO employee_projects (employee_id, project_id, allocation_percent) VALUES (?, ?, ?)",
                emp_proj_rows,
            )

        reviews: List[Tuple[Any, ...]] = []
        review_id = 1
        for (emp_id, *_r) in employees:
            for _ in range(random.randint(0, 3)):
                review_date = (now - timedelta(days=random.randint(30, 1000))).date().isoformat()
                score = round(random.uniform(2.5, 5.0), 2)
                reviewer = random.choice(["Manager", "Director", "Peer"])
                comments = random.choice([
                    "Solid performance", "Exceeds expectations", "Needs improvement", "Outstanding contribution"
                ])
                reviews.append((review_id, emp_id, review_date, score, reviewer, comments))
                review_id += 1
        if reviews:
            cur.executemany(
                "INSERT INTO performance_reviews (id, employee_id, review_date, score, reviewer, comments) VALUES (?, ?, ?, ?, ?, ?)",
                reviews,
            )

        conn.commit()
    finally:
        conn.close()


def get_connection(readonly: bool = True) -> sqlite3.Connection:
    ensure_database_exists()
    if readonly:
        # Enforce read-only mode to prevent writes from generated SQL
        uri = f"file:{DB_PATH.as_posix()}?mode=ro"
        return sqlite3.connect(uri, uri=True, timeout=5)
    return sqlite3.connect(DB_PATH.as_posix(), timeout=5)


def list_tables_and_schema() -> str:
    conn = get_connection(readonly=True)
    try:
        cur = conn.cursor()
        tables = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        ).fetchall()
        lines: List[str] = []
        # Human-readable descriptions for tables/columns to help LLM
        descriptions: Dict[str, Dict[str, str]] = {
            "departments": {
                "id": "Unique department identifier",
                "name": "Department name",
                "location": "Office location/city",
                "budget": "Annual department budget in USD",
                "created_at": "Department creation date (ISO)",
            },
            "employees": {
                "id": "Unique employee identifier",
                "first_name": "Employee given name",
                "last_name": "Employee family name",
                "email": "Work email",
                "phone": "Contact phone",
                "title": "Job title",
                "department_id": "FK to departments.id",
                "manager_id": "FK to employees.id (manager)",
                "hire_date": "Hire date (ISO)",
                "date_of_birth": "Birth date (ISO)",
                "salary_band": "Compensation band (A..E)",
                "status": "Employment status (active/on_leave/terminated)",
            },
            "salaries": {
                "id": "Unique salary record identifier",
                "employee_id": "FK to employees.id",
                "amount": "Salary amount for the record",
                "currency": "Currency code (e.g., USD)",
                "effective_date": "Effective date (ISO)",
            },
            "projects": {
                "id": "Unique project identifier",
                "name": "Project name",
                "department_id": "FK to departments.id",
                "start_date": "Project start date (ISO)",
                "end_date": "Project end date (ISO, nullable)",
                "budget": "Project budget in USD",
            },
            "employee_projects": {
                "employee_id": "FK to employees.id",
                "project_id": "FK to projects.id",
                "allocation_percent": "Allocation percent of employee to project",
            },
            "performance_reviews": {
                "id": "Unique performance review identifier",
                "employee_id": "FK to employees.id",
                "review_date": "Review date (ISO)",
                "score": "Performance score (2.5-5.0)",
                "reviewer": "Reviewer role",
                "comments": "Short comments",
            },
        }
        for (table_name,) in tables:
            lines.append(f"TABLE {table_name}")
            cols = cur.execute(f"PRAGMA table_info({table_name});").fetchall()
            for col in cols:
                # pragma: cid, name, type, notnull, dflt_value, pk
                cname = col[1]
                cdesc = descriptions.get(table_name, {}).get(cname, "")
                desc_suffix = f" — {cdesc}" if cdesc else ""
                lines.append(
                    f"  - {cname} {col[2]}{' NOT NULL' if col[3] else ''}{' PRIMARY KEY' if col[5] else ''}{desc_suffix}"
                )
        return "\n".join(lines)
    finally:
        conn.close()


def execute_readonly(sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    if not sql.strip().lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    if ";" in sql.strip().rstrip(";"):
        raise ValueError("Only a single SELECT statement is allowed.")

    conn = get_connection(readonly=True)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description] if cur.description else []
        return headers, rows
    finally:
        conn.close()

