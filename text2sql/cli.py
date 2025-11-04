import os
import sys
from typing import List

from dotenv import load_dotenv
from tabulate import tabulate

from .db import ensure_database_exists, execute_readonly
from .llm import generate_sql_from_nl


def main(argv: List[str]) -> int:
    load_dotenv()  # Load MISTRAL_API_KEY if provided in .env
    ensure_database_exists()

    if len(argv) < 1:
        print("Usage: python -m text2sql \"your question in natural language\"")
        return 2

    question = " ".join(argv)

    try:
        sql = generate_sql_from_nl(question)
    except Exception as e:
        print(f"Failed to generate SQL: {e}")
        return 1

    print("Generated SQL:\n" + sql)

    try:
        headers, rows = execute_readonly(sql)
    except Exception as e:
        print(f"Failed to execute SQL: {e}")
        return 1

    if not rows:
        print("No rows.")
        return 0

    print()
    print(tabulate(rows, headers=headers, tablefmt="github"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

