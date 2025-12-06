import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd

MAX_AMBIGUOUS_COLUMN_ATTEMPTS = 5


def read_sql_queries(base_dir: Path, query: str, *, replace_create_view: bool = False):
    path = base_dir / f"{query}.sql"
    text = path.read_text()
    for sql in text.split(";"):
        sql = strip_limit(sql.strip())  # noqa: PLW2901
        if replace_create_view:
            sql = sql.replace("create view", "create temp view")  # noqa: PLW2901
        if sql:
            yield sql


def is_ddl(sql: str) -> bool:
    return any(x in sql for x in ("create view", "create temp view", "drop view"))


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.size == 0:
        return df

    normalized = df.copy()
    normalized.columns = [f"col_{i}" for i in range(len(normalized.columns))]

    for column in normalized.columns:
        normalized[column] = normalized[column].map(_normalize_scalar)

    normalized = normalized.convert_dtypes()
    return normalized.sort_values(by=normalized.columns.tolist()).reset_index(drop=True)


def _normalize_scalar(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if value == "":
        return None
    return value


def run_duck_query(duck, sql: str, *, max_attempts: int = MAX_AMBIGUOUS_COLUMN_ATTEMPTS) -> pd.DataFrame:
    duck_sql = prepare_duck_sql(sql)
    attempts = 0
    while True:
        try:
            return duck.sql(duck_sql).fetch_arrow_table().to_pandas()
        except duckdb.BinderException as e:
            if not (parsed := _parse_ambiguous_column_error(str(e))):
                raise
            column, qualified = parsed
            duck_sql = _qualify_column(duck_sql, column, qualified)
            attempts += 1
            if attempts > max_attempts:
                raise


def prepare_duck_sql(sql: str) -> str:
    """DuckDB does not accept Spark-style backtick identifiers or some aliases."""
    return (
        sql.replace("`", '"')
        .replace(" returns,", ' "returns",')
        .replace(" returns ", ' "returns" ')
        .replace(" at,", ' "at",')
        .replace(" at ", ' "at" ')
    )


def _parse_ambiguous_column_error(message: str):
    match = re.search(r'Ambiguous reference to column name "([^"]+)" \(use: ([^)]+)\)', message)
    if not match:
        return None
    column = match.group(1)
    options = match.group(2).replace('"', "").split(" or ")
    if not options:
        return None
    return column, options[0]


def _qualify_column(sql: str, column: str, qualified: str) -> str:
    # Replace unqualified occurrences of the column while avoiding alias definitions.
    pattern = re.compile(rf"\b{re.escape(column)}\b")
    keywords = {
        "select",
        "where",
        "and",
        "or",
        "on",
        "by",
        "order",
        "group",
        "having",
        "distinct",
        "union",
        "with",
        "when",
        "then",
        "case",
        "over",
        "partition",
        "from",
        "join",
        "left",
        "right",
        "full",
        "inner",
        "cross",
        "rollup",
        "cube",
        "values",
        "limit",
    }

    def repl(match: re.Match) -> str:
        s = match.string
        i = match.start() - 1
        # Skip if previous non-space character is an identifier/quote, which likely
        # indicates an alias definition like "i_item_id item_id".
        while i >= 0 and s[i].isspace():
            i -= 1
        if i >= 0:
            if s[i] == ".":
                return match.group(0)
            if s[i].isalnum() or s[i] in '_"':
                prev_token_match = re.search(r"(\w+)\W*$", s[: match.start()])
                prev_token = prev_token_match.group(1).lower() if prev_token_match else ""
                if prev_token not in keywords:
                    return match.group(0)
        return qualified

    return pattern.sub(repl, sql)


def strip_limit(sql: str) -> str:
    """Remove a trailing LIMIT clause to compare full result sets across engines."""
    return re.sub(r"(?is)\blimit\s+\d+\s*;?\s*$", "", sql)
