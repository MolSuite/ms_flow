from __future__ import annotations

import re
from typing import Any, Mapping, Sequence


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\\.]*$")


def normalize_identifier(value: str, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{label} must not be empty.")
    if not IDENTIFIER_RE.match(normalized):
        raise ValueError(f"Invalid {label} '{value}'.")
    return normalized


def normalize_fields(fields: Sequence[str] | None) -> tuple[str, ...]:
    return tuple(normalize_identifier(field, label="field") for field in (fields or ()))


def normalize_order(order: Sequence[str] | None) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_item in order or ():
        item = str(raw_item or "").strip()
        if not item:
            continue
        desc = item.startswith("-")
        field = item[1:] if desc else item
        normalized_field = normalize_identifier(field, label="order field")
        normalized.append(f"-{normalized_field}" if desc else normalized_field)
    return tuple(normalized)


def compile_field(value: str) -> str:
    """A column name, or a declarative JSON path `column->key->key`.

    JSON columns (metrics, extra_data, ...) can be filtered without writing SQL:
    `{"metrics->protocol->hash": h}` compiles to `json_extract(metrics, '$.protocol.hash') = ?`.
    """
    raw = str(value or "")
    if "->" not in raw:
        return normalize_identifier(raw, label="filter field")
    parts = [normalize_identifier(part, label="json field") for part in raw.split("->")]
    return f"json_extract({parts[0]}, '$.{'.'.join(parts[1:])}')"


def compile_subquery(value: Any) -> tuple[str, tuple[Any, ...]]:
    """Compile the value of an `__in_subquery` / `__not_in_subquery` filter.

    The value is another declarative specification (QuerySpec or equivalent): any
    object with `.compile() -> (sql, params)`. It must project a single column for
    `IN (...)` to be valid.
    """
    compile_fn = getattr(value, "compile", None)
    if not callable(compile_fn):
        raise ValueError("Subquery filters require a spec object with .compile().")
    fields = getattr(value, "fields", None)
    if fields is not None and len(fields) != 1:
        raise ValueError("Subquery filters require a spec projecting exactly one field.")
    sub_sql, sub_params = compile_fn()
    return str(sub_sql), tuple(sub_params)


def compile_subquery_sql(value: Any, *, null_safe: bool = False) -> tuple[str, tuple[Any, ...]]:
    """Same as `compile_subquery`, but wraps the derived table for `NOT IN`.

    `NOT IN` returns zero rows if the subquery yields a NULL. The wrapper drops NULLs
    without correlating, so SQLite still materialises the derived table only once.
    """
    sub_sql, sub_params = compile_subquery(value)
    if not null_safe:
        return sub_sql, sub_params
    fields = getattr(value, "fields", None)
    if not fields:
        raise ValueError("A null-safe subquery requires a spec declaring its field.")
    col = normalize_identifier(str(fields[0]).split(".")[-1], label="subquery field")
    wrapped = f"SELECT __sq.{col} FROM ({sub_sql}) AS __sq WHERE __sq.{col} IS NOT NULL"
    return wrapped, sub_params


def compile_filters(filters: Mapping[str, Any] | None) -> tuple[list[str], list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    for raw_key, raw_value in (filters or {}).items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if "__" in key:
            field_name, op = key.split("__", 1)
        else:
            field_name, op = key, "eq"
        field = compile_field(field_name)
        operator = op.strip().lower()
        value = raw_value

        if operator == "eq":
            conditions.append(f"{field} = ?")
            params.append(value)
        elif operator == "ne":
            conditions.append(f"{field} != ?")
            params.append(value)
        elif operator == "gt":
            conditions.append(f"{field} > ?")
            params.append(value)
        elif operator == "gte":
            conditions.append(f"{field} >= ?")
            params.append(value)
        elif operator == "lt":
            conditions.append(f"{field} < ?")
            params.append(value)
        elif operator == "lte":
            conditions.append(f"{field} <= ?")
            params.append(value)
        elif operator == "contains":
            conditions.append(f"{field} LIKE ?")
            params.append(f"%{value}%")
        elif operator == "startswith":
            conditions.append(f"{field} LIKE ?")
            params.append(f"{value}%")
        elif operator == "endswith":
            conditions.append(f"{field} LIKE ?")
            params.append(f"%{value}")
        elif operator == "in":
            values = [] if value is None else list(value) if isinstance(value, (list, tuple, set)) else [value]
            if not values:
                conditions.append("1 = 0")
            else:
                placeholders = ", ".join("?" for _ in values)
                conditions.append(f"{field} IN ({placeholders})")
                params.extend(values)
        elif operator == "not_in":
            values = [] if value is None else list(value) if isinstance(value, (list, tuple, set)) else [value]
            if not values:
                continue
            placeholders = ", ".join("?" for _ in values)
            conditions.append(f"{field} NOT IN ({placeholders})")
            params.extend(values)
        elif operator in ("in_subquery", "not_in_subquery"):
            # Several subqueries on the same column are ANDed: the filter dict key is
            # "field__op", so they could not coexist any other way.
            specs = value if isinstance(value, (list, tuple)) else [value]
            for spec in specs:
                sub_sql, sub_params = compile_subquery_sql(
                    spec, null_safe=operator == "not_in_subquery"
                )
                keyword = "IN" if operator == "in_subquery" else "NOT IN"
                conditions.append(f"{field} {keyword} ({sub_sql})")
                params.extend(sub_params)
        elif operator == "is_null":
            flag = True if value is None else bool(value)
            conditions.append(f"{field} IS NULL" if flag else f"{field} IS NOT NULL")
        elif operator == "is_not_null":
            flag = True if value is None else bool(value)
            conditions.append(f"{field} IS NOT NULL" if flag else f"{field} IS NULL")
        else:
            raise ValueError(f"Unsupported filter operator '{operator}' in '{key}'.")

    return conditions, params


def compile_select(
    *,
    table: str = "",
    fields: Sequence[str] = (),
    filters: Mapping[str, Any] | None = None,
    order: Sequence[str] = (),
    limit: int | None = None,
    offset: int = 0,
    query: str = "",
    params: Sequence[Any] = (),
) -> tuple[str, tuple[Any, ...]]:
    """The project's only SELECT compiler. A raw `query` takes precedence."""
    if query:
        return query, tuple(params)

    select_fields = ", ".join(fields) if fields else "*"
    sql = f"SELECT {select_fields} FROM {table}"
    where_parts, bound_params = compile_filters(filters)

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    if order:
        order_parts = []
        for item in order:
            desc = item.startswith("-")
            field = item[1:] if desc else item
            order_parts.append(f"{field} DESC" if desc else f"{field} ASC")
        sql += " ORDER BY " + ", ".join(order_parts)

    if offset and offset > 0 and limit is None:
        # SQLite rejects a bare OFFSET: without LIMIT it has to be written as "all the rest".
        sql += " LIMIT -1"
    if limit is not None:
        sql += " LIMIT ?"
        bound_params.append(int(limit))
    if offset and offset > 0:
        sql += " OFFSET ?"
        bound_params.append(int(offset))
    return sql, tuple(bound_params)


def compile_count(**kwargs: Any) -> tuple[str, tuple[Any, ...]]:
    """Count the rows `compile_select(**kwargs)` would return."""
    inner_sql, params = compile_select(**kwargs)
    return f"SELECT COUNT(*) FROM ({inner_sql})", params
