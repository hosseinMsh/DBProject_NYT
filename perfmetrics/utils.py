import time
from typing import Sequence, Any, Dict
from django.db import connection
from django.utils.module_loading import import_string
from perfmetrics.models import QueryHit

def _fetch_all_dict(cur) -> list[dict]:
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall() if cur.description else []
    return [dict(zip(cols, r)) for r in rows]

def run_sql_logged_return_data(sql: str, label: str, view_name: str, optimized: bool = False, params: Sequence[Any] | None = None):
    """
    Execute SQL and return ONLY data (for V1 compatibility), but log metrics in DB.
    """
    t0 = time.perf_counter()
    with connection.cursor() as cur:
        cur.execute(sql, params or [])
        data = _fetch_all_dict(cur)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    QueryHit.objects.create(
        label=label,
        view_name=view_name,
        sql_text=sql,
        elapsed_ms=elapsed_ms,
        rows=len(data),
        optimized=optimized,
    )
    return data

def run_sql_logged_return_timed(sql: str, label: str, view_name: str, optimized: bool = False, params: Sequence[Any] | None = None) -> Dict[str, Any]:
    """
    Execute SQL and return timed structure (for V2 compatibility), and log metrics in DB.
    """
    t0 = time.perf_counter()
    with connection.cursor() as cur:
        cur.execute(sql, params or [])
        data = _fetch_all_dict(cur)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    QueryHit.objects.create(
        label=label,
        view_name=view_name,
        sql_text=sql,
        elapsed_ms=elapsed_ms,
        rows=len(data),
        optimized=optimized,
    )
    return {"elapsed_ms": round(elapsed_ms, 2), "rows": len(data), "data": data}
