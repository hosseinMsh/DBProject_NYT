
import time
from django.http import JsonResponse
from django.db import connection

def run_sql_timed(sql: str, params=None):
    t0 = time.monotonic()
    with connection.cursor() as cur:
        cur.execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    data = [dict(zip(cols, r)) for r in rows]
    return {"elapsed_ms": round(elapsed_ms, 2), "rows": len(data), "data": data}

def tbl(name: str, optimized: bool):
    if optimized:
        if name == "trip": return "trip_clean"
        if name == "location": return "core_location"
    else:
        if name == "trip": return "core_trip"
        if name == "location": return "core_location"
    return name

def daily_trips(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT date(tpep_pickup_datetime) AS d, COUNT(*) AS trips
    FROM {t}
    GROUP BY d
    ORDER BY d
    """
    return JsonResponse(run_sql_timed(sql))

def avg_fare_by_vendor(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT vendor_id, AVG(fare_amount) AS avg_fare
    FROM {t}
    GROUP BY vendor_id
    ORDER BY avg_fare DESC
    """
    return JsonResponse(run_sql_timed(sql))

def total_distance_by_pickup(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT pu_location_id, SUM(trip_distance) AS total_miles
    FROM {t}
    GROUP BY pu_location_id
    ORDER BY total_miles DESC
    LIMIT 50
    """
    return JsonResponse(run_sql_timed(sql))

def avg_tip_by_payment(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT payment_type, AVG(tip_amount) AS avg_tip
    FROM {t}
    GROUP BY payment_type
    ORDER BY avg_tip DESC
    """
    return JsonResponse(run_sql_timed(sql))

def monthly_revenue_by_dropoff(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT date_trunc('month', tpep_dropoff_datetime) AS month, do_location_id, SUM(total_amount) AS revenue
    FROM {t}
    GROUP BY month, do_location_id
    ORDER BY month, revenue DESC
    LIMIT 500
    """
    return JsonResponse(run_sql_timed(sql))

def rolling_7day_avg_trips(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    WITH daily AS (
        SELECT date(tpep_pickup_datetime) AS d, COUNT(*) AS trips
        FROM {t}
        GROUP BY d
    )
    SELECT d,
           AVG(trips) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d
    FROM daily
    ORDER BY d
    """
    return JsonResponse(run_sql_timed(sql))

def top10_pairs_by_revenue(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT pu_location_id, do_location_id, SUM(total_amount) AS revenue
    FROM {t}
    GROUP BY pu_location_id, do_location_id
    ORDER BY revenue DESC
    LIMIT 10
    """
    return JsonResponse(run_sql_timed(sql))

def daily_p90_distance(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT d, percentile_cont(0.90) WITHIN GROUP (ORDER BY trip_distance) AS p90
    FROM (
        SELECT date(tpep_pickup_datetime) AS d, trip_distance
        FROM {t}
    ) t
    GROUP BY d
    ORDER BY d
    """
    return JsonResponse(run_sql_timed(sql))

def neighborhood_tip_ranking(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    loc = tbl("location", optimized)
    sql = f"""
    SELECT l.zone, AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) ELSE 0 END) AS tip_ratio
    FROM {t} t
    JOIN {loc} l ON l.location_id = t.do_location_id
    GROUP BY l.zone
    ORDER BY tip_ratio DESC
    LIMIT 50
    """
    return JsonResponse(run_sql_timed(sql))

def vendor_95th_percentile_days(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    WITH daily_vendor AS (
        SELECT date(tpep_pickup_datetime) AS d, vendor_id, COUNT(*) AS trips
        FROM {t}
        GROUP BY d, vendor_id
    ),
    percentile AS (
        SELECT d, percentile_cont(0.95) WITHIN GROUP (ORDER BY trips) AS p95
        FROM daily_vendor
        GROUP BY d
    )
    SELECT dv.d, dv.vendor_id, dv.trips, p.p95
    FROM daily_vendor dv
    JOIN percentile p ON p.d = dv.d
    WHERE dv.trips > p.p95
    ORDER BY dv.d, dv.trips DESC
    """
    return JsonResponse(run_sql_timed(sql))
