#V2
import time
from django.http import JsonResponse
from django.db import connection

from perfmetrics.utils import run_sql_logged_return_timed  # <-- add
def run_timed_with_opt(sql: str, view_name: str, optimized: bool):
    # Call perfmetrics timed runner but with correct label including opt flag
    from perfmetrics.utils import run_sql_logged_return_timed
    label = f"V2.{view_name}{'.opt' if optimized else ''}"
    return run_sql_logged_return_timed(sql=sql, label=label, view_name=view_name, optimized=optimized)


def run_sql_timed(sql: str, params=None):
    # Preserve V2 response shape; log internally
    import inspect
    caller = inspect.stack()[1].function  # view function name
    # optimized is derived from querystring by callers; we detect it inside views and pass in
    # To keep signature identical, we detect optimized flag later; so we wrap below in each view.
    # Here we just run without optimized flag. Views will override via a small wrapper.
    return run_sql_logged_return_timed(sql=sql, label=f"V2.{caller}", view_name=caller, optimized=False, params=params)

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
    return JsonResponse(run_timed_with_opt(sql, "daily_trips", optimized))
def avg_fare_by_vendor(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT vendor_id, AVG(fare_amount) AS avg_fare
    FROM {t}
    GROUP BY vendor_id
    ORDER BY avg_fare DESC
    """
    return JsonResponse(run_timed_with_opt(sql, "avg_fare_by_vendor", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "total_distance_by_pickup", optimized))
def avg_tip_by_payment(request):
    optimized = request.GET.get("optimized") == "1"
    t = tbl("trip", optimized)
    sql = f"""
    SELECT payment_type, AVG(tip_amount) AS avg_tip
    FROM {t}
    GROUP BY payment_type
    ORDER BY avg_tip DESC
    """
    return JsonResponse(run_timed_with_opt(sql, "avg_tip_by_payment", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "monthly_revenue_by_dropoff", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "rolling_7day_avg_trips", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "top10_pairs_by_revenue", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "daily_p90_distance", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "neighborhood_tip_ranking", optimized))
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
    return JsonResponse(run_timed_with_opt(sql, "vendor_95th_percentile_days", optimized))