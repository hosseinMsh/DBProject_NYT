from django.http import JsonResponse
from django.db import connection

# Helper: raw SQL to list[dict]
def run_sql(sql: str, params=None):
    with connection.cursor() as cur:
        cur.execute(sql, params or [])
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]

def daily_trips(request):
    sql = """
    SELECT date(tpep_pickup_datetime) AS d, COUNT(*) AS trips
    FROM core_trip
    GROUP BY d
    ORDER BY d
    """
    return JsonResponse(run_sql(sql), safe=False)

def avg_fare_by_vendor(request):
    sql = """
    SELECT vendor_id, AVG(fare_amount) AS avg_fare
    FROM core_trip
    GROUP BY vendor_id
    ORDER BY avg_fare DESC
    """
    return JsonResponse(run_sql(sql), safe=False)

def total_distance_by_pickup(request):
    sql = """
    SELECT pu_location_id, SUM(trip_distance) AS total_miles
    FROM core_trip
    GROUP BY pu_location_id
    ORDER BY total_miles DESC
    LIMIT 50
    """
    return JsonResponse(run_sql(sql), safe=False)

def avg_tip_by_payment(request):
    sql = """
    SELECT payment_type, AVG(tip_amount) AS avg_tip
    FROM core_trip
    GROUP BY payment_type
    ORDER BY avg_tip DESC
    """
    return JsonResponse(run_sql(sql), safe=False)

def monthly_revenue_by_dropoff(request):
    sql = """
    SELECT date_trunc('month', tpep_dropoff_datetime) AS month, do_location_id, SUM(total_amount) AS revenue
    FROM core_trip
    GROUP BY month, do_location_id
    ORDER BY month, revenue DESC
    LIMIT 500
    """
    return JsonResponse(run_sql(sql), safe=False)

def rolling_7day_avg_trips(request):
    sql = """
    WITH daily AS (
        SELECT date(tpep_pickup_datetime) AS d, COUNT(*) AS trips
        FROM core_trip
        GROUP BY d
    )
    SELECT d,
           AVG(trips) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d
    FROM daily
    ORDER BY d
    """
    return JsonResponse(run_sql(sql), safe=False)

def top10_pairs_by_revenue(request):
    sql = """
    SELECT pu_location_id, do_location_id, SUM(total_amount) AS revenue
    FROM core_trip
    GROUP BY pu_location_id, do_location_id
    ORDER BY revenue DESC
    LIMIT 10
    """
    return JsonResponse(run_sql(sql), safe=False)

def daily_p90_distance(request):
    sql = """
    SELECT d, percentile_cont(0.90) WITHIN GROUP (ORDER BY trip_distance) AS p90
    FROM (
        SELECT date(tpep_pickup_datetime) AS d, trip_distance
        FROM core_trip
    ) t
    GROUP BY d
    ORDER BY d
    """
    return JsonResponse(run_sql(sql), safe=False)

def neighborhood_tip_ranking(request):
    sql = """
    SELECT l.zone, AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) ELSE 0 END) AS tip_ratio
    FROM core_trip t
    JOIN core_location l ON l.location_id = t.do_location_id
    GROUP BY l.zone
    ORDER BY tip_ratio DESC
    LIMIT 50
    """
    return JsonResponse(run_sql(sql), safe=False)

def vendor_95th_percentile_days(request):
    sql = """
    WITH daily_vendor AS (
        SELECT date(tpep_pickup_datetime) AS d, vendor_id, COUNT(*) AS trips
        FROM core_trip
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
    return JsonResponse(run_sql(sql), safe=False)
