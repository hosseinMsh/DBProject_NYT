
from django.urls import path
from analytics.views import (
    daily_trips, avg_fare_by_vendor, total_distance_by_pickup, avg_tip_by_payment,
    monthly_revenue_by_dropoff, rolling_7day_avg_trips, top10_pairs_by_revenue,
    daily_p90_distance, neighborhood_tip_ranking, vendor_95th_percentile_days
)
urlpatterns = [
    path("daily-trips/", daily_trips),
    path("avg-fare-by-vendor/", avg_fare_by_vendor),
    path("total-distance-by-pickup/", total_distance_by_pickup),
    path("avg-tip-by-payment/", avg_tip_by_payment),
    path("monthly-revenue-by-dropoff/", monthly_revenue_by_dropoff),
    path("rolling-7day-avg-trips/", rolling_7day_avg_trips),
    path("top10-pairs-by-revenue/", top10_pairs_by_revenue),
    path("daily-p90-distance/", daily_p90_distance),
    path("neighborhood-tip-ranking/", neighborhood_tip_ranking),
    path("vendor-95th-percentile-days/", vendor_95th_percentile_days),
]
