from django.urls import path
from analytics.views import v1, v2, v3

urlpatterns = [
    # v1 (legacy)
    path("v1/daily-trips/", v1.daily_trips),
    path("v1/avg-fare-by-vendor/", v1.avg_fare_by_vendor),
    path("v1/total-distance-by-pickup/", v1.total_distance_by_pickup),
    path("v1/avg-tip-by-payment/", v1.avg_tip_by_payment),
    path("v1/monthly-revenue-by-dropoff/", v1.monthly_revenue_by_dropoff),
    path("v1/rolling-7day-avg-trips/", v1.rolling_7day_avg_trips),
    path("v1/top10-pairs-by-revenue/", v1.top10_pairs_by_revenue),
    path("v1/daily-p90-distance/", v1.daily_p90_distance),
    path("v1/neighborhood-tip-ranking/", v1.neighborhood_tip_ranking),
    path("v1/vendor-95th-percentile-days/", v1.vendor_95th_percentile_days),

    # v2 (timed + optional optimized tables via ?optimized=1)
    path("v2/daily-trips/", v2.daily_trips),
    path("v2/avg-fare-by-vendor/", v2.avg_fare_by_vendor),
    path("v2/total-distance-by-pickup/", v2.total_distance_by_pickup),
    path("v2/avg-tip-by-payment/", v2.avg_tip_by_payment),
    path("v2/monthly-revenue-by-dropoff/", v2.monthly_revenue_by_dropoff),
    path("v2/rolling-7day-avg-trips/", v2.rolling_7day_avg_trips),
    path("v2/top10-pairs-by-revenue/", v2.top10_pairs_by_revenue),
    path("v2/daily-p90-distance/", v2.daily_p90_distance),
    path("v2/neighborhood-tip-ranking/", v2.neighborhood_tip_ranking),
    path("v2/vendor-95th-percentile-days/", v2.vendor_95th_percentile_days),

    # v3 (optimized-by-default, timed)
    # path("v3/build-optimized/", v3.build_optimized),
    # path("v3/daily-trips/", v3.daily_trips),
    # path("v3/avg-fare-by-vendor/", v3.avg_fare_by_vendor),
    # path("v3/total-distance-by-pickup/", v3.total_distance_by_pickup),
    # path("v3/avg-tip-by-payment/", v3.avg_tip_by_payment),
    # path("v3/monthly-revenue-by-dropoff/", v3.monthly_revenue_by_dropoff),
    # path("v3/rolling-7day-avg-trips/", v3.rolling_7day_avg_trips),
    # path("v3/top10-pairs-by-revenue/", v3.top10_pairs_by_revenue),
    # path("v3/daily-p90-distance/", v3.daily_p90_distance),
    # path("v3/neighborhood-tip-ranking/", v3.neighborhood_tip_ranking),
    # path("v3/vendor-95th-percentile-days/", v3.vendor_95th_percentile_days),
]
