from django.urls import path
from perfmetrics.views import latest_hits, summary_by_label

urlpatterns = [
    path("hits/latest/", latest_hits, name="metrics-latest-hits"),
    path("hits/summary/", summary_by_label, name="metrics-summary-by-label"),
]
