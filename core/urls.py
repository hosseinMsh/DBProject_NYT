from django.urls import path
from core.views import *

urlpatterns = [
    path("", v1, name="dashboard_index"),
    path("v2/", v2, name="dashboard_optimized"),
    # path("v3/", v3, name="dashboard_optimized"),
    path("compare/", compare, name="dashboard_compare"),
    path("ingest/upload/", upload_page, name="upload_page"),
    path("ingest/process/<int:pk>/", process_upload, name="process_upload"),
    path("ingest/urls/", upload_urls, name="upload_urls"),
    path("ingest/process-urls/", process_urls, name="process_urls"),
    path("ingest/status/<int:pk>/", upload_status, name="upload_status"),
]
