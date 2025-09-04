from django.urls import path
from core.views import *

urlpatterns = [
    path("", index, name="dashboard_index"),
    path("ingest/upload/", upload_page, name="upload_page"),
    path("ingest/process/<int:pk>/", process_upload, name="process_upload"),
    path("ingest/urls/", upload_urls, name="upload_urls"),
    path("ingest/process-urls/", process_urls, name="process_urls"),
    path("ingest/status/<int:pk>/", upload_status, name="upload_status"),
]
