
from django.contrib import admin
from .models import UploadedFile, Trip, Location, URLBatch, URLItem

@admin.register(UploadedFile)
class UploadedFileAdmin(admin.ModelAdmin):
    list_display = ("id","file","kind","status","processed_rows","created_at")
    list_filter = ("kind","status")

@admin.register(Trip)
class TripAdmin(admin.ModelAdmin):
    list_display = ("id","vendor_id","tpep_pickup_datetime","pu_location_id","do_location_id","total_amount")
    list_filter = ("vendor_id","payment_type")

@admin.register(Location)
class LocationAdmin(admin.ModelAdmin):
    list_display = ("location_id","borough","zone","service_zone")
    search_fields = ("borough","zone")

@admin.register(URLBatch)
class URLBatchAdmin(admin.ModelAdmin):
    list_display = ("id","status","total","done","created_at")

@admin.register(URLItem)
class URLItemAdmin(admin.ModelAdmin):
    list_display = ("id","batch","url","kind","status","processed_rows")
    list_filter = ("status","kind")
    search_fields = ("url",)
