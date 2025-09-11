from django.contrib import admin
from .models import QueryHit

@admin.register(QueryHit)
class QueryHitAdmin(admin.ModelAdmin):
    list_display = ("created_at", "label", "view_name", "elapsed_ms", "rows", "optimized")
    list_filter = ("label", "view_name", "optimized", "created_at")
    search_fields = ("sql_text",)
    readonly_fields = ("created_at", "sql_text")
