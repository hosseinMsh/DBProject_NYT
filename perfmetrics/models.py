from django.db import models
from django.utils import timezone

class QueryHit(models.Model):
    # Context labels to slice your data later (e.g. V1.daily_trips, V2.avg_fare_by_vendor)
    label = models.CharField(max_length=128)
    view_name = models.CharField(max_length=128)
    sql_text = models.TextField()

    # Execution metrics
    elapsed_ms = models.FloatField()
    rows = models.IntegerField()

    # Optional flags
    optimized = models.BooleanField(default=False)

    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["label", "created_at"]),
            models.Index(fields=["view_name", "created_at"]),
        ]

    def __str__(self):
        return f"{self.created_at:%Y-%m-%d %H:%M:%S} | {self.label} | {self.elapsed_ms:.2f} ms | rows={self.rows}"
