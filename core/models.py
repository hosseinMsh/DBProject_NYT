from django.db import models

class UploadedFile(models.Model):
    file = models.FileField(upload_to="uploads/")
    kind = models.CharField(max_length=20, choices=[("parquet","parquet"),("zones_csv","zones_csv")])
    status = models.CharField(max_length=20, default="pending")
    processed_rows = models.PositiveIntegerField(default=0)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

class Location(models.Model):
    location_id = models.IntegerField(primary_key=True)
    borough = models.CharField(max_length=64)
    zone = models.CharField(max_length=128)
    service_zone = models.CharField(max_length=64, null=True, blank=True)

class Trip(models.Model):
    vendor_id = models.SmallIntegerField(db_index=True)
    tpep_pickup_datetime = models.DateTimeField(db_index=True)
    tpep_dropoff_datetime = models.DateTimeField(db_index=True)
    passenger_count = models.SmallIntegerField(null=True, blank=True)
    trip_distance = models.FloatField()
    ratecode_id = models.SmallIntegerField(null=True, blank=True)
    store_and_fwd_flag = models.CharField(max_length=1, null=True, blank=True)
    pu_location_id = models.IntegerField(db_index=True)
    do_location_id = models.IntegerField(db_index=True)
    payment_type = models.SmallIntegerField(db_index=True)
    fare_amount = models.DecimalField(max_digits=10, decimal_places=2)
    extra = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mta_tax = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    tip_amount = models.DecimalField(max_digits=10, decimal_places=2)
    tolls_amount = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    total_amount = models.DecimalField(max_digits=10, decimal_places=2)
    class Meta:
        indexes = [
            models.Index(fields=["tpep_pickup_datetime"]),
            models.Index(fields=["pu_location_id"]),
            models.Index(fields=["do_location_id"]),
            models.Index(fields=["vendor_id","tpep_pickup_datetime"]),
            models.Index(fields=["payment_type"]),
        ]

class URLBatch(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    total = models.PositiveIntegerField(default=0)
    done = models.PositiveIntegerField(default=0)
    status = models.CharField(max_length=20, default="pending")
    error_message = models.TextField(null=True, blank=True)

class URLItem(models.Model):
    batch = models.ForeignKey(URLBatch, on_delete=models.CASCADE, related_name="items")
    url = models.TextField()
    kind = models.CharField(max_length=20, null=True, blank=True)  # parquet or zones_csv
    status = models.CharField(max_length=20, default="pending")    # pending/processing/done/error
    processed_rows = models.PositiveIntegerField(default=0)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
