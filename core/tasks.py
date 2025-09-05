from celery import shared_task
from django.db import transaction
from django.utils import timezone
from datetime import timezone as tz
import tempfile, os, requests, csv
import pyarrow.parquet as pq

from .models import URLItem, Trip, Location

def _ensure_aware(dt):
    # Make datetime aware only if it's naive
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return timezone.make_aware(dt, timezone=tz.utc)
    return dt

def _valid_trip_row(row: dict) -> bool:
    try:
        if float(row.get("trip_distance", 0)) < 0: return False
        if float(row.get("fare_amount", 0)) < 0: return False
        if float(row.get("total_amount", 0)) < 0: return False
        return True
    except Exception:
        return False

def _download_to_temp(url: str) -> str:
    # Stream download to a temporary file
    suffix = ".parquet" if url.lower().endswith(".parquet") else (".csv" if url.lower().endswith(".csv") else "")
    fd, tmp = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return tmp

def _ingest_parquet(file_path: str) -> int:
    # Read in bounded batches to limit memory usage
    table = pq.read_table(file_path)
    total = 0
    for batch in table.to_batches(max_chunksize=250_000):
        pyd = batch.to_pydict()
        size = len(pyd.get("VendorID", []))
        objs = []
        for i in range(size):
            row = {
                "VendorID": pyd.get("VendorID", [None] * size)[i],
                "tpep_pickup_datetime": pyd.get("tpep_pickup_datetime")[i],
                "tpep_dropoff_datetime": pyd.get("tpep_dropoff_datetime")[i],
                "passenger_count": pyd.get("passenger_count", [None] * size)[i],
                "trip_distance": pyd.get("trip_distance")[i],
                "RatecodeID": pyd.get("RatecodeID", [None] * size)[i],
                "store_and_fwd_flag": pyd.get("store_and_fwd_flag", [None] * size)[i],
                "PULocationID": pyd.get("PULocationID")[i],
                "DOLocationID": pyd.get("DOLocationID")[i],
                "payment_type": pyd.get("payment_type")[i],
                "fare_amount": pyd.get("fare_amount")[i],
                "extra": pyd.get("extra", [0] * size)[i],
                "mta_tax": pyd.get("mta_tax", [0] * size)[i],
                "tip_amount": pyd.get("tip_amount")[i],
                "tolls_amount": pyd.get("tolls_amount", [0] * size)[i],
                "total_amount": pyd.get("total_amount")[i],
            }
            if not _valid_trip_row(row):
                continue
            objs.append(Trip(
                vendor_id=int(row["VendorID"]) if row["VendorID"] is not None else 0,
                tpep_pickup_datetime=_ensure_aware(row["tpep_pickup_datetime"]),
                tpep_dropoff_datetime=_ensure_aware(row["tpep_dropoff_datetime"]),
                passenger_count=int(row["passenger_count"]) if row["passenger_count"] is not None else None,
                trip_distance=float(row["trip_distance"]),
                ratecode_id=int(row["RatecodeID"]) if row["RatecodeID"] is not None else None,
                store_and_fwd_flag=(row["store_and_fwd_flag"] or None),
                pu_location_id=int(row["PULocationID"]),
                do_location_id=int(row["DOLocationID"]),
                payment_type=int(row["payment_type"]),
                fare_amount=row["fare_amount"],
                extra=row.get("extra") or 0,
                mta_tax=row.get("mta_tax") or 0,
                tip_amount=row["tip_amount"],
                tolls_amount=row.get("tolls_amount") or 0,
                total_amount=row["total_amount"],
            ))
        if objs:
            Trip.objects.bulk_create(objs, batch_size=50_000)
            total += len(objs)
    return total

def _ingest_zones_csv(file_path: str) -> int:
    cnt = 0
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        bulk = []
        for r in reader:
            try:
                bulk.append(Location(
                    location_id=int(r.get("LocationID")),
                    borough=r.get("Borough") or "",
                    zone=r.get("Zone") or "",
                    service_zone=r.get("service_zone") or "",
                ))
            except Exception:
                continue
        if bulk:
            # Replace all to keep it simple
            with transaction.atomic():
                Location.objects.all().delete()
                Location.objects.bulk_create(bulk, batch_size=2000)
            cnt = len(bulk)
    return cnt

@shared_task(bind=True, autoretry_for=(requests.RequestException,), retry_backoff=True, max_retries=3)
def process_url_item(self, item_id: int):
    # Process a single URLItem in background
    item = URLItem.objects.select_related("batch").get(pk=item_id)
    if item.status not in ("pending", "error", "processing"):
        return

    item.status = "processing"
    item.save(update_fields=["status"])
    try:
        path = _download_to_temp(item.url)
        if item.kind == "zones_csv" or item.url.lower().endswith(".csv"):
            rows = _ingest_zones_csv(path)
        else:
            rows = _ingest_parquet(path)
        item.processed_rows = rows
        item.status = "done"
        item.error_message = ""
        item.save(update_fields=["processed_rows", "status", "error_message"])
    except Exception as e:
        item.status = "error"
        item.error_message = str(e)
        item.save(update_fields=["status", "error_message"])
    finally:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    # Update batch progress
    batch = item.batch
    done_count = batch.items.filter(status__in=["done", "error"]).count()
    batch.done = done_count
    batch.status = "done" if (done_count >= batch.total and not batch.items.filter(status="error").exists()) else (
        "error" if batch.items.filter(status="error").exists() and done_count == batch.total else "processing"
    )
    batch.save(update_fields=["done", "status"])
