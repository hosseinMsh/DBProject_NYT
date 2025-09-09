from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, HttpResponseBadRequest, JsonResponse
from django.utils import timezone
from datetime import timezone as tz
from core.tasks import process_url_item
from core.models import UploadedFile, Trip, Location, URLBatch, URLItem
import csv, os, tempfile, requests
import pyarrow.parquet as pq

@login_required(login_url='/admin/login/?next=/')
def upload_page(request: HttpRequest):
    if request.method == "POST":
        f = request.FILES.get("file")
        kind = request.POST.get("kind", "parquet")
        if not f:
            return HttpResponseBadRequest("No file provided")
        uf = UploadedFile.objects.create(file=f, kind=kind, status="pending")
        return redirect("process_upload", pk=uf.pk)
    return render(request, "dashboard/upload.html", {})


def _valid_trip_row(row: dict) -> bool:
    try:
        if float(row.get("trip_distance", 0)) < 0: return False
        if float(row.get("fare_amount", 0)) < 0: return False
        if float(row.get("total_amount", 0)) < 0: return False
        return True
    except Exception:
        return False


def _ingest_parquet(file_path: str) -> int:
    table = pq.read_table(file_path)
    total = 0
    for batch in table.to_batches(max_chunksize=350000):
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
                tpep_pickup_datetime=timezone.make_aware(row["tpep_pickup_datetime"], timezone=tz.utc) if row[
                    "tpep_pickup_datetime"] else None,
                tpep_dropoff_datetime=timezone.make_aware(row["tpep_dropoff_datetime"], timezone=tz.utc) if row[
                    "tpep_dropoff_datetime"] else None,
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
            Trip.objects.bulk_create(objs, batch_size=350000)

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
            Location.objects.all().delete()
            Location.objects.bulk_create(bulk, batch_size=2000)
            cnt = len(bulk)
    return cnt


@login_required(login_url='/admin/login/?next=/')
def process_upload(request: HttpRequest, pk: int):
    uf = get_object_or_404(UploadedFile, pk=pk)
    if uf.status not in ("pending", "error"):
        return redirect("upload_page")
    uf.status = "processing";
    uf.save(update_fields=["status"])
    file_path = uf.file.path
    try:
        if uf.kind == "parquet":
            rows = _ingest_parquet(file_path)
        elif uf.kind == "zones_csv":
            rows = _ingest_zones_csv(file_path)
        else:
            return HttpResponseBadRequest("Unknown file kind")
        uf.status = "done";
        uf.processed_rows = rows
        uf.save(update_fields=["status", "processed_rows"])
        return render(request, "dashboard/done.html", {"uf": uf})
    except Exception as e:
        uf.status = "error";
        uf.error_message = str(e)
        uf.save(update_fields=["status", "error_message"])
        return render(request, "dashboard/done.html", {"uf": uf})


@login_required(login_url='/admin/login/?next=/')
def upload_urls(request: HttpRequest):
    # Create a batch and enqueue Celery tasks; then redirect to status page immediately
    if request.method == "POST":
        raw = request.POST.get("urls", "")
        urls = [u.strip() for u in raw.splitlines() if u.strip()]
        if not urls:
            return HttpResponseBadRequest("No URLs provided")

        batch = URLBatch.objects.create(status="processing", total=len(urls), done=0)
        items = []
        for u in urls:
            kind = "parquet" if u.lower().endswith(".parquet") else ("zones_csv" if u.lower().endswith(".csv") else "parquet")
            items.append(URLItem(batch=batch, url=u, kind=kind, status="pending"))
        URLItem.objects.bulk_create(items, batch_size=1000)

        # Enqueue Celery tasks per item (lightweight loop)
        for item_id in batch.items.values_list("id", flat=True):
            process_url_item.delay(item_id)

        return redirect("upload_status", pk=batch.pk)

    return render(request, "dashboard/upload_urls.html", {})

@login_required(login_url='/admin/login/?next=/')
def upload_status(request: HttpRequest, pk: int):
    # Render a page with polling JS; items themselves not heavy-rendered here
    batch = get_object_or_404(URLBatch, pk=pk)
    return render(request, "dashboard/status.html", {"batch": batch})

@login_required(login_url='/admin/login/?next=/')
def upload_status_api(request: HttpRequest, pk: int):
    # Lightweight JSON for polling
    batch = get_object_or_404(URLBatch, pk=pk)
    items = list(batch.items.order_by("id").values("id", "url", "kind", "status", "processed_rows", "error_message"))
    return JsonResponse({
        "batch": {
            "id": batch.id,
            "status": batch.status,
            "total": batch.total,
            "done": batch.done,
        },
        "items": items
    })

def _download_to_temp(url: str) -> str:
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        suffix = ".parquet" if url.lower().endswith(".parquet") else (".csv" if url.lower().endswith(".csv") else "")
        fd, tmp = tempfile.mkstemp(suffix=suffix)
        with os.fdopen(fd, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk: f.write(chunk)
        return tmp


@login_required(login_url='/admin/login/?next=/')
def process_urls(request: HttpRequest):
    pending = URLItem.objects.filter(status="pending").order_by("id")
    for item in pending:
        item.status = "processing";
        item.save(update_fields=["status"])
        try:
            path = _download_to_temp(item.url)
            if item.kind == "parquet":
                rows = _ingest_parquet(path)
            elif item.kind == "zones_csv":
                rows = _ingest_zones_csv(path)
            else:
                rows = _ingest_parquet(path)
            item.processed_rows = rows
            item.status = "done"
            item.save(update_fields=["processed_rows", "status"])
        except Exception as e:
            item.status = "error";
            item.error_message = str(e)
            item.save(update_fields=["status", "error_message"])
        batch = item.batch
        batch.done = batch.items.filter(status__in=["done", "error"]).count()
        if batch.done >= batch.total:
            batch.status = "error" if batch.items.filter(status="error").exists() else "done"
        else:
            batch.status = "processing"
        batch.save(update_fields=["done", "status"])
    return redirect("upload_urls")


@login_required(login_url='/admin/login/?next=/')
def v1(request):
    return render(request, "dashboard/index/v1.html", {})


@login_required(login_url='/admin/login/?next=/v2')
def v2(request):
    return render(request, "dashboard/index/v2.html", {})

def v3(request):
    return render(request, "dashboard/index/v3.html", {})


@login_required(login_url='/admin/login/?next=/compare')
def compare(request):
    return render(request, "dashboard/compare.html", {})
