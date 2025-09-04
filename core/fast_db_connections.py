from django.db import connection, transaction
from django.utils import timezone
from datetime import datetime
import io, csv, math
import pyarrow as pa
import pyarrow.parquet as pq

from core.models import Trip


# Map None/NaN safely to Python types
def _safe_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None

def _safe_int(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return int(x)
    except Exception:
        return None

def _dt_to_aware_utc(dt):
    """
    Convert various datetime-like values to timezone-aware UTC datetimes.
    Handles datetime, string, pyarrow timestamp scalars, or None.
    """
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return timezone.make_aware(dt, timezone=timezone.utc) if timezone.is_naive(dt) else dt.astimezone(timezone.utc)
    # pyarrow scalar or string
    try:
        # Attempt pandas-like parse using fromisoformat fallback
        return timezone.make_aware(datetime.fromisoformat(str(dt)), timezone=timezone.utc)
    except Exception:
        # Last resort: let Django parseat save time; but ensure awareness
        try:
            parsed = datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S")
            return timezone.make_aware(parsed, timezone=timezone.utc)
        except Exception:
            return None

def _iter_arrow_batches(table: pa.Table, chunk_size: int = 50000):
    # Yield record batches of at most chunk_size rows
    for b in table.to_batches(max_chunksize=chunk_size):
        yield b

def _ingest_parquet_fast(file_path: str) -> int:
    """
    Fast ingestion:
      - PostgreSQL: stream rows via COPY for maximum throughput.
      - Others: fall back to optimized bulk_create in chunks.
    Ensures pickup/dropoff datetimes are UTC-aware to avoid Django warnings.
    """
    table = pq.read_table(file_path)

    # Select only the columns we actually need (avoid copying unused columns)
    needed = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount",
    ]
    cols = [c for c in needed if c in table.column_names]
    table = table.select(cols)

    # Normalize timestamp columns at Arrow level if they are tz-naive
    # (We will still guard per-row as well)
    def _normalize_arrow_ts(col_name):
        if col_name in table.column_names:
            f = table.schema.field(col_name)
            if pa.types.is_timestamp(f.type) and f.type.tz is None:
                # Cast to UTC timestamp to make it aware
                idx = table.column_names.index(col_name)
                col = table.column(idx).cast(pa.timestamp('us', tz='UTC'))
                return idx, col
        return None, None

    for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        i, new_col = _normalize_arrow_ts(name)
        if i is not None:
            table = table.set_column(i, name, new_col)

    is_pg = (connection.vendor == "postgresql")
    total_rows = 0

    if is_pg:
        db_table = Trip._meta.db_table  # e.g., "app_trip"
        # List of DB columns in target insertion order
        db_cols = [
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "ratecode_id",
            "store_and_fwd_flag",
            "pu_location_id",
            "do_location_id",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]

        # Stream to COPY in chunks
        with transaction.atomic():
            with connection.cursor() as cur:
                for batch in _iter_arrow_batches(table, chunk_size=100_000):
                    # Convert arrow batch columns to Python lists once
                    B = {name: batch.column(name).to_pylist() if name in batch.schema.names else [] for name in cols}
                    n = len(B.get("VendorID", []))
                    buf = io.StringIO()
                    writer = csv.writer(buf)

                    for i in range(n):
                        # Build one row, normalized
                        row_vendor = _safe_int(B.get("VendorID", [None]*n)[i])
                        dt_pick = _dt_to_aware_utc(B.get("tpep_pickup_datetime", [None]*n)[i])
                        dt_drop = _dt_to_aware_utc(B.get("tpep_dropoff_datetime", [None]*n)[i])
                        passenger = _safe_int(B.get("passenger_count", [None]*n)[i])
                        dist = _safe_float(B.get("trip_distance", [None]*n)[i])
                        rate = _safe_int(B.get("RatecodeID", [None]*n)[i])
                        saf = (B.get("store_and_fwd_flag", [None]*n)[i] or None)
                        pu = _safe_int(B.get("PULocationID", [None]*n)[i])
                        do = _safe_int(B.get("DOLocationID", [None]*n)[i])
                        pay = _safe_int(B.get("payment_type", [None]*n)[i])
                        fare = _safe_float(B.get("fare_amount", [None]*n)[i])
                        extra = _safe_float(B.get("extra", [0]*n)[i]) or 0.0
                        mta = _safe_float(B.get("mta_tax", [0]*n)[i]) or 0.0
                        tip = _safe_float(B.get("tip_amount", [None]*n)[i])
                        tolls = _safe_float(B.get("tolls_amount", [0]*n)[i]) or 0.0
                        total = _safe_float(B.get("total_amount", [None]*n)[i])

                        # Basic row validation (reuse your rule)
                        if (dist is None or dist < 0) or (fare is None or fare < 0) or (total is None or total < 0):
                            continue

                        writer.writerow([
                            row_vendor,
                            dt_pick.isoformat() if dt_pick else None,
                            dt_drop.isoformat() if dt_drop else None,
                            passenger,
                            dist,
                            rate,
                            saf,
                            pu,
                            do,
                            pay,
                            fare,
                            extra,
                            mta,
                            tip,
                            tolls,
                            total,
                        ])
                    buf.seek(0)
                    # COPY expects NULL as empty by default; safer to set explicitly
                    copy_sql = f"COPY {db_table} ({', '.join(db_cols)}) FROM STDIN WITH (FORMAT CSV, NULL '');"
                    cur.copy_expert(copy_sql, buf)
                    total_rows += n
        return total_rows

    # Fallback for non-Postgres DBs: optimized bulk_create in chunks
    # Avoid table.to_pydict() for large tables; iterate per batch and per index
    objs = []
    for batch in _iter_arrow_batches(table, chunk_size=20_000):
        B = {name: batch.column(name).to_pylist() if name in batch.schema.names else [] for name in cols}
        n = len(B.get("VendorID", []))
        for i in range(n):
            row_vendor = _safe_int(B.get("VendorID", [None]*n)[i])
            dt_pick = _dt_to_aware_utc(B.get("tpep_pickup_datetime", [None]*n)[i])
            dt_drop = _dt_to_aware_utc(B.get("tpep_dropoff_datetime", [None]*n)[i])
            passenger = _safe_int(B.get("passenger_count", [None]*n)[i])
            dist = _safe_float(B.get("trip_distance", [None]*n)[i])
            rate = _safe_int(B.get("RatecodeID", [None]*n)[i])
            saf = (B.get("store_and_fwd_flag", [None]*n)[i] or None)
            pu = _safe_int(B.get("PULocationID", [None]*n)[i])
            do = _safe_int(B.get("DOLocationID", [None]*n)[i])
            pay = _safe_int(B.get("payment_type", [None]*n)[i])
            fare = _safe_float(B.get("fare_amount", [None]*n)[i])
            extra = _safe_float(B.get("extra", [0]*n)[i]) or 0.0
            mta = _safe_float(B.get("mta_tax", [0]*n)[i]) or 0.0
            tip = _safe_float(B.get("tip_amount", [None]*n)[i])
            tolls = _safe_float(B.get("tolls_amount", [0]*n)[i]) or 0.0
            total = _safe_float(B.get("total_amount", [None]*n)[i])

            if (dist is None or dist < 0) or (fare is None or fare < 0) or (total is None or total < 0):
                continue

            objs.append(Trip(
                vendor_id=row_vendor or 0,
                tpep_pickup_datetime=dt_pick,
                tpep_dropoff_datetime=dt_drop,
                passenger_count=passenger,
                trip_distance=dist or 0.0,
                ratecode_id=rate,
                store_and_fwd_flag=saf,
                pu_location_id=pu or 0,
                do_location_id=do or 0,
                payment_type=pay or 0,
                fare_amount=fare or 0.0,
                extra=extra,
                mta_tax=mta,
                tip_amount=tip or 0.0,
                tolls_amount=tolls,
                total_amount=total or 0.0,
            ))

        if objs:
            Trip.objects.bulk_create(objs, batch_size=10_000)
            total_rows += len(objs)
            objs.clear()

    return total_rows
