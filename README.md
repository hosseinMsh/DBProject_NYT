
# NYC Taxi Analytics (Django) – URL Bulk Ingest + Raw SQL Analytics

Features:
- Paste many URLs (one per line). Server downloads and ingests them.
- Detects .parquet (trips) vs .csv (taxi zones).
- Trip/Location tables via Django models.
- Analytics endpoints use raw SQL (no ORM) per project requirements.
- Dashboard with Chart.js to visualize outputs.

## Setup
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
pip install -r requirements.txt

# Configure Postgres via .env (see .env.example)
python manage.py makemigrations
python manage.py migrate
python manage.py runserver
```

## Usage
- Upload single file: /ingest/upload/
- Paste many URLs: /ingest/urls/  (then run /ingest/process-urls/ to process pending items)
- Dashboard: /
- APIs: /api/...

## Notes
- Raw SQL is used in `analytics/views.py` for all metrics.
- Ingestion validates basic numeric fields and inserts in batches.
- To run on large sets, consider Celery or a background worker later.
