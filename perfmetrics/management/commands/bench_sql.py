from django.core.management.base import BaseCommand, CommandError
from perfmetrics.utils import run_latency_benchmark, fetch_idx_usage_pct, fetch_db_cache_hit_pct
from perfmetrics.models import BenchmarkRun

class Command(BaseCommand):
    help = "Run a latency benchmark for a given SQL and store results."

    def add_arguments(self, parser):
        parser.add_argument("--label", required=True, help="e.g., V1 / V2 / V3")
        parser.add_argument("--sql", required=True, help="SQL text to execute")
        parser.add_argument("--runs", type=int, default=30, help="Repetitions (default: 30)")
        parser.add_argument("--no-stats", action="store_true", help="Skip DB stats snapshot")

    def handle(self, *args, **options):
        label = options["label"]
        sql = options["sql"]
        runs = options["runs"]

        if runs < 1:
            raise CommandError("runs must be >= 1")

        self.stdout.write(self.style.WARNING(f"Running benchmark: {label} (runs={runs})"))
        res = run_latency_benchmark(sql=sql, runs=runs)

        idx_pct = None
        hit_pct = None
        if not options["no-stats"]:
            try:
                idx_pct = fetch_idx_usage_pct()
                hit_pct = fetch_db_cache_hit_pct()
            except Exception:
                pass

        br = BenchmarkRun.objects.create(
            label=label,
            sql_text=sql,
            runs=runs,
            avg_ms=res["avg_ms"],
            min_ms=res["min_ms"],
            max_ms=res["max_ms"],
            p50_ms=res["p50_ms"],
            p95_ms=res["p95_ms"],
            samples_ms=res["samples_ms"],
            idx_usage_pct=idx_pct,
            db_cache_hit_pct=hit_pct,
        )

        self.stdout.write(self.style.SUCCESS(
            f"Saved: {br} | p50={br.p50_ms:.2f} ms p95={br.p95_ms:.2f} ms "
            f"cache_hit={br.db_cache_hit_pct} idx_usage={br.idx_usage_pct}"
        ))
