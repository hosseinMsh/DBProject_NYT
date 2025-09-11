from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.db.models import Avg, Min, Max, Count
from .models import QueryHit

@require_GET
def latest_hits(request):
    qs = QueryHit.objects.order_by("-created_at")[:200]
    data = [
        {
            "ts": q.created_at.isoformat(),
            "label": q.label,
            "view": q.view_name,
            "ms": round(q.elapsed_ms, 2),
            "rows": q.rows,
            "opt": q.optimized,
        }
        for q in qs
    ]
    return JsonResponse({"results": data}, status=200)

@require_GET
def summary_by_label(request):
    qs = (
        QueryHit.objects.values("label")
        .annotate(
            cnt=Count("id"),
            avg_ms=Avg("elapsed_ms"),
            min_ms=Min("elapsed_ms"),
            max_ms=Max("elapsed_ms"),
        )
        .order_by("label")
    )
    data = [
        {
            "label": r["label"],
            "count": r["cnt"],
            "avg_ms": round(r["avg_ms"] or 0, 2),
            "min_ms": round(r["min_ms"] or 0, 2),
            "max_ms": round(r["max_ms"] or 0, 2),
        }
        for r in qs
    ]
    return JsonResponse({"summary": data}, status=200)
