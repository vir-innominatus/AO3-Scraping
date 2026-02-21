from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from ao3_scraper.recommender import load_model_from_cache, recommend_similar


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export AO3 recommendations from recommender cache into static JSON for GitHub Pages."
    )
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=Path("data/kudos_recommender_cache.pkl"),
        help="Path to recommender pickle cache.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("docs/data/recommendations.json"),
        help="Output JSON path for the static web app.",
    )
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--min-overlap", type=int, default=3)
    parser.add_argument("--shrinkage-alpha", type=float, default=25.0)
    parser.add_argument("--min-candidate-kudos", type=int, default=25)
    parser.add_argument(
        "--min-target-kudos",
        type=int,
        default=25,
        help="Only export target works with at least this many scraped kudos edges.",
    )
    parser.add_argument(
        "--max-target-works",
        type=int,
        default=0,
        help="Optional cap on number of target works (0 means no cap).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Print progress every N processed target works.",
    )
    parser.add_argument(
        "--pretty",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Write pretty JSON with indentation (default is compact).",
    )
    return parser.parse_args()


def _work_payload(model, work_id: int) -> dict[str, object]:
    meta = model.work_meta.get(work_id)
    return {
        "id": work_id,
        "title": "" if meta is None else meta.title,
        "author": "" if meta is None else meta.author_name,
        "url": "" if meta is None else meta.work_url,
        "kudos": None if meta is None else meta.kudos,
        "guest_kudos": None if meta is None else meta.guest_kudos,
        "hits": None if meta is None else meta.hits,
        "kudos_edges": model.work_like_counts.get(work_id, 0),
    }


def main() -> None:
    args = parse_args()
    if args.top_k <= 0:
        raise SystemExit("--top-k must be > 0")
    if args.min_overlap <= 0:
        raise SystemExit("--min-overlap must be > 0")
    if args.min_candidate_kudos < 0:
        raise SystemExit("--min-candidate-kudos must be >= 0")
    if args.min_target_kudos < 0:
        raise SystemExit("--min-target-kudos must be >= 0")
    if args.max_target_works < 0:
        raise SystemExit("--max-target-works must be >= 0")
    if args.progress_every <= 0:
        raise SystemExit("--progress-every must be > 0")

    model = load_model_from_cache(args.cache_path)

    target_ids = [
        work_id
        for work_id in model.work_ids
        if model.work_like_counts.get(work_id, 0) >= args.min_target_kudos
    ]
    target_ids.sort(key=lambda wid: model.work_like_counts.get(wid, 0), reverse=True)

    if args.max_target_works > 0:
        target_ids = target_ids[: args.max_target_works]

    recommendations: dict[str, list[dict[str, object]]] = {}
    referenced_work_ids: set[int] = set(target_ids)

    total = len(target_ids)
    print(f"Exporting recommendations for {total} target works...")

    for idx, target_work_id in enumerate(target_ids, start=1):
        recs = recommend_similar(
            model=model,
            target_work_id=target_work_id,
            top_k=args.top_k,
            min_overlap=args.min_overlap,
            shrinkage_alpha=args.shrinkage_alpha,
            min_candidate_kudos=args.min_candidate_kudos,
        )
        row: list[dict[str, object]] = []
        for rec in recs:
            row.append(
                {
                    "id": rec.work_id,
                    "score": round(rec.score, 6),
                    "cosine": round(rec.cosine, 6),
                    "overlap": rec.overlap,
                    "kudos_edges": rec.kudos_edges,
                }
            )
            referenced_work_ids.add(rec.work_id)
        recommendations[str(target_work_id)] = row

        if (idx % args.progress_every) == 0 or idx == total:
            print(f"Progress: {idx}/{total} targets processed")

    works_payload = [_work_payload(model, wid) for wid in sorted(referenced_work_ids)]

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_cache_path": str(args.cache_path),
        "parameters": {
            "top_k": args.top_k,
            "min_overlap": args.min_overlap,
            "shrinkage_alpha": args.shrinkage_alpha,
            "min_candidate_kudos": args.min_candidate_kudos,
            "min_target_kudos": args.min_target_kudos,
            "max_target_works": args.max_target_works,
        },
        "stats": {
            "model_work_count": len(model.work_ids),
            "target_work_count": len(target_ids),
            "exported_work_count": len(works_payload),
            "recommendation_row_count": len(recommendations),
        },
        "target_work_ids": target_ids,
        "works": works_payload,
        "recommendations": recommendations,
    }

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as fh:
        if args.pretty:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        else:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = args.output_path.stat().st_size / (1024 * 1024)
    print(f"Wrote: {args.output_path} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    main()
