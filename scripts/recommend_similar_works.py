from __future__ import annotations

import argparse
from pathlib import Path

from ao3_scraper.recommender import (
    RecommenderModel,
    Recommendation,
    build_model,
    connect_db,
    load_cached_model,
    recommend_similar,
    resolve_target_work_id,
    save_cached_model,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Item-to-item recommendations from AO3 kudos with weighted cosine similarity "
            "and overlap shrinkage."
        )
    )
    parser.add_argument("--db-path", type=Path, default=Path("data/ao3.db"))
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=Path("data/kudos_recommender_cache.pkl"),
        help="Pickle cache for the sparse model.",
    )
    parser.add_argument(
        "--rebuild-cache",
        action="store_true",
        help="Ignore cache and rebuild model from the SQLite database.",
    )
    parser.add_argument(
        "--work-id",
        type=int,
        default=None,
        help="Target work_id to find similar works for.",
    )
    parser.add_argument(
        "--title-query",
        type=str,
        default=None,
        help="Title search fallback when work_id is not provided.",
    )
    parser.add_argument("--title-limit", type=int, default=20)
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--min-overlap", type=int, default=3)
    parser.add_argument(
        "--shrinkage-alpha",
        type=float,
        default=25.0,
        help="Higher values penalize low-overlap pairs more strongly.",
    )
    parser.add_argument(
        "--min-candidate-kudos",
        type=int,
        default=25,
        help="Minimum number of scraped kudos edges for candidate works.",
    )
    parser.add_argument(
        "--max-user-degree",
        type=int,
        default=0,
        help=(
            "Optional cap for very heavy users. 0 disables this filter. "
            "Example: 1500."
        ),
    )
    return parser.parse_args()


def print_target_summary(model: RecommenderModel, target_work_id: int) -> None:
    meta = model.work_meta.get(target_work_id)
    edges = model.work_like_counts.get(target_work_id, 0)
    if meta is None:
        print(f"Target work_id={target_work_id} (metadata missing), kudos_edges={edges}")
        return
    author = meta.author_name or "Unknown"
    print(f"Target: {meta.title} [{author}]")
    print(f"work_id={target_work_id} | scraped_kudos_edges={edges} | url={meta.work_url}")


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def print_recommendations(model: RecommenderModel, recs: list[Recommendation]) -> None:
    if not recs:
        print("No recommendations matched the current filters.")
        return

    header = (
        f"{'rank':>4} {'work_id':>9} {'score':>9} {'cosine':>9} "
        f"{'overlap':>8} {'kudos':>8}  title [author]"
    )
    print(header)
    print("-" * len(header))
    for idx, rec in enumerate(recs, start=1):
        meta = model.work_meta.get(rec.work_id)
        if meta is None:
            title_author = "(metadata missing)"
        else:
            title_author = f"{_truncate(meta.title, 70)} [{_truncate(meta.author_name or 'Unknown', 35)}]"
        print(
            f"{idx:>4} {rec.work_id:>9} {rec.score:>9.4f} {rec.cosine:>9.4f} "
            f"{rec.overlap:>8} {rec.kudos_edges:>8}  {title_author}"
        )


def main() -> None:
    args = parse_args()
    conn = connect_db(args.db_path)
    try:
        target_work_id = resolve_target_work_id(
            conn=conn,
            work_id=args.work_id,
            title_query=args.title_query,
            title_limit=args.title_limit,
        )

        model: RecommenderModel | None = None
        if not args.rebuild_cache:
            model = load_cached_model(
                args.cache_path, args.db_path, max_user_degree=args.max_user_degree
            )
            if model is not None:
                print(f"Loaded cached model: {args.cache_path}")

        if model is None:
            model = build_model(conn, args.db_path, max_user_degree=args.max_user_degree)
            save_cached_model(args.cache_path, model)
            print(f"Saved model cache: {args.cache_path}")

        print_target_summary(model, target_work_id)
        recs = recommend_similar(
            model=model,
            target_work_id=target_work_id,
            top_k=args.top_k,
            min_overlap=args.min_overlap,
            shrinkage_alpha=args.shrinkage_alpha,
            min_candidate_kudos=args.min_candidate_kudos,
        )
        print_recommendations(model, recs)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
