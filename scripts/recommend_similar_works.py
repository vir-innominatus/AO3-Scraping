from __future__ import annotations

import argparse
import heapq
import math
import pickle
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass
class WorkMeta:
    work_id: int
    title: str
    author_name: str
    work_url: str
    kudos: int | None
    guest_kudos: int | None
    hits: int | None


@dataclass
class RecommenderModel:
    db_size: int
    db_mtime: float
    work_ids: list[int]
    work_to_row: dict[int, int]
    row_users: list[list[int]]
    user_to_rows: list[list[int]]
    user_weight_sq: list[float]
    row_norms: list[float]
    work_like_counts: dict[int, int]
    work_meta: dict[int, WorkMeta]
    max_user_degree: int


@dataclass
class Recommendation:
    work_id: int
    score: float
    cosine: float
    overlap: int
    kudos_edges: int


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


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def load_work_metadata(conn: sqlite3.Connection) -> dict[int, WorkMeta]:
    metadata: dict[int, WorkMeta] = {}
    for row in conn.execute(
        """
        SELECT work_id, title, author_name, work_url, kudos, guest_kudos, hits
        FROM works
        """
    ):
        metadata[row["work_id"]] = WorkMeta(
            work_id=row["work_id"],
            title=row["title"] or "",
            author_name=row["author_name"] or "",
            work_url=row["work_url"] or "",
            kudos=row["kudos"],
            guest_kudos=row["guest_kudos"],
            hits=row["hits"],
        )
    return metadata


def load_work_like_counts(conn: sqlite3.Connection) -> dict[int, int]:
    counts: dict[int, int] = {}
    for row in conn.execute(
        """
        SELECT work_id, COUNT(*) AS c
        FROM kudos
        GROUP BY work_id
        """
    ):
        counts[row["work_id"]] = row["c"]
    return counts


def build_model(conn: sqlite3.Connection, db_path: Path, max_user_degree: int) -> RecommenderModel:
    print("Loading metadata and kudos counts...")
    work_meta = load_work_metadata(conn)
    work_like_counts = load_work_like_counts(conn)

    work_ids = sorted(work_like_counts.keys())
    if not work_ids:
        raise SystemExit("No kudos edges found. Run scrape-kudos-from-db first.")

    work_to_row = {work_id: row for row, work_id in enumerate(work_ids)}
    row_users: list[list[int]] = [[] for _ in work_ids]
    user_to_rows: list[list[int]] = []
    user_to_idx: dict[str, int] = {}

    print("Building sparse work-user matrix...")
    query = "SELECT work_id, pseud_url FROM kudos ORDER BY work_id, pseud_url"
    for row in conn.execute(query):
        work_id = row["work_id"]
        row_idx = work_to_row.get(work_id)
        if row_idx is None:
            continue

        pseud_url = row["pseud_url"]
        user_idx = user_to_idx.get(pseud_url)
        if user_idx is None:
            user_idx = len(user_to_rows)
            user_to_idx[pseud_url] = user_idx
            user_to_rows.append([])

        row_users[row_idx].append(user_idx)
        user_to_rows[user_idx].append(row_idx)

    if max_user_degree > 0:
        print(f"Applying max user degree filter: {max_user_degree}")
        keep_users = [len(rows) <= max_user_degree for rows in user_to_rows]
        if not any(keep_users):
            raise SystemExit("All users were filtered out by max_user_degree.")
        row_users = _rebuild_row_users_with_user_filter(row_users, keep_users)
        user_to_rows = _rebuild_user_to_rows(row_users, sum(keep_users))

    print("Computing user-frequency weights and row norms...")
    n_works = len(work_ids)
    user_weight_sq: list[float] = []
    for rows in user_to_rows:
        degree = len(rows)
        # Down-weight high-degree users similarly to IDF.
        weight = math.log((1.0 + n_works) / (1.0 + degree)) + 1.0
        user_weight_sq.append(weight * weight)

    row_norms: list[float] = []
    for users in row_users:
        norm_sq = 0.0
        for user_idx in users:
            norm_sq += user_weight_sq[user_idx]
        row_norms.append(math.sqrt(norm_sq))

    stat = db_path.stat()
    return RecommenderModel(
        db_size=stat.st_size,
        db_mtime=stat.st_mtime,
        work_ids=work_ids,
        work_to_row=work_to_row,
        row_users=row_users,
        user_to_rows=user_to_rows,
        user_weight_sq=user_weight_sq,
        row_norms=row_norms,
        work_like_counts=work_like_counts,
        work_meta=work_meta,
        max_user_degree=max_user_degree,
    )


def _rebuild_row_users_with_user_filter(
    row_users: list[list[int]], keep_users: list[bool]
) -> list[list[int]]:
    old_to_new: dict[int, int] = {}
    next_idx = 0
    for old_idx, keep in enumerate(keep_users):
        if keep:
            old_to_new[old_idx] = next_idx
            next_idx += 1

    rebuilt: list[list[int]] = []
    for users in row_users:
        filtered = []
        for old_user in users:
            new_user = old_to_new.get(old_user)
            if new_user is not None:
                filtered.append(new_user)
        rebuilt.append(filtered)
    return rebuilt


def _rebuild_user_to_rows(row_users: list[list[int]], user_count: int) -> list[list[int]]:
    user_to_rows: list[list[int]] = [[] for _ in range(user_count)]
    for row_idx, users in enumerate(row_users):
        for user_idx in users:
            user_to_rows[user_idx].append(row_idx)
    return user_to_rows


def load_cached_model(
    cache_path: Path, db_path: Path, max_user_degree: int
) -> RecommenderModel | None:
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("rb") as fh:
            model = pickle.load(fh)
    except Exception:
        return None

    if not isinstance(model, RecommenderModel):
        return None

    stat = db_path.stat()
    if model.db_size != stat.st_size or abs(model.db_mtime - stat.st_mtime) > 1e-6:
        return None
    if getattr(model, "max_user_degree", 0) != max_user_degree:
        return None
    return model


def save_cached_model(cache_path: Path, model: RecommenderModel) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("wb") as fh:
        pickle.dump(model, fh)


def resolve_target_work_id(
    conn: sqlite3.Connection,
    work_id: int | None,
    title_query: str | None,
    title_limit: int,
) -> int:
    if work_id is not None:
        return work_id

    if not title_query:
        raise SystemExit("Provide either --work-id or --title-query.")

    pattern = f"%{title_query}%"
    matches = list(
        conn.execute(
            """
            SELECT work_id, title, author_name, kudos
            FROM works
            WHERE title LIKE ?
            ORDER BY kudos DESC, work_id DESC
            LIMIT ?
            """,
            (pattern, title_limit),
        )
    )
    if not matches:
        raise SystemExit(f"No works matched title query: {title_query!r}")

    if len(matches) == 1:
        chosen = matches[0]
        print(f"Selected only match: {chosen['work_id']} - {chosen['title']}")
        return int(chosen["work_id"])

    print(f"Multiple works matched {title_query!r}:")
    for idx, row in enumerate(matches, start=1):
        author = row["author_name"] or "Unknown"
        print(
            f"{idx:>2}. {row['work_id']:>9} | kudos={row['kudos'] or 0:>6} "
            f"| {row['title']} [{author}]"
        )

    if not sys.stdin.isatty():
        raise SystemExit(
            "Multiple matches found in non-interactive mode. "
            "Re-run with --work-id."
        )

    while True:
        try:
            raw = input("Select a number from the list above: ").strip()
        except EOFError as exc:
            raise SystemExit(
                "No interactive input available. Re-run with --work-id."
            ) from exc
        if not raw.isdigit():
            print("Please enter a valid number.")
            continue
        choice = int(raw)
        if 1 <= choice <= len(matches):
            return int(matches[choice - 1]["work_id"])
        print("Choice out of range.")


def recommend_similar(
    model: RecommenderModel,
    target_work_id: int,
    top_k: int,
    min_overlap: int,
    shrinkage_alpha: float,
    min_candidate_kudos: int,
) -> list[Recommendation]:
    target_row = model.work_to_row.get(target_work_id)
    if target_row is None:
        raise SystemExit(
            "Target work has no scraped kudos edges in the model. "
            "Scrape more kudos or choose a different work."
        )

    target_norm = model.row_norms[target_row]
    if target_norm <= 0:
        raise SystemExit("Target work vector is empty after filtering.")

    dots: dict[int, float] = defaultdict(float)
    overlaps: dict[int, int] = defaultdict(int)

    for user_idx in model.row_users[target_row]:
        weight_sq = model.user_weight_sq[user_idx]
        for other_row in model.user_to_rows[user_idx]:
            if other_row == target_row:
                continue
            dots[other_row] += weight_sq
            overlaps[other_row] += 1

    scored: list[Recommendation] = []
    for other_row, dot in dots.items():
        overlap = overlaps[other_row]
        if overlap < min_overlap:
            continue

        other_work_id = model.work_ids[other_row]
        kudos_edges = model.work_like_counts.get(other_work_id, 0)
        if kudos_edges < min_candidate_kudos:
            continue

        denom = target_norm * model.row_norms[other_row]
        if denom <= 0:
            continue

        cosine = dot / denom
        if shrinkage_alpha > 0:
            shrink = overlap / (overlap + shrinkage_alpha)
        else:
            shrink = 1.0
        score = cosine * shrink
        scored.append(
            Recommendation(
                work_id=other_work_id,
                score=score,
                cosine=cosine,
                overlap=overlap,
                kudos_edges=kudos_edges,
            )
        )

    return heapq.nlargest(top_k, scored, key=lambda item: item.score)


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
