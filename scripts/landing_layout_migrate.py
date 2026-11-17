"""Apply the landing-layout cleanup from a manifest made by landing_layout_manifest.py.

Two separate phases so a team can review in between:

    copy    server-side copy source -> target, never overwriting (idempotent, safe to re-run)
    delete  delete a source ONLY if its target exists with identical size + checksum

Everything is a dry run unless --execute is passed. Only manifest rows with status OK are touched.
Requires: pip install google-cloud-storage, and `gcloud auth application-default login`.

    python scripts/landing_layout_migrate.py copy   --manifest m.csv --bucket B                # dry run
    python scripts/landing_layout_migrate.py copy   --manifest m.csv --bucket B --execute --confirm-bucket B
    python scripts/landing_layout_migrate.py delete --manifest m.csv --bucket B --execute --confirm-bucket B
"""

import argparse
import csv
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed


def load_rows(manifest: str, bucket: str, limit: int | None) -> list[tuple[str, str]]:
    pairs, prefix = [], f"gs://{bucket}/"
    with open(manifest, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["status"] != "OK":
                continue
            src, dst = r["source_uri"], r["target_uri"]
            if not (src.startswith(prefix) and dst.startswith(prefix)):
                sys.exit(f"Manifest row is not in gs://{bucket}: {src} -> {dst}")
            pairs.append((src[len(prefix) :], dst[len(prefix) :]))
    return pairs[:limit] if limit else pairs


def same_object(src, dst) -> bool:
    return (
        src is not None
        and dst is not None
        and src.size == dst.size
        and src.crc32c == dst.crc32c
        and src.md5_hash == dst.md5_hash
    )


def do_copy(bucket, pair):
    src_name, dst_name = pair
    src = bucket.get_blob(src_name)
    if src is None:
        return pair, "SOURCE_MISSING"
    try:
        bucket.copy_blob(
            src, bucket, dst_name, if_generation_match=0
        )  # 0 = only if target absent
        return pair, "COPIED"
    except Exception as exc:  # noqa: BLE001
        if "412" in str(exc) or "conditionNotMet" in str(exc):
            return pair, "TARGET_EXISTS"
        return pair, f"ERROR {exc}"


def do_delete(bucket, pair):
    src_name, dst_name = pair
    src, dst = bucket.get_blob(src_name), bucket.get_blob(dst_name)
    if src is None:
        return pair, "ALREADY_GONE"
    if not same_object(src, dst):
        return pair, "TARGET_MISSING_OR_DIFFERENT"  # never delete on a mismatch
    src.delete(if_generation_match=src.generation)
    return pair, "DELETED"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["copy", "delete"])
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument(
        "--execute", action="store_true", help="actually do it (default: dry run)"
    )
    ap.add_argument(
        "--confirm-bucket", help="must equal --bucket when --execute is set"
    )
    ap.add_argument("--limit", type=int, help="only the first N rows (for a pilot)")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--results", default=None, help="CSV of per-object outcomes")
    args = ap.parse_args()

    pairs = load_rows(args.manifest, args.bucket, args.limit)
    print(f"{args.phase}: {len(pairs)} OK rows in gs://{args.bucket}")
    for s, d in pairs[:3]:
        print(f"  e.g. {s}\n    -> {d}")

    if not args.execute:
        print(
            "DRY RUN: nothing changed. Re-run with --execute --confirm-bucket <bucket> to apply."
        )
        return
    if args.confirm_bucket != args.bucket:
        sys.exit("Refusing: --confirm-bucket must exactly match --bucket.")

    from google.cloud import (
        storage,  # imported late so the dry run needs no credentials
    )

    bucket = storage.Client().bucket(args.bucket)
    fn = do_copy if args.phase == "copy" else do_delete
    results_path = args.results or f"landing_layout_{args.phase}_results.csv"
    outcomes = Counter()
    with (
        open(results_path, "w", newline="") as fh,
        ThreadPoolExecutor(args.workers) as pool,
    ):
        w = csv.writer(fh)
        w.writerow(["source", "target", "outcome"])
        futures = [pool.submit(fn, bucket, p) for p in pairs]
        for i, fut in enumerate(as_completed(futures), 1):
            (src, dst), outcome = fut.result()
            outcomes[outcome.split(" ")[0]] += 1
            w.writerow([src, dst, outcome])
            if i % 5000 == 0:
                print(f"  {i}/{len(pairs)} {dict(outcomes)}", flush=True)
    print(f"Done. {dict(outcomes)}  results -> {results_path}")
    if any(
        k in outcomes
        for k in ("ERROR", "SOURCE_MISSING", "TARGET_MISSING_OR_DIFFERENT")
    ):
        sys.exit("Some rows need attention; see the results CSV.")


if __name__ == "__main__":
    main()
