"""Dry-run manifest for the landing-layout cleanup (read-only; copies/deletes nothing).

Legacy objects sit in date folders at entity level:
    landing/<source>/<entity>/<date>/<file>
The pipelines now write:
    landing/<source>/<entity>/<account>/<date>/<file>

This lists every legacy object, derives the account from the filename, and writes a CSV
manifest (source_uri, target_uri, status). Anything it can't map confidently is flagged,
never guessed. Only stdlib + the `gcloud` CLI are needed.

    python scripts/landing_layout_manifest.py --bucket bgr-datalake-bronze-sandbox
"""

import argparse
import csv
import re
import subprocess
import sys
from collections import Counter

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

STRIPE_ENTITIES = [
    "balance",
    "balance_transactions",
    "charges",
    "customers",
    "disputes",
    "events",
    "invoices",
    "payment_intents",
    "prices",
    "products",
    "radar_early_fraud_warnings",
    "refunds",
    "subscriptions",
]
SOLIDGATE_ENTITIES = [
    "apm_orders",
    "card_orders",
    "chargebacks",
    "financial_entries",
    "fraud_alerts",
    "subscriptions",
]
CHARGEBACKHIT_ENTITIES = ["alerts", "disputes"]

SOURCES = {
    "stripe": STRIPE_ENTITIES,
    "solidgate": SOLIDGATE_ENTITIES,
    "chargebackhit": CHARGEBACKHIT_ENTITIES,
}


def list_entity(bucket: str, source: str, entity: str) -> list[str]:
    """One paginated listing of every object under the entity (far faster than a glob per date)."""
    proc = subprocess.run(
        ["gcloud", "storage", "ls", f"gs://{bucket}/landing/{source}/{entity}/**"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0 and "matched no objects" not in proc.stderr:
        sys.exit(f"gcloud failed for {source}/{entity}: {proc.stderr.strip()}")
    return [
        ln
        for ln in proc.stdout.splitlines()
        if ln and not ln.endswith("/") and not ln.endswith(":")
    ]


def derive_account(source: str, entity: str, filename: str) -> str | None:
    if source in ("stripe", "chargebackhit"):
        m = re.fullmatch(
            r"(.+?)(?:_raw_compiled|_raw_page_\d+|_\d{4}-\d{2}-\d{2}_page_\d+)\.json",
            filename,
        )
        return m.group(1) if m else None
    if source == "solidgate":
        m = re.fullmatch(
            rf"(.+)_{re.escape(entity)}(?:_\d{{4}}-\d{{2}}-\d{{2}}_\d{{4}}-\d{{2}}-\d{{2}})?\.(?:csv|json)",
            filename,
        )
        return m.group(1).lower() if m else None
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--out", default="landing_layout_manifest.csv")
    ap.add_argument("--source", choices=list(SOURCES), help="limit to one source")
    ap.add_argument(
        "--entity",
        help="limit to one entity (use with --source to parallelise big sources)",
    )
    args = ap.parse_args()

    rows, summary = [], Counter()
    for source, entities in SOURCES.items():
        if args.source and source != args.source:
            continue
        for entity in entities:
            if args.entity and entity != args.entity:
                continue
            prefix = f"gs://{args.bucket}/landing/{source}/{entity}/"
            objects = list_entity(args.bucket, source, entity)
            parts = [(uri, uri[len(prefix) :].split("/")) for uri in objects]
            known = {p[0] for _, p in parts if len(p) > 1 and not DATE_RE.match(p[0])}
            for uri, p in parts:
                if not DATE_RE.match(p[0]):
                    continue  # already in <account>/<date>/ layout
                date, filename = p[0], "/".join(p[1:])
                if len(p) != 2:
                    rows.append((uri, "", "SKIP_NESTED"))
                    summary[(source, entity, "SKIP_NESTED")] += 1
                    continue
                account = derive_account(source, entity, filename)
                if account is None:
                    status, target = "UNPARSED_FILENAME", ""
                else:
                    target = f"{prefix}{account}/{date}/{filename}"
                    status = "OK" if account in known else "NEW_ACCOUNT_FOLDER"
                rows.append((uri, target, status))
                summary[(source, entity, status)] += 1
            print(f"{source}/{entity}: done", file=sys.stderr)

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["source_uri", "target_uri", "status"])
        w.writerows(rows)

    print(f"\nManifest: {args.out} ({len(rows)} legacy objects)")
    for (source, entity, status), n in sorted(summary.items()):
        print(f"  {source}/{entity:<28} {status:<20} {n}")
    flagged = sum(n for (_, _, s), n in summary.items() if s != "OK")
    print(f"\nNeeds review (not OK): {flagged}")


if __name__ == "__main__":
    main()
