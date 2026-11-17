import re
from pathlib import Path

SQL_PATH = (
    Path(__file__).parents[2]
    / "include"
    / "sql"
    / "paypal"
    / "merge_transactions.sql"
)


def test_merge_deduplicates_and_matches_on_transaction_and_account():
    sql = SQL_PATH.read_text(encoding="utf-8")

    assert re.search(
        r"PARTITION BY\s+JSON_VALUE\(jp,\s*'\$\.transaction_info\.transaction_id'\)"
        r"\s*,\s*_elt_account_name",
        sql,
        re.IGNORECASE,
    )
    assert re.search(
        r"ON\s+T\.transaction_id\s*=\s*S\.transaction_id\s+"
        r"AND\s+T\._elt_account_name\s*=\s*S\._elt_account_name",
        sql,
        re.IGNORECASE,
    )
    assert re.search(
        r"WHERE\s+JSON_VALUE\(jp,\s*'\$\.transaction_info\.transaction_id'\)"
        r"\s+IS NOT NULL",
        sql,
        re.IGNORECASE,
    )
