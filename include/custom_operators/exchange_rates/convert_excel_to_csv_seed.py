"""
Run file manually to generate the history_fx.csv file. This is dbt seed file with history currency exchange rates.
Exchange rates file: https://docs.google.com/spreadsheets/d/1zuWK1-9XDEOj_pvz0zYvloxsJ6CdExq4soF9EEUuv3c/edit?gid=1699695340#gid=1699695340
Google Sheet formular used : =GoogleFinance("CURRENCY:MKDUSD", "price", TODAY()-3000, TODAY())
"""

import pandas as pd

file_path = "/path_to_downloaded_file/Google_Currency_Convert_Master.xlsx"

sheets = pd.read_excel(file_path, sheet_name=None)

result = pd.concat(
    [df.assign(sheet_name=sheet_name) for sheet_name, df in sheets.items()],
    ignore_index=True,
)

result = result.rename(
    columns={
        result.columns[0]: "date",
        result.columns[1]: "close",
        "sheet_name": "currency",
    }
)

# Move sheet_name to the third column
currency_col = result.pop("currency")
result.insert(2, "base_currency", currency_col)

result.to_csv("Google_Currency_Convert_Master.csv", index=False)
