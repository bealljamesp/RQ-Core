import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import yfinance as yf

from core.db_manager import RiskDBManager


def seed_aapl():
    db = RiskDBManager()
    ticker_symbol = "AAPL"

    # 1. Register the Security in our Master Table
    print(f"Registering {ticker_symbol} in Security Master...")
    db.get_data(
        "INSERT INTO securities (ticker, company_name) VALUES (%s, %s) ON CONFLICT (ticker) DO NOTHING",
        (ticker_symbol, "Apple Inc."),
    )

    # 2. Fetch 2 years of daily data
    print("Fetching market data from Yahoo Finance...")
    # We set auto_adjust=True so 'Close' IS the adjusted price
    # We set multi_level_index=False to keep the columns simple
    data = yf.download(
        ticker_symbol, period="2y", auto_adjust=True, multi_level_index=False
    )

    # 3. Get the internal Security ID
    res = db.get_data(
        "SELECT security_id FROM securities WHERE ticker = %s", (ticker_symbol,)
    )
    sec_id = int(res.iloc[0]["security_id"])

    # 4. Prepare data for SQL
    records = []
    for date, row in data.iterrows():
        # Note: Since auto_adjust=True, we now use "Close"
        # as it represents the dividend/split-adjusted price.
        records.append((sec_id, date.date(), float(row["Close"])))

    # 5. Execute Bulk Insert
    # Note: We use a standard psycopg2 connection here for speed
    import psycopg2

    with psycopg2.connect(**db.conn_params) as conn:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO daily_metrics (security_id, trade_date, adj_close)
                VALUES (%s, %s, %s)
                ON CONFLICT (security_id, trade_date) DO NOTHING
            """
            cur.executemany(insert_query, records)
            conn.commit()

    print(f"Successfully seeded {len(records)} rows for {ticker_symbol}.")


if __name__ == "__main__":
    seed_aapl()
