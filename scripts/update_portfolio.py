from datetime import timedelta

import pandas as pd
import yfinance as yf

from core.db_manager import RiskDBManager


def update_security(db, ticker, security_id):

    # 1. Check the last date in the DB
    result = db.get_data(
        f"SELECT MAX(trade_date) FROM daily_metrics WHERE security_id = {security_id}"
    )
    last_date = result.iloc[0, 0] if not result.empty else None

    # 2. MECHANISTIC FIX: Ensure start_date is always defined
    if last_date is not None:
        # Use the day after the last date in the DB
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # Fallback if the table is empty for this ticker
        start_date = "2024-03-21"

    # 3. Now yf.download can see the variable
    print(f"Fetching {ticker} from {start_date}...")
    data = yf.download(ticker, start=start_date)

    if data.empty:
        return

    # Flatten MultiIndex if it exists (e.g., [('Adj Close', 'SPY')] -> ['Adj Close'])
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    # CRITICAL: Remove extra spaces and standardize naming
    # This turns "Adj  Close " into "Adj Close"
    data.columns = data.columns.str.strip()

    # Handle the 'Adj Close' vs 'Adj Close' quirk
    # Some versions return 'Adj Close', others return 'Close' if it's already adjusted
    if "Adj Close" not in data.columns and "Close" in data.columns:
        data["Adj Close"] = data["Close"]

    insert_sql = """
        INSERT INTO daily_metrics (security_id, trade_date, adj_close)
        VALUES (%s, %s, %s)
        ON CONFLICT (security_id, trade_date) DO NOTHING;
    """

    for date, row in data.iterrows():
        adj_close = float(row["Adj Close"])

        # 2. Only send three parameters now
        db.execute_non_query(insert_sql, (security_id, date.to_pydatetime(), adj_close))


if __name__ == "__main__":
    manager = RiskDBManager()
    # Update our Portfolio
    update_security(manager, "SPY", 1)
    update_security(manager, "AAPL", 2)
    print("Portfolio update complete.")
