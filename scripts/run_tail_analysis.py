import pandas as pd

from core.db_manager import RiskDBManager
from core.risk_utils import calculate_tail_metrics


def main():
    # 1. Connect and Fetch
    db = RiskDBManager()
    query = "SELECT trade_date, adj_close FROM daily_metrics WHERE security_id = 2 ORDER BY trade_date ASC"
    df = db.get_data(query)

    # 2. Transform to Log Returns
    df["returns"] = pd.to_numeric(
        df["adj_close"]
    ).pct_change()  # Simple returns for reporting
    returns = df["returns"].dropna()

    # 3. Analyze 95% and 99%
    for conf in [0.95, 0.99]:
        m = calculate_tail_metrics(returns, conf)
        print(f"--- {conf * 100}% Confidence ---")
        print(f"VaR: {m['var']:.4%}")
        print(f"ES:  {m['es']:.4%}")
        print(f"Severity Ratio: {m['severity_ratio']:.2f}\n")


if __name__ == "__main__":
    main()
