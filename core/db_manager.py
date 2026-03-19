import pandas as pd
import psycopg2


class RiskDBManager:
    def __init__(
        self,
        dbname: str = "risk_lab",
        user: str = "postgres",
        password: str = "YOUR_PASSWORD",
    ):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": "localhost",
            "port": "5432",
        }

    def fetch_prices(self, ticker: str) -> pd.DataFrame:
        """Pulls adjusted close prices for a specific ticker."""
        query = """
            SELECT d.trade_date, d.adj_close
            FROM daily_metrics d
            JOIN securities s ON d.security_id = s.security_id
            WHERE s.ticker = %s
            ORDER BY d.trade_date ASC
        """
        # Using a context manager for the connection
        with psycopg2.connect(**self.conn_params) as conn:
            df = pd.read_sql(query, conn, params=(ticker,))

        # Convert date to index for time-series analysis
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df.set_index("trade_date")
