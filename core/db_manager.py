import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()


class RiskDBManager:
    def __init__(self):
        # Reach into the environment for credentials
        self.password = os.getenv("DB_PASSWORD")
        self.conn_params = {
            "dbname": "risk_lab",
            "user": "postgres",
            "password": self.password,
            "host": "localhost",
            "port": "5432",
        }

    def get_data(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Generic method to handle both SELECT and INSERT/UPDATE queries."""
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                # 1. Execute the query
                cur.execute(query, params)

                # 2. If it's a SELECT query, return a DataFrame
                if query.strip().upper().startswith("SELECT"):
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                    return pd.DataFrame(data, columns=columns)

                # 3. If it's an INSERT/UPDATE, just commit and return empty DF
                conn.commit()
                return pd.DataFrame()
