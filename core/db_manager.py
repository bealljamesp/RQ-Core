import pandas as pd
import psycopg2


class RiskDBManager:
    def __init__(self):
        # Axiomatic Setup: Establish and STORE the connection
        self.conn = psycopg2.connect(
            host="localhost",
            database="risk_lab",  # Ensure this matches your DB name
            user="postgres",  # Ensure this matches your user
            password="rqpgsql",
        )

    def get_data(self, query, params=None):
        # Use self.conn here
        return pd.read_sql(query, self.conn, params=params)

    def execute_non_query(self, query, params=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                self.conn.commit()
        except Exception as e:
            # THIS IS THE KEY: If an error happens, we rollback to clear the block
            self.conn.rollback()
            print(f"Database Error: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
