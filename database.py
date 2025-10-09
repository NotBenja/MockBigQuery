import duckdb
import pandas as pd
 
DB_PATH = "local_bigquery.db"
 
class DuckDBClient:
    def __init__(self, db_path: str = DB_PATH):
        self.con = duckdb.connect(db_path)
    def execute(self, query: str):
        try:
            result = self.con.execute(query)
            try:
                return result.fetchdf().to_dict(orient="records")
            except duckdb.CatalogException:
                return {"status": "ok"}
        except Exception as e:
            return {"error": str(e)}
 
    def insert_dataframe(self, table: str, df: pd.DataFrame):
        self.con.register("temp_df", df)
        self.con.execute(f"INSERT INTO {table} SELECT * FROM temp_df")