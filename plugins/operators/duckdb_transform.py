from airflow.models import BaseOperator
import duckdb
import os

class DuckDBTransformOperator(BaseOperator):
    def __init__(
        self,
        task_id: str,
        source_path: str,
        destination_path: str,
        sql_path: str,
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.source_path = source_path
        self.destination_path = destination_path
        self.sql_path = sql_path

    def execute(self, context):
        self.log.info(f"Starting DuckDB transformation from {self.source_path}")
        
        # # Initialize DuckDB
        # con = duckdb.connect(database=':memory:')
        
        # try:
        #     # Read SQL transformation
        #     if not os.path.exists(self.sql_path):
        #         raise FileNotFoundError(f"SQL file not found: {self.sql_path}")
                
        #     with open(self.sql_path, 'r') as f:
        #         sql = f.read()
            
        #     self.log.info("Creating view of source data")
        #     # Create view of source data
        #     con.execute(f"""
        #         CREATE VIEW raw_commits AS 
        #         SELECT * FROM read_parquet('{self.source_path}');
        #     """)
            
        #     self.log.info("Executing transformation and writing results")
        #     # Execute transformation and write results
        #     con.execute(f"""
        #         COPY ({sql}) TO '{self.destination_path}' (FORMAT PARQUET);
        #     """)
            
        #     # Get row count for logging
        #     result = con.execute("SELECT COUNT(*) FROM raw_commits").fetchone()
        #     row_count = result[0] if result else 0
            
        #     self.log.info(f"Successfully transformed {row_count} rows to {self.destination_path}")
            
        # except Exception as e:
        #     self.log.error(f"Error during DuckDB transformation: {str(e)}")
        #     raise
        
        # finally:
        #     con.close()
        #     self.log.info("Closed DuckDB connection")
