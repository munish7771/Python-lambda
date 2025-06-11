from locust import User, task, between
import pyodbc
import time
import csv
import threading
from datetime import datetime
from pathlib import Path
import itertools

# === GLOBAL CONFIG ===
DATABRICKS_HOST = "your-databricks-host"
DATABRICKS_HTTP_PATH = "your-http-path"
DATABRICKS_TOKEN = "dapiXXXXXXXXXXXXXXXXXXXXXXXX"

CSV_FILE = "locust_query_results.csv"
QUERY_DIR = Path("queries")  # folder with q1.sql, q2.sql, ...
QUERY_FILES = sorted(QUERY_DIR.glob("q*.sql"))
query_file_cycle = itertools.cycle(QUERY_FILES)  # rotate through query files for each user

csv_lock = threading.Lock()

class DatabricksUser(User):
    wait_time = between(1, 3)

    def on_start(self):
        # Assign one SQL file to this user
        self.query_file = next(query_file_cycle)
        self.query_text = self.query_file.read_text()

        # Setup ODBC connection
        self.conn = pyodbc.connect(
            f'DRIVER={{Simba Spark ODBC Driver}};'
            f'HOST={DATABRICKS_HOST};'
            'PORT=443;'
            'SparkServerType=3;'
            'Schema=default;'
            'AuthMech=3;'
            'UID=token;'
            f'PWD={DATABRICKS_TOKEN};'
            f'HTTPPath={DATABRICKS_HTTP_PATH}'
        )
        self.cursor = self.conn.cursor()

        # Write CSV header once (if file doesn't exist)
        with csv_lock:
            try:
                with open(CSV_FILE, "x", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp", "user_id", "query_file", "duration_sec", "row_count"])
            except FileExistsError:
                pass

    @task
    def run_query(self):
        start_time = time.time()
        try:
            self.cursor.execute(self.query_text)
            results = self.cursor.fetchall()
            duration = round(time.time() - start_time, 3)
            row_count = len(results)

            with csv_lock:
                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        datetime.utcnow().isoformat(),
                        id(self),
                        self.query_file.name,
                        duration,
                        row_count
                    ])
        except Exception as e:
            self.environment.events.request_failure.fire(
                request_type="SQL",
                name=str(self.query_file),
                response_time=int((time.time() - start_time) * 1000),
                exception=e,
            )

    def on_stop(self):
        self.cursor.close()
        self.conn.close()
