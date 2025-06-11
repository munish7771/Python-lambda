import pyodbc
import time
from pathlib import Path
import csv
from datetime import datetime

# ======== CONFIGURATION ========
DATABRICKS_CONN = {
    "DRIVER": "{Simba Spark ODBC Driver}",
    "HOST": "dbc-xxxx.cloud.databricks.com",
    "PORT": "443",
    "HTTPPath": "/sql/1.0/warehouses/xxxx",
    "UID": "token",
    "PWD": "dapiXXXXXXXXXXXXXXXXXXXXXXXX",
}

QUERIES_DIR = Path("queries")
QUERY_FILES = ["q1.sql", "q2.sql", "q3.sql"]
OUTPUT_CSV = "query_performance_results.csv"

# ======== CONNECTION STRING ========
def get_connection_string():
    return (
        f"DRIVER={DATABRICKS_CONN['DRIVER']};"
        f"HOST={DATABRICKS_CONN['HOST']};"
        f"PORT={DATABRICKS_CONN['PORT']};"
        f"SparkServerType=3;"
        f"AuthMech=3;"
        f"UID={DATABRICKS_CONN['UID']};"
        f"PWD={DATABRICKS_CONN['PWD']};"
        f"HTTPPath={DATABRICKS_CONN['HTTPPath']};"
        f"SSL=1;"
    )

# ======== RUN QUERY ========
def run_query(query_file):
    sql = (QUERIES_DIR / query_file).read_text()
    conn = pyodbc.connect(get_connection_string(), autocommit=True)
    cursor = conn.cursor()
    
    start = time.time()
    cursor.execute(sql)
    cursor.fetchall()
    end = time.time()
    
    cursor.close()
    conn.close()
    
    return round(end - start, 3)

# ======== MAIN DRIVER ========
def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []

    print("🔵 COLD START TEST (run when warehouse has been idle)...")
    for q in QUERY_FILES:
        t = run_query(q)
        print(f"  ❄️  {q}: {t:.3f} seconds")
        results.append({
            "query": q,
            "scenario": "cold_start",
            "execution_time_sec": t,
            "timestamp": timestamp
        })

    input("\n✅ Now run WARM CACHE TEST (warehouse still running). Press Enter to continue...")

    print("🟢 WARM CACHE TEST")
    for q in QUERY_FILES:
        t = run_query(q)
        print(f"  🔁  {q}: {t:.3f} seconds")
        results.append({
            "query": q,
            "scenario": "warm_cache_hit",
            "execution_time_sec": t,
            "timestamp": timestamp
        })

    # Write to CSV
    with open(OUTPUT_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "query", "scenario", "execution_time_sec"])
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(results)

    print(f"\n📄 Results saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
