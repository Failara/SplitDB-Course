import time
import numpy as np
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel

# Constants
KEYSPACE = "bess_lab"
NODE_IPS = ['cassandra-seed']
DEVICE_ID = "BESS_001"
TEST_TIME = datetime(2025, 1, 1, 14, 0, 0)
NUM_ITERATIONS = 50

class BenchmarkRunner:
    def __init__(self, session):
        self.session = session

    def run_query(self, query, params, cl=ConsistencyLevel.LOCAL_QUORUM):
        stmt = SimpleStatement(query, consistency_level=cl)
        latencies = []
        for _ in range(NUM_ITERATIONS):
            start = time.time()
            self.session.execute(stmt, params)
            end = time.time()
            latencies.append((end - start) * 1000)
        return {
            "avg": np.mean(latencies),
            "p50": np.percentile(latencies, 50),
            "p95": np.percentile(latencies, 95),
            "p99": np.percentile(latencies, 99)
        }

def get_session():
    cluster = Cluster(NODE_IPS)
    session = cluster.connect()
    session.execute(f"USE {KEYSPACE}")
    return cluster, session

def print_results(title, results):
    print(f"\n--- {title} ---")
    print(f"  Avg: {results['avg']:.2f} ms")
    print(f"  p50: {results['p50']:.2f} ms")
    print(f"  p95: {results['p95']:.2f} ms")
    print(f"  p99: {results['p99']:.2f} ms")

def main():
    cluster, session = get_session()
    try:
        runner = BenchmarkRunner(session)
        
        start_6hr = TEST_TIME - timedelta(hours=6)
        start_1hr = TEST_TIME - timedelta(hours=1)
        test_date = TEST_TIME.date()
        test_minute_bucket = TEST_TIME.replace(second=0, microsecond=0)

        print("="*50)
        print(f"Тестування запитів для пристрою: {DEVICE_ID}")
        print(f"Базовий час: {TEST_TIME}")
        print("="*50)

        # 1-hour queries
        queries_1hr = [
            ("Schema 1 (Simple) - 1 Hour Range", 
             "SELECT * FROM telemetry_simple WHERE device_id = %s AND timestamp >= %s AND timestamp <= %s",
             (DEVICE_ID, start_1hr, TEST_TIME)),
            ("Schema 3 (Daily) - 1 Hour Range",
             "SELECT * FROM telemetry_daily WHERE device_id = %s AND bucket_date = %s AND timestamp >= %s AND timestamp <= %s",
             (DEVICE_ID, test_date, start_1hr, TEST_TIME)),
        ]
        
        for title, query, params in queries_1hr:
            res = runner.run_query(query, params)
            print_results(title, res)

        # Minute bucket query
        res = runner.run_query(
            "SELECT * FROM telemetry_minute WHERE device_id = %s AND bucket_minute = %s",
            (DEVICE_ID, test_minute_bucket)
        )
        print_results("Schema 2 (Minute) - Latest Minute (1 Partition)", res)

        # 6-hour queries
        queries_6hr = [
            ("Schema 1 (Simple) - 6 Hour Range",
             "SELECT * FROM telemetry_simple WHERE device_id = %s AND timestamp >= %s AND timestamp <= %s",
             (DEVICE_ID, start_6hr, TEST_TIME)),
            ("Schema 3 (Daily) - 6 Hour Range",
             "SELECT * FROM telemetry_daily WHERE device_id = %s AND bucket_date = %s AND timestamp >= %s AND timestamp <= %s",
             (DEVICE_ID, test_date, start_6hr, TEST_TIME)),
        ]
        
        for title, query, params in queries_6hr:
            res = runner.run_query(query, params)
            print_results(title, res)

        print("\n--- Schema 2 (Minute) - 6 Hour Range --- \n  (Пропущено - вимагає 360 окремих запитів, поганий патерн)")

        # Aggregates query
        res = runner.run_query(
            "SELECT * FROM aggregates_hourly WHERE device_id = %s",
            (DEVICE_ID,)
        )
        print_results("Schema 3 (Aggregates) - Full Day (Читання 24 рядків)", res)

        # Materialized view vs ALLOW FILTERING comparison
        res_mv = runner.run_query(
            "SELECT * FROM frequency_alerts WHERE device_id = %s AND bucket_minute = %s AND grid_frequency > 50.1",
            (DEVICE_ID, test_minute_bucket)
        )
        print_results("Query 4 (With Materialized View) - Freq > 50.1", res_mv)
        
        res_filter = runner.run_query(
            "SELECT * FROM telemetry_minute WHERE device_id = %s AND bucket_minute = %s AND grid_frequency > 50.1 ALLOW FILTERING",
            (DEVICE_ID, test_minute_bucket)
        )
        print_results("Query 4 (ALLOW FILTERING) - Freq > 50.1", res_filter)
        
        print(f"\n*** Прискорення завдяки MV: {res_filter['avg'] / res_mv['avg']:.2f}x ***")

        # Consistency level comparison
        print("\n" + "="*50)
        print("Підваріант В: Тестування рівнів узгодженості")
        print("="*50)
        
        consistency_levels = [
            ("CL: ONE (Найшвидший, 1 вузол)", ConsistencyLevel.ONE),
            ("CL: LOCAL_QUORUM (Кворум в локальному ДЦ, dc1=2 вузли)", ConsistencyLevel.LOCAL_QUORUM),
            ("CL: EACH_QUORUM (Кворум в кожному ДЦ, dc1=2, dc2=1)", ConsistencyLevel.EACH_QUORUM),
        ]
        
        results_cl = {}
        for title, cl in consistency_levels:
            res = runner.run_query(
                "SELECT * FROM telemetry_minute WHERE device_id = %s AND bucket_minute = %s",
                (DEVICE_ID, test_minute_bucket),
                cl=cl
            )
            results_cl[title] = res
            print_results(title, res)

        res_one = results_cl["CL: ONE (Найшвидший, 1 вузол)"]
        res_eq = results_cl["CL: EACH_QUORUM (Кворум в кожному ДЦ, dc1=2, dc2=1)"]
        print(f"\n*** Вплив CL: EACH_QUORUM повільніше за ONE у {res_eq['avg'] / res_one['avg']:.2f}x разів ***")

    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()