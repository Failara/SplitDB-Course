import time
import random
import numpy as np
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
from tqdm import tqdm

# Constants
KEYSPACE = "bess_lab"
NODE_IPS = ['cassandra-seed', 'cassandra-dc1-node2', 'cassandra-dc2-node1']
TOTAL_RECORDS = 100_000
DEVICES = [f"BESS_{i:03d}" for i in range(1, 9)]
START_TIME = datetime(2025, 1, 1, 0, 0, 0)
DATA_FREQUENCY_SEC = 1
BATCH_SIZE = 100
MODE_FREQUENCY_PROBABILITY = 0.8
SOC_BASE = 50
SOC_AMPLITUDE = 40
SOC_NOISE_RANGE = 2
GRID_FREQUENCY_NORMAL = 50.0
GRID_FREQUENCY_RANGE = 0.05
POWER_OUTPUT_PEAK = 50000
POWER_OUTPUT_REGULATION = 15000

def get_session():
    cluster = Cluster(NODE_IPS)
    session = cluster.connect()
    session.execute(f"USE {KEYSPACE}")
    return cluster, session

def generate_bess_data(timestamp):
    hour = timestamp.hour
    soc = SOC_BASE + SOC_AMPLITUDE * np.sin(np.pi * (hour - 8) / 12)
    soc = round(soc + random.uniform(-SOC_NOISE_RANGE, SOC_NOISE_RANGE), 2)
    
    if random.random() < MODE_FREQUENCY_PROBABILITY:
        mode = "frequency_regulation"
        grid_frequency = round(random.uniform(GRID_FREQUENCY_NORMAL - GRID_FREQUENCY_RANGE, 
                                             GRID_FREQUENCY_NORMAL + GRID_FREQUENCY_RANGE), 3)
        power_output = round(random.uniform(-POWER_OUTPUT_REGULATION, POWER_OUTPUT_REGULATION), 2)
    else:
        mode = random.choice(["arbitrage", "peak_shaving"])
        grid_frequency = GRID_FREQUENCY_NORMAL
        power_output = random.choice([-POWER_OUTPUT_PEAK, POWER_OUTPUT_PEAK])
    
    return {"soc": soc, "grid_frequency": grid_frequency, "mode": mode, "power_output": power_output}

def run_write_benchmark(session, schema_name, query_str, prep_func):
    print(f"\n--- Benchmarking Write: {schema_name} ---")
    prepared_query = session.prepare(query_str)
    records_per_device = TOTAL_RECORDS // len(DEVICES)
    
    start_time = time.time()
    total_inserted = 0
    
    with tqdm(total=TOTAL_RECORDS, desc=schema_name) as pbar:
        for device in DEVICES:
            current_time = START_TIME
            batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            records_in_batch = 0
            
            for _ in range(records_per_device):
                data = generate_bess_data(current_time)
                params = prep_func(device, current_time, data)
                batch.add(prepared_query, params)
                
                current_time += timedelta(seconds=DATA_FREQUENCY_SEC)
                records_in_batch += 1
                
                if records_in_batch >= BATCH_SIZE:
                    session.execute(batch)
                    batch.clear()
                    records_in_batch = 0
                    pbar.update(BATCH_SIZE)
                    total_inserted += BATCH_SIZE
            
            if records_in_batch > 0:
                session.execute(batch)
                pbar.update(records_in_batch)
                total_inserted += records_in_batch

    end_time = time.time()
    duration = end_time - start_time
    throughput = total_inserted / duration
    print(f"Done! {schema_name}: {throughput:.2f} writes/sec")
    return throughput

def prep_simple(device, time, d):
    return (device, time, d['soc'], d['grid_frequency'], d['mode'], d['power_output'])

def prep_minute(device, time, d):
    bucket = time.replace(second=0, microsecond=0)
    return (device, bucket, time, d['soc'], d['grid_frequency'], d['mode'], d['power_output'])

def prep_daily(device, time, d):
    bucket = time.date()
    return (device, bucket, time, d['soc'], d['grid_frequency'], d['mode'], d['power_output'])

def main():
    cluster, session = get_session()
    try:
        th_simple = run_write_benchmark(session, "Schema 1 (Simple)", 
            "INSERT INTO telemetry_simple (device_id, timestamp, soc, grid_frequency, mode, power_output) VALUES (?, ?, ?, ?, ?, ?)",
            prep_simple)

        th_minute = run_write_benchmark(session, "Schema 2 (Minute)",
            "INSERT INTO telemetry_minute (device_id, bucket_minute, timestamp, soc, grid_frequency, mode, power_output) VALUES (?, ?, ?, ?, ?, ?, ?)",
            prep_minute)

        th_daily = run_write_benchmark(session, "Schema 3 (Daily)",
            "INSERT INTO telemetry_daily (device_id, bucket_date, timestamp, soc, grid_frequency, mode, power_output) VALUES (?, ?, ?, ?, ?, ?, ?)",
            prep_daily)

        print("\n=== ПІДСУМКОВА ТАБЛИЦЯ WRITE THROUGHPUT ===")
        print(f"Schema 1 (Simple): {th_simple:.2f} writes/sec")
        print(f"Schema 2 (Minute): {th_minute:.2f} writes/sec")
        print(f"Schema 3 (Daily):  {th_daily:.2f} writes/sec")
    
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()