import uuid
import random
import time
import os
from decimal import Decimal
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

# --- Функції для створення схеми ---
def create_schema(session):
    """Створює keyspace і таблиці, якщо вони не існують."""
    print("Створення схеми (keyspace + таблиці)...")
    
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS bess_monitoring
    WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': 1
    };
    """)
    
    session.set_keyspace('bess_monitoring')
    
    # Таблиця 1
    session.execute("""
    CREATE TABLE IF NOT EXISTS readings_by_bess (
        bess_id uuid,
        timestamp timestamp,
        soc decimal,
        power decimal,
        temperature decimal,
        PRIMARY KEY (bess_id, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """)
    
    # Таблиця 2
    session.execute("""
    CREATE TABLE IF NOT EXISTS mode_history_by_bess (
        bess_id uuid,
        timestamp timestamp,
        mode text,
        PRIMARY KEY (bess_id, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """)
    
    # Таблиця 3
    session.execute("""
    CREATE TABLE IF NOT EXISTS daily_balance (
        bess_id uuid,
        date date,
        energy_charged_kwh decimal,
        energy_discharged_kwh decimal,
        avg_soc decimal,
        PRIMARY KEY ((bess_id, date))
    );
    """)
    
    # Таблиця 4
    session.execute("""
    CREATE TABLE IF NOT EXISTS analytics_by_mode (
        mode text,
        timestamp timestamp,
        bess_id uuid,
        power_avg decimal,
        duration_minutes int,
        PRIMARY KEY (mode, timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """)
    print("Схему успішно створено/перевірено.")

# --- Основна логіка ---
def main():
    cluster = None
    session = None
    # Configurable settings via environment variables
    cassandra_host = os.getenv("CASSANDRA_HOST", "cassandra")
    max_retries = int(os.getenv("CASSANDRA_MAX_RETRIES", "10"))
    base_delay_sec = float(os.getenv("CASSANDRA_RETRY_DELAY_SEC", "2"))

    print(f"Спроба підключення до Cassandra ('{cassandra_host}:9042')...")
    retries = max_retries
    attempt = 0
    while retries > 0:
        attempt += 1
        try:
            cluster = Cluster([cassandra_host])
            session = cluster.connect()
            print("Підключення до Cassandra успішне!")
            break
        except Exception as e:
            sleep_for = min(base_delay_sec * (2 ** (attempt - 1)), 30)
            print(f"Помилка підключення: {e}. Повторна спроба через {sleep_for:.1f} сек...")
            retries -= 1
            time.sleep(sleep_for)

    if not session:
        print("Не вдалося підключитися до Cassandra. Вихід.")
        return

    # 2. Створення схеми (Keyspace + Таблиці)
    # *** КЛЮЧОВА ЗМІНА 2: Автоматичне створення схеми ***
    create_schema(session)

    # Prepared statements created once
    query_readings = session.prepare(
        "INSERT INTO readings_by_bess (bess_id, timestamp, soc, power, temperature) VALUES (?, ?, ?, ?, ?)"
    )
    query_modes = session.prepare(
        "INSERT INTO mode_history_by_bess (bess_id, timestamp, mode) VALUES (?, ?, ?)"
    )
    query_daily = session.prepare(
        "INSERT INTO daily_balance (bess_id, date, energy_charged_kwh, energy_discharged_kwh, avg_soc) VALUES (?, ?, ?, ?, ?)"
    )
    query_analytics = session.prepare(
        "INSERT INTO analytics_by_mode (mode, timestamp, bess_id, power_avg, duration_minutes) VALUES (?, ?, ?, ?, ?)"
    )

    print("Генерація тестових даних...")
    bess_ids = [uuid.uuid4() for _ in range(8)]
    start_time = datetime.now() - timedelta(days=1)
    total_readings = 0

    for bess_id in bess_ids:
        current_mode = 'standby'
        energy_charged = Decimal("0")
        energy_discharged = Decimal("0")

        for i in range(24):
            timestamp = start_time + timedelta(hours=i)
            soc_val = round(random.uniform(20.0, 90.0), 2)
            temp_val = round(random.uniform(15.0, 35.0), 2)

            # Convert to Decimal for Cassandra decimal columns
            soc = Decimal(str(soc_val))
            temp = Decimal(str(temp_val))

            if soc < Decimal("30"):
                current_mode = 'charge'
                power_val = round(random.uniform(100.0, 500.0), 2)
                power = Decimal(str(power_val))
                energy_charged += power
            elif soc > Decimal("85"):
                current_mode = 'discharge'
                power_val = -round(random.uniform(100.0, 500.0), 2)
                power = Decimal(str(power_val))
                energy_discharged += abs(power)
            else:
                current_mode = 'standby'
                power = Decimal("0")

            session.execute(query_readings, (bess_id, timestamp, soc, power, temp))

            if i == 0 or random.random() > 0.7:
                session.execute(query_modes, (bess_id, timestamp, current_mode))
                session.execute(query_analytics, (current_mode, timestamp, bess_id, abs(power), 60))

            total_readings += 1

        # avg_soc is a synthetic value; keep consistent Decimal
        session.execute(
            query_daily,
            (bess_id, start_time.date(), energy_charged, energy_discharged, Decimal("55.0"))
        )

    print(f"Дані успішно згенеровано. Всього записів (readings): {total_readings}")

    # 4. Базовий аналіз (Підваріант В)
    print("\n--- Динамічний аналіз (Підваріант В) ---")
    
    target_bess_id = bess_ids[0]
    query = session.prepare(
        "SELECT timestamp, power FROM readings_by_bess WHERE bess_id = ? AND timestamp >= ? AND timestamp <= ?"
    )
    
    day_start = start_time
    day_end = start_time + timedelta(days=1)
    
    try:
        rows = session.execute(query, [target_bess_id, day_start, day_end])
        
        min_power = (None, float('inf'))
        max_power = (None, float('-inf'))
        
        print(f"Динаміка потужності для BESS ID: {target_bess_id}")
        
        records = list(rows)
        
        if not records:
            print("Немає даних для аналізу.")
        else:
            for row in sorted(records, key=lambda r: r.timestamp):
                print(f"  {row.timestamp.strftime('%Y-%m-%d %H:%M')} -> Потужність: {row.power:.2f} кВт")
                
                if row.power < min_power[1]:
                    min_power = (row.timestamp, row.power)
                if row.power > max_power[1]:
                    max_power = (row.timestamp, row.power)

            print("\nВисновок (Підваріант В):")
            print(f"Максимальна потужність (зарядка) була {max_power[1]:.2f} кВт о {max_power[0].strftime('%H:%M')}.")
            print(f"Мінімальна потужність (розрядка) була {min_power[1]:.2f} кВт о {min_power[0].strftime('%H:%M')}.")
    except Exception as e:
        print(f"Помилка під час аналізу: {e}")
    finally:
        if cluster:
            cluster.shutdown()
        print("\nРоботу завершено. Контейнер Python зупиниться.")

if __name__ == "__main__":
    main()