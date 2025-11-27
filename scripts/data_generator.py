import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Топіки для гібридного навантаження (Підваріант C)
REALTIME_TOPIC = "bess-realtime-critical" # Для моніторингу частоти
ANALYTICS_TOPIC = "bess-analytics-soc"    # Для аналітики заряду батарей

def create_producer():
    """Створюємо Kafka producer"""
    print("🔌 Підключення до Kafka як Producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5
        )
        print("✅ Producer готовий до роботи!")
        return producer
    except Exception as e:
        print(f"❌ Помилка підключення Producer: {e}")
        return None

def generate_bess_data(device_id):
    """Генерує дані для однієї системи накопичення енергії (BESS)"""
    # 70% часу система працює в режимі регулювання частоти (real-time)
    # 30% - в інших режимах (аналітика)
    is_realtime_mode = random.random() >= 0.7 
    
    if is_realtime_mode:
        mode = "frequency_regulation"
        grid_frequency = round(random.uniform(49.95, 50.05), 3)
        power_output = round(random.uniform(-15000.0, 15000.0), 2)
    else:
        mode = random.choice(["peak_shaving", "arbitrage", "backup"])
        grid_frequency = 50.0
        power_output = random.choice([-50000.0, 50000.0])

    data = {
        "device_id": f"BESS_{device_id:03d}",
        "timestamp": datetime.now().isoformat(),
        "power_output": power_output,
        "efficiency": round(random.uniform(88.0, 94.5), 2),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "voltage": round(random.uniform(850.0, 1450.0), 2),
        "current": round(abs(power_output / 1000), 2),
        "status": random.choice(["charging", "discharging", "idle"]),
        "location": {"lat": round(random.uniform(46.0, 52.0), 4), "lon": round(random.uniform(22.0, 40.0), 4)},
        "maintenance_hours": random.randint(500, 15000),

        "soc": round(random.uniform(15.0, 85.0), 2),
        "grid_frequency": grid_frequency,
        "mode": mode
    }
    return data, is_realtime_mode

def main():
    producer = create_producer()
    if not producer:
        return

    print("🚀 Починаємо відправку гібридного потоку даних BESS...")
    msg_count = 0
    try:
        while True:
            device_num = random.randint(1, 8)
            bess_data, is_realtime = generate_bess_data(device_num)
            
            if is_realtime:
                topic = REALTIME_TOPIC
                print(f"SENDING TO REAL-TIME -> {bess_data['device_id']}: Freq={bess_data['grid_frequency']} Hz")
            else:
                topic = ANALYTICS_TOPIC
                print(f"SENDING TO ANALYTICS -> {bess_data['device_id']}: SoC={bess_data['soc']}%")
                
            producer.send(topic, value=bess_data)
            producer.flush()
            
            msg_count += 1
            time.sleep(5) 
            
    except KeyboardInterrupt:
        print(f"\n🛑 Зупинено. Всього відправлено {msg_count} повідомлень.")
    finally:
        producer.close()
        print("🔌 З'єднання з Kafka закрито.")

if __name__ == "__main__":
    main()