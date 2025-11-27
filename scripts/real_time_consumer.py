import json
from kafka import KafkaConsumer

REALTIME_TOPIC = "bess-realtime-critical"

def main():
    print("🔌 Підключення до real-time топіка...")
    consumer = KafkaConsumer(
        REALTIME_TOPIC,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        group_id='realtime-frequency-monitor-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    print(f"✅ Моніторинг топіка '{REALTIME_TOPIC}'...")
    for message in consumer:
        data = message.value
        freq = data.get('grid_frequency', 50.0)
        
        print(f"RECEIVED REAL-TIME: {data['device_id']}, Freq: {freq} Hz", end="")
        
        if not (49.9 < freq < 50.1):
            print("  -> 🚨 ALERT! FREQUENCY OUT OF BOUNDS! 🚨")
        else:
            print("  -> OK")

if __name__ == "__main__":
    main()