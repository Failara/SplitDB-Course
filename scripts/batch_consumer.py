import json
from kafka import KafkaConsumer

ANALYTICS_TOPIC = "bess-analytics-soc"

def main():
    print("🔌 Підключення до аналітичного топіка...")
    consumer = KafkaConsumer(
        ANALYTICS_TOPIC,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        group_id='analytics-soc-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"✅ Очікування аналітичних даних з '{ANALYTICS_TOPIC}'...")
    soc_values = []
    for message in consumer:
        data = message.value
        soc = data.get('soc', 0)
        soc_values.append(soc)
        
        print(f"RECEIVED FOR ANALYTICS: {data['device_id']}, SoC: {soc}%, Mode: {data['mode']}")

        if len(soc_values) >= 5:
            avg_soc = sum(soc_values) / len(soc_values)
            print(f"\n📊 ANALYTICS BATCH: Average SoC for last 5 readings is {avg_soc:.2f}%\n")
            soc_values = []

if __name__ == "__main__":
    main()