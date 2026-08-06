from kafka import KafkaConsumer
import json
import csv
import os

consumer = KafkaConsumer(
    "nifty-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

csv_file = "stocks_live_data.csv"

if not os.path.exists(csv_file):
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date",
            "time",
            "symbol",
            "price",
            "open",
            "high",
            "low",
            "volume"
        ])

print("Consumer Started...\n")

for message in consumer:

    data = message.value

    print(data)

    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            data["date"],
            data["time"],
            data["symbol"],
            data["price"],
            data["open"],
            data["high"],
            data["low"],
            data["volume"]
        ])
