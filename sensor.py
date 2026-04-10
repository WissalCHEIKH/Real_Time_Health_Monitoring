import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# -----------------------------
# Kafka configuration
# -----------------------------
TOPIC_NAME = "health_vitals"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

# -----------------------------
# Patients simulation
# -----------------------------
PATIENTS = [1, 2, 3, 4, 5]

def generate_vitals(patient_id):

    heart_rate = random.randint(60, 130)            # bpm
    temperature = round(random.uniform(36.0, 39.5), 1)  # °C
    oxygen_saturation = random.randint(90, 100)     # %
    respiratory_rate = random.randint(12, 30)       # breaths/min

    return {
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "temperature": temperature,
        "oxygen_saturation": oxygen_saturation,
        "respiratory_rate": respiratory_rate,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# -----------------------------
# Streaming loop
# -----------------------------
if __name__ == "__main__":
    print("🩺 Starting Real-Time Health Sensors...\n")

    while True:
        for patient in PATIENTS:

            vitals = generate_vitals(patient)

            producer.send(TOPIC_NAME, vitals)

            print("Sent:", json.dumps(vitals))

        producer.flush()

        time.sleep(1)