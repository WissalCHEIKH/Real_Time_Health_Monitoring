import time
import json
import random
from datetime import datetime

PATIENTS = [1, 2, 3, 4, 5]

def generate_vitals(patient_id):
    heart_rate = random.randint(60, 130)
    temperature = round(random.uniform(36.0, 39.5), 1)

    return {
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "temperature": temperature,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

if __name__ == "__main__":
    print("Starting Realistic Health Sensors...\n")

    while True:
        for patient in PATIENTS:
            vitals = generate_vitals(patient)
            print(json.dumps(vitals))

        time.sleep(1)
