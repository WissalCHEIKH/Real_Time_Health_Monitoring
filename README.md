# 🏥 Real-Time Health Monitoring System

## 📌 Project Overview
This project is a real-time data engineering pipeline designed to simulate and monitor patient health data.  
It processes streaming data from sensors and performs real-time analytics using a Big Data stack.

---

## ⚙️ Architecture

Sensor Data Generator → Kafka → Spark Streaming → Cassandra → Power BI Dashboard

---

## 🧰 Tech Stack

- Python (Data simulation & processing)
- Apache Kafka (Data streaming)
- Apache Spark Streaming (Real-time processing)
- Cassandra (NoSQL storage)
- Power BI (Visualization)
- Docker (Containerization)

---

## 🔄 Data Pipeline Workflow

1. **Sensor Module (`sensor.py`)**
   Generates real-time health metrics (heart rate, temperature, oxygen level, etc.)

2. **Kafka Producer**
   Sends streaming data to Kafka topic (`health_vitals`)

3. **Spark Streaming (`spark_streaming.py`)**
   Consumes Kafka stream and processes data in real-time

4. **Cassandra Database**
   Stores processed data for persistence and analysis

5. **Power BI Dashboard**
   Visualizes real-time health insights

---

## 🚀 How to Run the Project

### 1. Start Infrastructure (Kafka + Cassandra)

```bash
docker-compose up -d
```

### Check running containers:
docker ps

### Kafka consumer
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic health_vitals

### 2. Activate Python Environment
.\venv\Scripts\activate

### 3. Run Sensor Data Generator
python sensor.py

### 4. Run Spark Streaming Job
python spark_streaming.py

### Cassandra Setup
### Access Cassandra Shell
docker exec -it cassandra cqlsh

### Create Keyspace
CREATE KEYSPACE health_monitoring
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

### Use Keyspace
USE health_monitoring;

### Create Table
CREATE TABLE vitals (
    patient_id UUID PRIMARY KEY,
    heart_rate int,
    temperature float,
    oxygen_level int,
    timestamp timestamp
);

### Verify Data
SELECT * FROM vitals;

👨‍💻 Author

Wissal CHEIKH
Big Data and Information Systems Student

