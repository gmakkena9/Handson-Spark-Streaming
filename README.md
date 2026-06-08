# Ride Sharing Analytics Using Spark Streaming and Spark SQL

## Student Information
- Name: Gopi Bharath Makkena
- GitHub: gmakkena9
- Course: ITCS6190 - Summer 2026
- Repo: https://github.com/gmakkena9/Handson-Spark-Streaming

---

## Prerequisites
- Python 3.x
- PySpark: pip install pyspark
- Faker: pip install faker
- Java JDK 11: https://adoptium.net

---

## Project Structure

    Handson-Spark-Streaming/
    |
    |-- task1.py               (Basic streaming ingestion and parsing)
    |-- task2.py               (Real-time driver-level aggregations)
    |-- task3.py               (Windowed time-based analytics)
    |-- data_generator.py      (Simulates live ride data over socket)
    |-- README.md              (Project documentation)
    |
    |-- outputs/
        |-- task_1/
        |   |-- part-00000.csv       (Parsed streaming ride data)
        |-- task_2/
        |   |-- batch_0/
        |   |   |-- part-00000.csv   (Driver aggregations batch 0)
        |   |-- batch_1/
        |       |-- part-00000.csv   (Driver aggregations batch 1)
        |-- task_3/
            |-- batch_0/
            |   |-- part-00000.csv   (Windowed aggregations batch 0)
            |-- batch_1/
                |-- part-00000.csv   (Windowed aggregations batch 1)

---

## Data Generator
- Streams live ride events over TCP socket on localhost:9999
- Sends 1 JSON event per second
- Each event contains: trip_id, driver_id, distance_km, fare_amount, timestamp

---

## Task 1 - Streaming Ingestion and Parsing

### What it does
Reads JSON ride data from socket, parses into structured columns, saves to CSV.

### How it works
- Created SparkSession with app name RideSharingAnalytics
- Defined schema: trip_id, driver_id (String), distance_km, fare_amount (Double), timestamp (String)
- Read socket stream using spark.readStream.format("socket") on localhost:9999
- Parsed JSON using from_json() with defined schema
- Saved each micro-batch to outputs/task_1/ using foreachBatch with outputMode append

### Sample Output

    trip_id,driver_id,distance_km,fare_amount,timestamp
    a1b2c3d4,42,12.5,35.75,2026-06-08 10:00:01
    b2c3d4e5,17,8.3,22.10,2026-06-08 10:00:02
    c3d4e5f6,63,25.1,68.40,2026-06-08 10:00:03

---

## Task 2 - Real-Time Driver Aggregations

### What it does
Groups rides by driver_id and computes total fare and average distance per driver in real time.

### How it works
- Reused schema and socket connection from Task 1
- Parsed JSON into structured columns
- Grouped by driver_id and computed SUM(fare_amount) as total_fare and AVG(distance_km) as avg_distance
- Used outputMode complete since aggregation requires full result each batch
- Saved each batch to outputs/task_2/batch_{id}/ using foreachBatch

### Sample Output

    driver_id,total_fare,avg_distance
    42,167.15,19.73
    17,63.30,11.55
    63,120.70,21.85

---

## Task 3 - Windowed Time-Based Analytics

### What it does
Analyzes fare trends using 5-minute sliding windows sliding every 1 minute with 1-minute watermark for late data.

### How it works
- Parsed JSON same as previous tasks
- Converted timestamp String to TimestampType using to_timestamp() as column event_time
- Applied withWatermark("event_time", "1 minute") to handle late arriving events
- Used window() function with 5-minute window sliding every 1 minute
- Computed SUM(fare_amount) as total_fare per window
- Extracted window.start and window.end as separate columns
- Saved finalized windows to outputs/task_3/batch_{id}/ using outputMode append

### Sample Output

    window_start,window_end,total_fare
    2026-06-08 09:56:00,2026-06-08 10:01:00,491.55
    2026-06-08 09:57:00,2026-06-08 10:02:00,438.20
    2026-06-08 09:58:00,2026-06-08 10:03:00,512.80

---

## How to Run

    Terminal 1 - Start data generator (keep running the whole time):
    python data_generator.py

    Terminal 2 - Run Task 1 
    python task1.py

    Terminal 3 - Run Task 2 
    python task2.py

    Terminal 4 - Run Task 3 
    python task3.py

---

## Submission Checklist
- [x] task1.py, task2.py, task3.py completed
- [x] Output CSV files in outputs/ directory
- [x] README.md completed
- [x] Pushed to GitHub
- [x] Submitted on Canvas
