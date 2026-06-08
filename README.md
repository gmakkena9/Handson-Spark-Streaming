# Ride Sharing Analytics Using Spark Streaming and Spark SQL

## Student Information
- GitHub Username: gmakkena9
- Course: ITCS6190 - Summer 2026
- Repository: https://github.com/gmakkena9/Handson-Spark-Streaming

## Project Overview
This project builds a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. The pipeline ingests live ride data from a socket stream, parses it into structured columns, performs real-time aggregations at the driver level, and analyzes fare trends over time using sliding time windows.

## Project Structure
- task1.py: Basic streaming ingestion and parsing
- task2.py: Real-time driver-level aggregations  
- task3.py: Windowed time-based analytics
- data_generator.py: Simulates live ride data over socket on localhost:9999
- outputs/task_1/: CSV files with parsed ride records
- outputs/task_2/: CSV files with driver aggregations per batch
- outputs/task_3/: CSV files with windowed fare aggregations per batch
  
Handson-Spark-Streaming/
├── outputs/
│   ├── task_1/
│   │   └── part-00000.csv           <- Parsed streaming ride data
│   ├── task_2/
│   │   ├── batch_0/
│   │   │   └── part-00000.csv       <- Driver aggregations batch 0
│   │   └── batch_1/
│   │       └── part-00000.csv       <- Driver aggregations batch 1
│   └── task_3/
│       ├── batch_0/
│       │   └── part-00000.csv       <- Windowed aggregations batch 0
│       └── batch_1/
│           └── part-00000.csv       <- Windowed aggregations batch 1
├── task1.py                         <- Basic streaming ingestion and parsing
├── task2.py                         <- Real-time driver-level aggregations
├── task3.py                         <- Windowed time-based analytics
├── data_generator.py                <- Simulates live ride data over socket
└── README.md                        <- Project documentation

## Prerequisites
1. Python 3.x - https://www.python.org/downloads/
2. PySpark: pip install pyspark
3. Faker: pip install faker
4. Java JDK 11 - https://adoptium.net

## Data Generator
Simulates a real-time ride-sharing stream by opening a TCP socket on localhost:9999 and sending one JSON ride event per second. Each event contains trip_id, driver_id, distance_km, fare_amount, and timestamp.

## Task 1 - Basic Streaming Ingestion and Parsing

### Objective
Ingest live JSON ride data from a socket stream, parse into structured Spark DataFrame, save to CSV.

### Steps Performed
1. Created SparkSession with app name RideSharingAnalytics
2. Defined schema with StructType: trip_id and driver_id as StringType, distance_km and fare_amount as DoubleType, timestamp as StringType
3. Connected to localhost:9999 using spark.readStream.format("socket") - each row is one raw JSON line
4. Parsed JSON using from_json() with defined schema, used alias("data") and .select("data.*") to expand into columns
5. Used foreachBatch with outputMode("append") to save each micro-batch as CSV to outputs/task_1/

### Sample Output
trip_id,driver_id,distance_km,fare_amount,timestamp
a1b2c3d4-e5f6-7890-abcd-ef1234567890,42,12.5,35.75,2026-06-08 10:00:01
b2c3d4e5-f6a7-8901-bcde-f12345678901,17,8.3,22.10,2026-06-08 10:00:02

## Task 2 - Real-Time Aggregations (Driver-Level)

### Objective
Compute total fare and average distance per driver in real time, save to CSV per micro-batch.

### Steps Performed
1. Created SparkSession with app name RideSharingAggregations
2. Reused same schema and socket connection from Task 1
3. Parsed incoming JSON into structured columns using from_json()
4. Grouped by driver_id and computed two aggregations:
   - SUM(fare_amount) as total_fare: total money earned by each driver
   - AVG(distance_km) as avg_distance: average trip distance per driver
5. Used foreachBatch with outputMode("complete") - complete mode required for aggregations
6. Each micro-batch writes full updated aggregation to outputs/task_2/batch_{id}/

### Sample Output
driver_id,total_fare,avg_distance
42,167.15,19.73
17,63.30,11.55
63,120.70,21.85

## Task 3 - Windowed Time-Based Analytics

### Objective
Analyze fare trends using a 5-minute sliding window (sliding every 1 minute) with 1-minute watermark for late data handling.

### Steps Performed
1. Created SparkSession with app name RideSharingWindowed
2. Reused same schema and socket connection from previous tasks
3. Parsed JSON into structured columns using from_json()
4. Converted timestamp String to TimestampType using to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss") creating new column event_time - required for windowing operations
5. Applied withWatermark("event_time", "1 minute") to handle late-arriving events up to 1 minute late
6. Used window() function with 5-minute duration sliding every 1 minute, computed SUM(fare_amount) as total_fare
7. Extracted window.start and window.end as separate columns window_start and window_end
8. Used foreachBatch with outputMode("append") saving new windows to outputs/task_3/batch_{id}/

### Sample Output
window_start,window_end,total_fare
2026-06-08 09:56:00,2026-06-08 10:01:00,491.55
2026-06-08 09:57:00,2026-06-08 10:02:00,438.20

## How to Run

Terminal 1 - Start data generator (keep running the whole time):
python data_generator.py

Terminal 2 - Run Task 1 
python task1.py

Terminal 3 - Run Task 2 
python task2.py

Terminal 4 - Run Task 3 
python task3.py

## Submission Checklist
- [x] Python scripts (task1.py, task2.py, task3.py)
- [x] Output files in the outputs/ directory
- [x] Completed README.md
- [x] Committed everything to GitHub
- [x] Submitted GitHub repo link on Canvas

## Repository
GitHub: https://github.com/gmakkena9/Handson-Spark-Streaming
