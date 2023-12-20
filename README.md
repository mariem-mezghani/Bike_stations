# Bike Logistics and Real-time Analysis Project
## Context
Welcome to the Bike Logistics and Real-time Analysis Project! This initiative establishes a real-time data visualization pipeline using cutting-edge Big Data technologies. The process involves collecting real-time data through Kafka, utilizing Spark for data analysis, and leveraging Elasticsearch and Hive for storage. Kibana serves as the platform for creating insightful dashboards. The Python-based code streamlines data processing and triggers alerts when a station becomes empty, offering specific details such as the address, city, and the count of empty stations in that area. Historical data is stored in a Hadoop DataLake, while Spark simultaneously registers key bike information in both the Elasticsearch index and Hive table. Kibana's dashboard features station locations on a map and a table displaying available bike counts. Happy biking analytics!

## Technologies Used
* Scala: 2.12.15
* Apache Spark: 3.2.4
* Apache Kafka: 3.6.0
* Apache hadoop: 2.10.2
* Apache Hive: 2.3.9
* Elasticsearch: 8.8.2
* kibana : 8.8.2

## Project Architecture
 
## Run Project
Before you begin, make sure to run Zookeeper and Kafka servers. To do so, navigate to the Kafka directory and execute the following commands:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
In this project, you have the option to store your data either in an Elasticsearch index or a Hive table.

### Store Historical Data in an Elasticsearch index
1. Start Elasticsearch:
Navigate to the Elasticsearch directory and execute the following command:
```
./bin/elasticsearch
```
2. Create Elasticsearch index
```
python3 create_index.py
```
3. Run the Kafka producer:
```
python3 producer.py
```
4. Run the consumer:
```
./execute_consumer.sh
```

### Store Historical Data in a Hive Table
1. Run the hadoop cluster:
Navigate to the Hadoop directory and execute the following command:
```
./sbin/start-all.sh
```
3. Run the Hive service metastore:
```
hive --service metastore
```
4. Run the Kafka producer:
```
python3 producer.py
```
5. Run the consumer:
```
./execute_consumer_hive.sh
```

## Visualize results
