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
