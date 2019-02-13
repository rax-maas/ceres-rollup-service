# Rollup Service
This service aggregates raw data into multiple rolled-up topics. It consumes raw data from one topic and produces aggregated data into multiple topics depending on what all aggregation levels are configured.

## Setup
Install docker. Once done with that, you can use [`test-infrastructure`](https://github.com/racker/ceres-test-infrastructure) repository to install and run `Kafka`, `InfluxDB` and `Redis`. Please follow instruction from that repository to install them. Rollup service needs `Kafka` only. <br />
**WORK-IN-PROGRESS** <br />
Ideally we should have all of these components in `docker-compose`, but for now, we might have to follow a little manual process. <br />
To run or test Rollup Service locally:
- Get repo `ingestion-service-functional-test` and after building it. 
  - Go to `ingestion-service-functional-test` folder locally
  - Run `java -jar target/kafka-influxdb-functional-test-0.0.1-SNAPSHOT.jar` This will create raw test data into Kafka.
  <br />
  Now you can run rollup-service using IntelliJ to do your development work.
