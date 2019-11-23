# Rollup Service
We are using Kafka to do aggregation (rollup) of raw data. This service aggregates raw data into multiple rolled-up topics. It consumes raw data from one topic and produces aggregated data into multiple topics depending on what all aggregation levels are configured.

## Setup
Install docker. Once done with that, you can use [`test-infrastructure`](https://github.com/racker/ceres-test-infrastructure) repository to install and run `Kafka`, `InfluxDB` and `Redis`. Please follow instruction from that repository to install them. Rollup service needs `Kafka` only. <br />

To run or test Rollup Service locally:
- Get repo [`test-data-generator`](https://github.com/racker/ceres-test-data-generator). 
  - Go to `test-data-generator` folder locally
  - Run `mvn clean install`
  - Run `java -jar target/test-data-generator-0.0.1-SNAPSHOT.jar` This will create raw test data into Kafka.
  <br />
  Now you can run rollup-service using IntelliJ to do your development work.
