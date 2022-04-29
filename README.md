# kafka-topic-monitor
A stand alone service that reads kafka topic offsets and reports metrics using 
metrics4j to your favorite time-series database (see https://github.com/kairosdb/metrics4j for details)

You can run multiple monitors against a single Kafka cluster for redundancy and fail over.  The Kafka Topic Montior
will divide the work between each instance by using internal kafka topics.  This provides a reliable platform
for alerts to detect when consumers are having problems.

### Metrics that this service will report
For metric names and configuring the destination please see the metrics4j.conf file
  * Topic producer count - count of events for each topic being produced.
  * Topic/group consumer count - count of events consumed for each group/topic tuple.
  * Group/topic/partition lag - difference between the offset and the head of the topic for each group/topic/patition tuple.
  * Group/topic lag summary - same as above but a summary across all partitions (faster dashboard loading when you have a lot of partitions).
  * Group time to process - A rough calculation based on the consumer rate for the group and the amount of lag.
  * Offset age - time in milliseconds since the offset has been updated (data used to alert when a consumer has stopped functioning)
  * Stale offset age - Similar to offset age but is only reported when the partition also has a lag.
  
## Testing
This service uses Jooby as the service framework so the easiest way to run it is 
the following command line
>mvn jooby:run

## Deployment
The project is packaged with stork.  Calling `mvn package` will create a complete
install directory under target/stork

## Configuration
All configuration is done in application.conf see the comments in the file for details
