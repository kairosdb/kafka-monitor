# kafka-monitor
Plugin that reads and reports kafka topic lag

### Metrics that this plugin will report
  * Topic producer rate - events per min for each topic being produced.
  * Topic/group consumer rate - events per min consuming rate for each group/topic tuple.
  * Group/topic/partition lag - difference between the offset and the head of the topic for each group/topic/patition tuple.
  * Group/topic lag summary - same as above but a summary across all partitions (faster dashboard loading when you have a lot of partitions).
  * Offset age - time in milliseconds since the offset has been updated (data used to alert when a consumer has stopped functioning)
  
## Deployment
This plugin can be loaded into Kairos to monitor Kafka offsets in a variety of ways:
  1. Load plugin on your existing Kairos nodes.
  1. Load plugin on a dedicated Kairos node that writes directly to C*.
  1. Load plugin on a dedicated Kairos node that uses the remote datastore to forward the 
  data to your existing Kairos ingest nodes.
  1. Create a custom datastore to forward the data to whatever you like.
