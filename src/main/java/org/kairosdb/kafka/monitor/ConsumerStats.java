package org.kairosdb.kafka.monitor;

import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface ConsumerStats
{
	LongCollector gatherFailure();
	LongCollector consumeCount(@Key("group") String group, @Key("proxy_group") String proxyGroup, @Key("topic") String topic);
	LongCollector offsetAge(@Key("group") String group, @Key("proxy_group") String proxyGroup, @Key("topic") String topic, @Key("partition") String partition);
	LongCollector partitionLag(@Key("group") String group, @Key("proxy_group") String proxyGroup, @Key("topic") String topic, @Key("partition") String partition);
	LongCollector groupLag(@Key("group") String group, @Key("proxy_group") String proxyGroup, @Key("topic") String topic);
	LongCollector groupTimeToProcess(@Key("group") String group, @Key("proxy_group") String proxyGroup, @Key("topic") String topic);
	LongCollector produceCount(@Key("topic") String topic);
	LongCollector offsetGatherTime();
	LongCollector stalePartitions(@Key("group") String group, @Key("proxy_group") String proxyGroup, @Key("topic") String topic);
}
