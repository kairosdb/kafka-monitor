package org.kairosdb.kafka.monitor;

import org.kairosdb.metrics4j.collectors.LongCollector;

public interface MonitorStats
{
	LongCollector rawOffsetsRead();
	LongCollector partitionedOffsetsRead();
}
