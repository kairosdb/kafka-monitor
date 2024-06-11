package org.kairosdb.kafka.monitor;

import org.kairosdb.metrics4j.MetricsContext;
import org.kairosdb.metrics4j.triggers.MetricCollection;
import org.kairosdb.metrics4j.triggers.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class MetricsTrigger implements Trigger
{
	private static final Logger log = LoggerFactory.getLogger(MetricsTrigger.class);
	private MetricCollection m_collection;

	@Override
	public void setMetricCollection(MetricCollection collection)
	{
		m_collection = collection;
	}

	@Override
	public void init(MetricsContext context)
	{

	}

	public void reportMetrics()
	{
		try
		{
			m_collection.reportMetrics(Instant.now());
		}
		catch (Throwable t)
		{
			log.error("Error while trying to send metrics", t);
		}
	}
}
