package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class MonitorConfig
{
	@Inject
	@Named("kairosdb.kafka_monitor.application_id")
	private String m_applicationId = "kafka_monitor";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.client_id")
	private String m_clientId = null; //defaults to hostname if null

	@Inject
	@Named("kairosdb.kafka_monitor.bootstrap_servers")
	private String m_bootStrapServers;

	@Inject
	@Named("kairosdb.kafka_monitor.stream_state_directory")
	private String m_streamStateDirectory = "stream-state";

	@Inject
	@Named("kairosdb.kafka_monitor.topic_owner_topic_name")
	private String m_topicOwnerTopicName = "kafka_monitor_topic_owner";

	@Inject
	@Named("kairosdb.kafka_monitor.metric.prefix")
	private String m_metricPrefix = "kafka_monitor.";

	@Inject
	@Named("kairosdb.kafka_monitor.metric.consumer_rate")
	private String m_consumerRateMetric = "consumer_rate";

	@Inject
	@Named("kairosdb.kafka_monitor.metric.offset_age")
	private String m_offsetAgeMetric = "offset_age";

	@Inject
	@Named("kairosdb.kafka_monitor.metric.partition_lag")
	private String m_partitionLagMetric = "partition_lag";

	@Inject
	@Named("kairosdb.kafka_monitor.metric.group_lag")
	private String m_groupLagMetric = "group_lag";

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	public MonitorConfig()
	{
	}

	public String getApplicationId()
	{
		return m_applicationId;
	}

	public String getClientId()
	{
		if (m_clientId != null)
			return m_clientId;
		else
			return m_hostName;
	}

	public String getBootStrapServers()
	{
		return m_bootStrapServers;
	}

	public String getStreamStateDirectory()
	{
		return m_streamStateDirectory;
	}

	public String getTopicOwnerTopicName()
	{
		return m_topicOwnerTopicName;
	}

	public String getConsumerRateMetric()
	{
		return m_metricPrefix + m_consumerRateMetric;
	}

	public String getOffsetAgeMetric()
	{
		return m_metricPrefix + m_offsetAgeMetric;
	}

	public String getPartitionLagMetric()
	{
		return m_metricPrefix + m_partitionLagMetric;
	}

	public String getGroupLagMetric()
	{
		return m_metricPrefix + m_groupLagMetric;
	}
}
