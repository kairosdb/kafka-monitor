package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import com.google.inject.name.Named;


public class MonitorConfig
{
	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.application_id")
	private String m_applicationId = "kafka_monitor";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.client_id")
	private String m_clientId = null; //defaults to hostname if null

	@Inject
	@Named("kairosdb.kafka_monitor.bootstrap_servers")
	private String m_bootStrapServers;

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.topic_owner_topic_name")
	private String m_topicOwnerTopicName = "kafka_monitor_topic_owner";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.offsets_topic_name")
	private String m_offsetsTopicName = "kafka_monitor_offsets";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.exclude_monitor_offsets")
	private boolean m_excludeMonitorOffsets = true;

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.rate_tracker_size")
	private int m_rateTrackerSize = 10;

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.tracker_retention_minutes")
	private long m_trackerRetentionMinutes = 1_440; //default to 24hrs

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

	public String getTopicOwnerTopicName()
	{
		return m_topicOwnerTopicName;
	}

	public String getOffsetsTopicName()
	{
		return m_offsetsTopicName;
	}

	public boolean isExcludeMonitorOffsets()
	{
		return m_excludeMonitorOffsets;
	}

	public int getRateTrackerSize()
	{
		return m_rateTrackerSize;
	}

	public long getTrackerRetentionMinutes()
	{
		return m_trackerRetentionMinutes;
	}
}
