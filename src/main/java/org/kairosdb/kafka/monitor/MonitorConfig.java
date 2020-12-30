package org.kairosdb.kafka.monitor;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.time.Duration;

/**
 If you add any property that is a Duration then you must add that property to
 DurationConfigModule so it can be parsed correctly
 */
public class MonitorConfig
{
	@Inject(optional = true)
	@Named("topic_monitor.application_id")
	private String m_applicationId = "kafka_monitor";

	@Inject(optional = true)
	@Named("topic_monitor.client_id")
	private String m_clientId = null; //defaults to hostname if null

	@Inject
	@Named("topic_monitor.bootstrap_servers")
	private String m_bootStrapServers;

	@Inject(optional = true)
	@Named("topic_monitor.topic_owner_topic_name")
	private String m_topicOwnerTopicName = "kafka_monitor_topic_owner";

	@Inject(optional = true)
	@Named("topic_monitor.offsets_topic_name")
	private String m_offsetsTopicName = "kafka_monitor_offsets";

	@Inject(optional = true)
	@Named("topic_monitor.exclude_monitor_offsets")
	private boolean m_excludeMonitorOffsets = true;

	@Inject(optional = true)
	@Named("topic_monitor.rate_tracker_size")
	private int m_rateTrackerSize = 10;

	@Inject(optional = true)
	@Named("topic_monitor.tracker_retention")
	private Duration m_trackerRetentionMinutes = Duration.ofHours(24);

	@Inject(optional = true)
	@Named("topic_monitor.stale_partition_age")
	private Duration m_stalePartitionAge = Duration.ofMinutes(1);

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "localhost";

	@Inject(optional = false)
	@Named("topic_monitor.dead_client_restart")
	private Duration m_deadClientRestart = Duration.ofMinutes(5);

	public MonitorConfig()
	{
	}

	public Duration getDeadClientRestart()
	{
		return m_deadClientRestart;
	}

	public Duration getStalePartitionAge()
	{
		return m_stalePartitionAge;
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

	public Duration getTrackerRetention()
	{
		return m_trackerRetentionMinutes;
	}
}
