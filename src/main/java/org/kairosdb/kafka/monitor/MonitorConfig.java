package org.kairosdb.kafka.monitor;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Collections;
import java.util.Map;

public class MonitorConfig
{
	private static final Splitter.MapSplitter AdditionalTagsSplitter = Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=');

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
	@Named("kairosdb.kafka_monitor.stream_state_directory")
	private String m_streamStateDirectory = "stream-state";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.topic_owner_topic_name")
	private String m_topicOwnerTopicName = "kafka_monitor_topic_owner";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.offsets_topic_name")
	private String m_offsetsTopicName = "kafka_monitor_offsets";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.prefix")
	private String m_metricPrefix = "kafka_monitor.";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.consumer_rate")
	private String m_consumerRateMetric = "consumer_rate";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.producer_rate")
	private String m_producerRateMetric = "producer_rate";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.offset_age")
	private String m_offsetAgeMetric = "offset_age";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.partition_lag")
	private String m_partitionLagMetric = "partition_lag";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.group_lag")
	private String m_groupLagMetric = "group_lag";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.group_time_to_process")
	private String m_groupTimeToProcessMetric = "group_time_to_process";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.offset_gather_time_ms")
	private String m_offsetGatherTimeMetric = "offset_gather_time";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.metric.gather_failure")
	private String m_gatherFailureMetric = "gather_failure";

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.exclude_monitor_offsets")
	private boolean m_excludeMonitorOffsets = true;

	@Inject(optional = true)
	@Named("kairosdb.kafka_monitor.rate_tracker_size")
	private int m_rateTrackerSize = 10;

	private Map<String, String> m_additionalTags = Collections.EMPTY_MAP;

	@Inject(optional = true)
	public void setAdditionalTags(@Named("kairosdb.kafka_monitor.metric.additional_tags") String additionalTags)
	{
		//map returned is unmodifiable
		m_additionalTags = AdditionalTagsSplitter.split(additionalTags);
	}

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

	public String getOffsetsTopicName()
	{
		return m_offsetsTopicName;
	}

	public String getConsumerRateMetric()
	{
		return m_metricPrefix + m_consumerRateMetric;
	}

	public String getProducerRateMetric()
	{
		return m_metricPrefix + m_producerRateMetric;
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

	public String getGroupTimeToProcessMetric()
	{
		return m_metricPrefix + m_groupTimeToProcessMetric;
	}

	public String getOffsetGatherTimeMetric()
	{
		return m_metricPrefix + m_offsetGatherTimeMetric;
	}

	public String getGatherFailureMetric()
	{
		return m_metricPrefix + m_gatherFailureMetric;
	}

	public Map<String, String> getAdditionalTags()
	{
		return m_additionalTags;
	}

	public boolean isExcludeMonitorOffsets()
	{
		return m_excludeMonitorOffsets;
	}

	public int getRateTrackerSize()
	{
		return m_rateTrackerSize;
	}
}
