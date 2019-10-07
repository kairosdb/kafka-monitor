package org.kairosdb.kafka.monitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.PartitionInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetsTracker
{

	private final long m_trackerRetentionMinutes;
	private Set<String> m_topics = ConcurrentHashMap.newKeySet();
	private Map<Pair<String, String>, GroupStats> m_groupStatsMap = new ConcurrentHashMap<>();

	private Map<String, List<PartitionInfo>> m_kafkaTopics = new HashMap<>();

	private final int m_rateTrackerSize;

	@Inject
	public OffsetsTracker(MonitorConfig config)
	{
		m_rateTrackerSize = config.getRateTrackerSize();
		m_trackerRetentionMinutes = config.getTrackerRetentionMinutes();
	}

	public void removeTrackedTopic(String topic)
	{
		m_topics.remove(topic);
	}

	public Set<String> getTopics()
	{
		return ImmutableSet.copyOf(m_topics);
	}

	public void updateOffset(Offset offset)
	{
		m_topics.add(offset.getTopic());

		Pair<String, String> groupKey = Pair.of(offset.getTopic(), offset.getGroup());
		GroupStats groupStats = m_groupStatsMap.get(groupKey);

		if (groupStats == null)
		{
			groupStats = new GroupStats(offset.getGroup(), offset.getTopic(),
					m_rateTrackerSize, m_trackerRetentionMinutes);
			m_groupStatsMap.put(groupKey, groupStats);
		}

		groupStats.offsetChange(offset.getPartition(), offset.getOffset(), offset.getCommitTime());
	}

	public List<GroupStats> copyOfCurrentStats()
	{
		ImmutableList.Builder<GroupStats> builder = ImmutableList.builder();

		for (Map.Entry<Pair<String, String>, GroupStats> groupStatsEntry : m_groupStatsMap.entrySet())
		{
			//Remove group stats if we don't own the topic or all offsets have expired
			if (!m_topics.contains(groupStatsEntry.getValue().getTopic()) ||
					groupStatsEntry.getValue().getOffsetStats().isEmpty())
			{
				m_groupStatsMap.remove(groupStatsEntry);
				continue;  //We are not tracking this topic anymore
			}

			builder.add(groupStatsEntry.getValue().copyAndReset());
		}

		return builder.build();
	}

	public void updateTopics(Map<String, List<PartitionInfo>> kafkaTopics)
	{
		m_kafkaTopics = ImmutableMap.copyOf(kafkaTopics);
	}

	public List<PartitionInfo> getPartitionInfo(String topic)
	{
		return m_kafkaTopics.get(topic);
	}


}
