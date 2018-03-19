package org.kairosdb.kafka.monitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetsTracker
{

	private Set<String> m_topics = new ConcurrentHashSet<>();
	private Map<Pair<String, String>, GroupStats> m_groupStatsMap = new ConcurrentHashMap<>();


	private final int m_rateTrackerSize;

	public OffsetsTracker(int rateTrackerSize)
	{
		m_rateTrackerSize = rateTrackerSize;
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
					m_rateTrackerSize);
			m_groupStatsMap.put(groupKey, groupStats);
		}

		groupStats.offsetChange(offset.getPartition(), offset.getOffset(), offset.getCommitTime());
	}

	public List<GroupStats> copyOfCurrentStats()
	{
		ImmutableList.Builder<GroupStats> builder = ImmutableList.builder();

		for (Map.Entry<Pair<String, String>, GroupStats> groupStatsEntry : m_groupStatsMap.entrySet())
		{
			if (!m_topics.contains(groupStatsEntry.getValue().getTopic()))
			{
				m_groupStatsMap.remove(groupStatsEntry);
				continue;  //We are not tracking this topic anymore
			}

			builder.add(groupStatsEntry.getValue().copyAndReset());
		}

		return builder.build();
	}


}
