package org.kairosdb.kafka.monitor;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class GroupStats
{
	private final String m_groupName;
	private final String m_topic;
	private final AtomicLong m_consumeCount = new AtomicLong();
	private final Map<Integer, OffsetStat> m_partitionStats;

	public GroupStats(String groupName, String topic)
	{
		m_groupName = groupName;
		m_topic = topic;
		m_partitionStats = new ConcurrentHashMap<>();
	}

	public GroupStats copy()
	{
		GroupStats copy = new GroupStats(m_groupName, m_topic);
		copy.m_consumeCount.getAndSet(m_consumeCount.get());

		return copy;
	}

	public String getGroupName()
	{
		return m_groupName;
	}

	public String getTopic()
	{
		return m_topic;
	}

	public void offsetChange(int partition, long offset, long timestamp)
	{
		OffsetStat offsetStat = m_partitionStats.get(partition);

		if (offsetStat == null)
		{
			m_partitionStats.put(partition, new OffsetStat(offset, timestamp, partition));
		}
		else
		{
			m_consumeCount.addAndGet(offsetStat.updateOffset(offset, timestamp));
		}
	}

	public long getAndResetCounter()
	{
		return m_consumeCount.getAndSet(0L);
	}

	public List<OffsetStat> getOffsetStats()
	{
		return ImmutableList.copyOf(m_partitionStats.values());
	}

}
