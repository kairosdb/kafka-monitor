package org.kairosdb.kafka.monitor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GroupStats
{
	private final String m_groupName;
	private final String m_topic;
	private long m_consumeCount;
	private final Map<Integer, OffsetStat> m_partitionStats;
	private final Object m_offsetLock = new Object();
	private final ProcessRate m_processRate;


	public GroupStats(String groupName, String topic, int trackerCount)
	{
		this(groupName, topic, new ProcessRate(trackerCount));
	}

	private GroupStats(String groupName, String topic, ProcessRate processRate)
	{
		m_groupName = groupName;
		m_topic = topic;
		m_partitionStats = new ConcurrentHashMap<>();
		m_processRate = processRate;
	}

	/**
	 Creates a deep copy of this GroupStats object and resets the consume count
	 @return
	 */
	public GroupStats copyAndReset()
	{
		GroupStats copy = new GroupStats(m_groupName, m_topic, m_processRate);

		synchronized (m_offsetLock)
		{
			copy.m_consumeCount = m_consumeCount;
			m_consumeCount = 0L;

			for (Map.Entry<Integer, OffsetStat> offset : m_partitionStats.entrySet())
			{
				copy.m_partitionStats.put(offset.getKey(), offset.getValue().copy());
			}
		}

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

	/**
	 Returns the average rate in milliseconds
	 @return
	 */
	public double getCurrentRate()
	{
		return m_processRate.getCurrentRate();
	}

	public void offsetChange(int partition, long offset, long timestamp)
	{
		synchronized (m_offsetLock)
		{
			OffsetStat offsetStat = m_partitionStats.get(partition);

			if (offsetStat == null)
			{
				m_partitionStats.put(partition, new OffsetStat(offset, timestamp, partition));
			}
			else
			{
				long time = timestamp - offsetStat.getTimestamp();

				//make sure the time really changed
				if (time > 0)
				{
					long diff = OffsetStat.calculateDiff(offset, offsetStat.getOffset());

					double rate = (double) diff / (double) time;

					m_processRate.addRate(rate);

					m_consumeCount += offsetStat.updateOffset(offset, timestamp);
				}
			}
		}
	}

	public long getConsumeCount()
	{
		return m_consumeCount;
	}

	public List<OffsetStat> getOffsetStats()
	{
		return ImmutableList.copyOf(m_partitionStats.values());
	}

}
