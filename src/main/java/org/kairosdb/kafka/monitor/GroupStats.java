package org.kairosdb.kafka.monitor;

import com.google.common.collect.ImmutableList;
import org.kairosdb.kafka.monitor.util.Stopwatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GroupStats
{
	private final String m_groupName;
	private final String m_topic;
	private final String m_proxyGroup; //rest proxy combines group and topic with __
	private long m_consumeCount;
	private final Map<Integer, OffsetStat> m_partitionStats;
	private final Object m_offsetLock = new Object();
	private final ProcessRate m_processRate;
	private Stopwatch m_rateTimer;
	private final long m_trackerRetention;  //Minutes to track offsets


	public GroupStats(String groupName, String topic, int trackerCount, long trackerRetention)
	{
		this(groupName, topic, new ProcessRate(trackerCount), trackerRetention);
	}

	private GroupStats(String groupName, String topic, ProcessRate processRate, long trackerRetention)
	{
		m_groupName = groupName;
		m_topic = topic;
		m_partitionStats = new ConcurrentHashMap<>();
		m_processRate = processRate;
		m_rateTimer = Stopwatch.createStarted();
		m_trackerRetention = trackerRetention;

		if (m_groupName.contains("__"))
		{
			int index = m_groupName.lastIndexOf("__");
			m_proxyGroup = m_groupName.substring(0, index);
		}
		else
			m_proxyGroup = m_groupName;
	}

	//For testing
	protected void setRateTimer(Stopwatch stopwatch)
	{
		m_rateTimer = stopwatch;
	}

	/**
	 Creates a deep copy of this GroupStats object and resets the consume count
	 @return
	 */
	public GroupStats copyAndReset()
	{
		long expireTime = System.currentTimeMillis() - (m_trackerRetention * 60 * 1000);
		GroupStats copy = new GroupStats(m_groupName, m_topic, m_processRate, m_trackerRetention);

		synchronized (m_offsetLock)
		{
			long elapsed = m_rateTimer.elapsed(TimeUnit.SECONDS);
			m_rateTimer.reset().start();

			m_processRate.addRate((double)m_consumeCount / (double)elapsed);

			copy.m_consumeCount = m_consumeCount;
			m_consumeCount = 0L;

			for (Map.Entry<Integer, OffsetStat> offset : m_partitionStats.entrySet())
			{
				if (expireTime < offset.getValue().getCommitTime())
					copy.m_partitionStats.put(offset.getKey(), offset.getValue().copy());
				else
					m_partitionStats.remove(offset.getKey());  //Remove entry if expired so we don't track it anymore
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

	public String getProxyGroup()
	{
		return m_proxyGroup;
	}

	/**
	 Returns the average rate in milliseconds
	 @return
	 */
	public double getCurrentRate()
	{
		return m_processRate.getCurrentRate();
	}

	public void offsetChange(int partition, long offset, long commitTime)
	{
		synchronized (m_offsetLock)
		{
			OffsetStat offsetStat = m_partitionStats.get(partition);

			if (offsetStat == null)
			{
				m_partitionStats.put(partition, new OffsetStat(partition, offset, commitTime));
			}
			else
			{
				long time = commitTime - offsetStat.getCommitTime();

				//make sure the time really changed in the right direction
				if (time > 0)
				{
					m_consumeCount += offsetStat.updateOffset(offset, commitTime);
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
