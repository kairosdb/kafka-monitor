package org.kairosdb.kafka.monitor;

import java.util.concurrent.atomic.AtomicLong;

public class OffsetStat
{
	//todo review if needs to be atomic
	private long m_offset;
	private long m_timestamp;
	private final int m_partition;

	public OffsetStat(long offset, long timestamp, int partition)
	{
		m_offset = offset;
		m_timestamp = timestamp;
		m_partition = partition;
	}

	public OffsetStat copy()
	{
		OffsetStat copy = new OffsetStat(m_offset, m_timestamp, m_partition);

		return copy;
	}

	public long updateOffset(long offset, long timestamp)
	{
		long diff = calculateDiff(offset, m_offset);
		m_offset = offset;
		m_timestamp = timestamp;

		return diff;
	}

	public long getTimestamp()
	{
		return m_timestamp;
	}

	public int getPartition()
	{
		return m_partition;
	}

	public long getOffset()
	{
		return m_offset;
	}

	public static long calculateDiff(long higher, long lower)
	{
		if (lower > higher)
		{
			return (Long.MAX_VALUE - lower) + higher;
		}
		else
			return higher - lower;
	}
}
