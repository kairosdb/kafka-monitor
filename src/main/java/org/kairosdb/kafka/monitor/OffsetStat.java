package org.kairosdb.kafka.monitor;

import java.util.concurrent.atomic.AtomicLong;

public class OffsetStat
{
	private final AtomicLong m_offsetCounter = new AtomicLong();
	private long m_offset; //atomic
	private long m_timestamp;
	private final int m_partition;

	public OffsetStat(long offset, long timestamp, int partition)
	{
		m_offset = offset;
		m_timestamp = timestamp;
		m_partition = partition;
	}

	public long updateOffset(long offset, long timestamp)
	{
		long diff = offset - m_offset;
		m_offset = offset;
		m_timestamp = timestamp;

		m_offsetCounter.addAndGet(diff);

		return diff; //todo need to handle rollover
	}

	public long getTimestamp()
	{
		return m_timestamp;
	}

	public long getAndResetCounter()
	{
		return m_offsetCounter.getAndSet(0L);
	}

	public int getPartition()
	{
		return m_partition;
	}

	public long getOffset()
	{
		return m_offset;
	}
}
