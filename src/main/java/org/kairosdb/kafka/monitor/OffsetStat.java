package org.kairosdb.kafka.monitor;

public class OffsetStat
{
	private final int m_partition;
	//todo review if needs to be atomic
	private long m_offset;
	private long m_commitTime;


	public OffsetStat(int partition, long offset, long commitTime)
	{
		m_offset = offset;
		m_commitTime = commitTime;
		m_partition = partition;
	}

	public OffsetStat copy()
	{
		OffsetStat copy = new OffsetStat(m_partition, m_offset, m_commitTime);

		return copy;
	}

	public long updateOffset(long offset, long commitTime)
	{
		long diff = calculateDiff(offset, m_offset);

		if (diff != 0)
		{
			m_offset = offset;
			m_commitTime = commitTime;
		}

		return diff;
	}

	public long getCommitTime()
	{
		return m_commitTime;
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
